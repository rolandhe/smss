package replica

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/pkg/dir"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/replica/slave"
	"github.com/rolandhe/smss/store"
	"net"
	"sync/atomic"
	"time"
)

const (
	connectTimeout           = time.Second * 3
	netReadTimeout           = time.Millisecond * 3000
	netWriteTimeout          = time.Millisecond * 3000
	replicaReadNewLogTimeout = time.Millisecond * 10000
)

func newSlaveReplicaClient(masterHost string, masterPort int, worker slave.DependWorker) (*slaveClient, error) {
	c := &slaveClient{
		host:   masterHost,
		port:   masterPort,
		worker: worker,
	}
	if err := c.connect(); err != nil {
		return nil, err
	}
	return c, nil
}

type slaveClient struct {
	host        string
	port        int
	conn        net.Conn
	endNotify   chan struct{}
	worker      slave.DependWorker
	state       atomic.Bool
	lastEventId int64
}

func (sc *slaveClient) connect() error {
	var err error
	sc.conn, err = net.DialTimeout("tcp", fmt.Sprintf("%s:%d", sc.host, sc.port), connectTimeout)
	return err
}

func (sc *slaveClient) Close() error {
	if sc.state.Load() {
		return nil
	}
	if sc.state.CompareAndSwap(false, true) {
		return sc.conn.Close()
	}
	return nil
}

func (sc *slaveClient) getValidMq(seqId int64) ([]*store.MqInfo, error) {
	buf := make([]byte, 28)
	buf[0] = byte(protocol.CommandValidList)
	binary.LittleEndian.PutUint64(buf[protocol.HeaderSize:], uint64(seqId))
	if err := nets.WriteAll(sc.conn, buf, netWriteTimeout); err != nil {
		return nil, err
	}
	hBuf := buf[:protocol.RespHeaderSize]
	if err := nets.ReadAll(sc.conn, hBuf, netReadTimeout); err != nil {
		return nil, err
	}
	code := binary.LittleEndian.Uint16(hBuf)
	if code == protocol.ErrCode {
		errMsgLen := int(binary.LittleEndian.Uint16(hBuf[2:]))
		if errMsgLen == 0 {
			return nil, errors.New("unknown err")
		}
		eMsgBuf := make([]byte, errMsgLen)
		if err := nets.ReadAll(sc.conn, eMsgBuf, netReadTimeout); err != nil {
			return nil, err
		}
		return nil, errors.New(string(eMsgBuf))
	}

	payLen := int(binary.LittleEndian.Uint32(hBuf[2:]))
	if payLen == 0 {
		return nil, nil
	}
	body := make([]byte, payLen)
	err := nets.ReadAll(sc.conn, body, netReadTimeout)
	if err != nil {
		return nil, err
	}
	var rets []*store.MqInfo
	err = json.Unmarshal(body, &rets)
	if err != nil {
		return nil, err
	}
	return rets, nil
}

func (sc *slaveClient) replica(seqId int64) error {
	logger.Get().Infof("slave begin to replica,eventId=%d", seqId)
	sc.lastEventId = seqId
	buf := make([]byte, 28)
	buf[0] = byte(protocol.CommandReplica)
	binary.LittleEndian.PutUint64(buf[protocol.HeaderSize:], uint64(seqId))
	if err := nets.WriteAll(sc.conn, buf, netWriteTimeout); err != nil {
		return err
	}

	cmdParser := &msgParser{}
	count := int64(0)

	for {
		hBuf := buf[:protocol.RespHeaderSize]
		err := nets.ReadAll(sc.conn, hBuf, replicaReadNewLogTimeout)
		if err != nil {
			if nets.IsTimeoutError(err) {
				logger.Get().Infof("wait new binlog data timeout,wait...")
				continue
			}
			return err
		}
		code := int(binary.LittleEndian.Uint16(hBuf[:2]))
		if code == protocol.OkCode {
			var body []byte
			body, err = readPayload(sc.conn, buf[:4])
			if err != nil {
				return err
			}
			if sc.lastEventId, err = applyBinlog(body, cmdParser, sc.worker, count); err != nil {
				return err
			}
			count++
			continue
		}
		if code == protocol.AliveCode {
			logger.Get().Infof("slave recv alive msg")
			continue
		}
		if code == protocol.ErrCode {
			errMsgLen := int(binary.LittleEndian.Uint16(hBuf[2:]))
			if errMsgLen == 0 {
				return errors.New("unknown err")
			}
			eMsgBuf := make([]byte, errMsgLen)
			if err = nets.ReadAll(sc.conn, eMsgBuf, netReadTimeout); err != nil {
				return err
			}
			return errors.New(string(eMsgBuf))
		}
		logger.Get().Infof("invalid response:%d", code)
		return errors.New("invalid response")
	}
}

func applyBinlog(body []byte, cmdParse *msgParser, worker slave.DependWorker, count int64) (int64, error) {
	defer cmdParse.Reset()
	cmdLen := binary.LittleEndian.Uint32(body)
	cmdLine, err := cmdParse.ParseCmd(body[4 : cmdLen+4])
	if err != nil {
		return 0, err
	}
	next := body[cmdLen+4:]
	var payload []byte
	if cmdLine.GetPayloadSize() > 0 {
		payload = next[:cmdLine.GetPayloadSize()-1]
	}

	hfunc := bbHandlerMap[cmdLine.GetCmd()]
	if hfunc == nil {
		logger.Get().Infof("not support cmd:%d", cmdLine.GetCmd())
		return 0, dir.NewBizError("not support cmd")
	}
	st := time.Now().UnixMilli()
	err = hfunc(cmdParse.cmd, payload, worker)
	if count%conf.LogSample == 0 {
		rcost := time.Now().UnixMilli() - st
		logger.Get().Infof("slave: tid=%s,cmd=%d,eventId=%d,count=%d,delay=%dms,rcost=%d,err:%v", cmdParse.cmd.TraceId, cmdParse.cmd.Command, cmdParse.cmd.EventId, count, cmdParse.cmd.GetDelay(), rcost, err)
	}
	return cmdParse.cmd.EventId, err
}

func readPayload(conn net.Conn, lenBuf []byte) ([]byte, error) {
	err := nets.ReadAll(conn, lenBuf, netReadTimeout)
	if err != nil {
		return nil, err
	}
	bodyLen := int(binary.LittleEndian.Uint32(lenBuf))
	body := make([]byte, bodyLen)
	err = nets.ReadAll(conn, body, netReadTimeout)
	if err != nil {
		return nil, err
	}
	return body, nil
}
