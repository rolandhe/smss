package replica

import (
	"encoding/binary"
	"errors"
	"github.com/google/uuid"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/cmd/repair"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"net"
	"sync/atomic"
	"time"
)

const BinlogOutTimeout = time.Second * 120

type WalMonitorSupport interface {
	GetRoot() string
	standard.LogFileControl
}

func getFilePosByEventId(root string, eventId int64, lastFileId int64) (int64, int64, error) {
	var fileId int64
	var err error
	if eventId == 0 {
		fileId, err = standard.ReadFirstFileId(root, lastFileId)
		if err != nil {
			return 0, 0, err
		}
		return fileId, 0, nil
	}

	return repair.FindBinlogPosByEventId(root, eventId, lastFileId)
}

func MasterHandle(conn net.Conn, header *protocol.CommonHeader, walMonitor WalMonitorSupport, readTimeout, writeTimeout time.Duration) error {
	buf := make([]byte, 8)
	err := nets.ReadAll(conn, buf, readTimeout)
	if err != nil {
		return err
	}
	lastEventId := int64(binary.LittleEndian.Uint64(buf))

	if lastEventId < 0 {
		logger.Infof("tid=%s,replca server,eventId=%d, event id must >=", header.TraceId, lastEventId)
		return nets.OutputRecoverErr(conn, "invalid replica eventId", writeTimeout)
	}

	uuidStr := uuid.NewString()

	reader := newBlockReader(uuidStr, walMonitor)

	if err = reader.Init(func(lastFileId int64) (int64, int64, error) {
		return getFilePosByEventId(walMonitor.GetRoot(), lastEventId, lastFileId)
	}); err != nil {
		logger.Infof("tid=%s,replica server,eventId=%d, init error:%v", header.TraceId, lastEventId, err)
		return nets.OutputRecoverErr(conn, err.Error(), writeTimeout)
	}

	err = noAckPush(conn, header.TraceId, reader)
	if err != nil {
		logger.Infof("master handle finish,eventId=%d, err:%v", lastEventId, err)
	}
	return err
}

func noAckPush(conn net.Conn, tid string, reader serverBinlogBlockReader) error {
	var err error

	clientClosedNotify := &store.ClientClosedNotifyEquipment{
		ClientClosedNotifyChan: make(chan struct{}),
	}

	go peerCloseMonitor(conn, clientClosedNotify, tid)
	defer func() {
		reader.Close()
		clientClosedNotify.ClientClosedFlag.Store(true)
	}()
	count := int64(0)
	totalCost := int64(0)
	for {
		var msgs []*binlogBlock
		if clientClosedNotify.ClientClosedFlag.Load() {
			return errors.New("conn end by flag")
		}
		msgs, err = reader.Read(clientClosedNotify)
		if err == nil {
			outBuf := make([]byte, protocol.RespHeaderSize+4+len(msgs[0].data))
			binary.LittleEndian.PutUint16(outBuf, protocol.OkCode)
			msgLen := len(msgs[0].data)
			binary.LittleEndian.PutUint32(outBuf[protocol.RespHeaderSize:], uint32(msgLen))
			copy(outBuf[protocol.RespHeaderSize+4:], msgs[0].data)

			start := time.Now().UnixMilli()
			readDelay := start - msgs[0].rawMsg.WriteTime

			if err = writeAllWithTotalTimeout(conn, outBuf, BinlogOutTimeout, &clientClosedNotify.ClientClosedFlag); err != nil {
				logger.Infof("tid=%s, eventId=%d,err:%v", tid, msgs[0].rawMsg.EventId, err)
				return err
			}
			cost := start - time.Now().UnixMilli()
			totalCost += cost
			if conf.LogSample > 0 && count%conf.LogSample == 0 {
				logger.Infof("master to slave:tid=%s, eventId=%d,count=%d, delay time=%dms,readDelay=%dms,total cost=%dms", tid, msgs[0].rawMsg.EventId, count, readDelay+cost, readDelay, totalCost)
			}
			count++
			continue
		}
		if errors.Is(err, standard.WaitNewTimeoutErr) {
			err = nets.OutAlive(conn, BinlogOutTimeout)
			logger.Infof("tid=%s,sub wait new data timeout, send alive:%v", tid, err)
			if err != nil {
				return err
			}
			continue
		}

		return nets.OutputRecoverErr(conn, err.Error(), BinlogOutTimeout)
	}
}

func peerCloseMonitor(conn net.Conn, clientClosedNotify *store.ClientClosedNotifyEquipment, tid string) {
	buf := make([]byte, 20)
	for {
		err := nets.ReadAll(conn, buf, time.Second*5)
		if clientClosedNotify.ClientClosedFlag.Load() {
			break
		}
		if err != nil && nets.IsTimeoutError(err) {
			continue
		}
		if err != nil {
			logger.Infof("tid=%s,peerCloseMonitor met err,and exit monitor:%v", tid, err)
			clientClosedNotify.ClientClosedFlag.Store(true)
			close(clientClosedNotify.ClientClosedNotifyChan)
			break
		}
	}

}

func writeAllWithTotalTimeout(conn net.Conn, buf []byte, timeout time.Duration, endFlag *atomic.Bool) error {
	d := time.Second

	for {
		l := len(buf)
		conn.SetWriteDeadline(time.Now().Add(d))
		timeout -= d
		if endFlag.Load() {
			return errors.New("conn end by flag")
		}
		n, err := conn.Write(buf)
		if err != nil {
			if !nets.IsTimeoutError(err) || timeout <= 0 {
				return err
			}
		}
		if n == l {
			return nil
		}
		buf = buf[n:]
	}
}
