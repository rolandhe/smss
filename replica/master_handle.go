package replica

import (
	"encoding/binary"
	"errors"
	"github.com/google/uuid"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/cmd/repair"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/pkg/dir"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/standard"
	"net"
	"runtime"
	"sync/atomic"
	"time"
)

const DefaultAckTimeout = 10 * 1000

const BinlogOutTimeout = time.Second * 120

type WalMonitorSupport interface {
	GetRoot() string
	standard.LogFileControl
}

func getFilePosByEventId(root string, eventId int64) (int64, int64, error) {
	var fileId int64
	var err error
	if eventId == 0 {
		fileId, err = standard.ReadFirstFileId(root)
		if err != nil {
			return 0, 0, err
		}
		return fileId, 0, nil
	}

	return repair.FindBinlogPosByEventId(root, eventId)
}

func MasterHandle(conn net.Conn, header *protocol.CommonHeader, walMonitor WalMonitorSupport, readTimeout, writeTimeout time.Duration) error {
	runtime.LockOSThread()
	buf := make([]byte, 8)
	err := nets.ReadAll(conn, buf, readTimeout)
	if err != nil {
		return err
	}
	lastEventId := int64(binary.LittleEndian.Uint64(buf))

	if lastEventId < 0 {
		return dir.NewBizError("invalid replica eventId")
	}

	uuidStr := uuid.NewString()
	fileId, pos, err := getFilePosByEventId(walMonitor.GetRoot(), lastEventId)
	if err != nil {
		logger.Get().Infof("tid=%s,replca server,eventId=%d, error:%v", header.TraceId, lastEventId, err)
		return nets.OutputRecoverErr(conn, err.Error(), writeTimeout)
	}

	reader := newBlockReader(uuidStr, walMonitor)

	if err = reader.Init(fileId, pos); err != nil {
		logger.Get().Infof("tid=%s,replica server,eventId=%d, init error:%v", header.TraceId, lastEventId, err)
		return nets.OutputRecoverErr(conn, err.Error(), writeTimeout)
	}

	err = noAckPush(conn, header.TraceId, reader)
	if err != nil {
		logger.Get().Infof("master handle finish,eventId=%d, err:%v", lastEventId, err)
	}
	return err
}

func noAckPush(conn net.Conn, tid string, reader serverBinlogBlockReader) error {
	var err error
	recvCh := make(chan int, 1)
	var endFlag atomic.Bool

	go peerCloseMonitor(conn, recvCh, &endFlag, tid)
	defer func() {
		reader.Close()
		endFlag.Store(true)
	}()
	count := int64(0)
	totalCost := int64(0)
	for {
		var msgs []*binlogBlock
		if endFlag.Load() {
			return errors.New("conn end by flag")
		}
		msgs, err = reader.Read(recvCh)
		if err == nil {
			outBuf := make([]byte, protocol.RespHeaderSize+4+len(msgs[0].data))
			binary.LittleEndian.PutUint16(outBuf, protocol.OkCode)
			msgLen := len(msgs[0].data)
			binary.LittleEndian.PutUint32(outBuf[protocol.RespHeaderSize:], uint32(msgLen))
			copy(outBuf[protocol.RespHeaderSize+4:], msgs[0].data)

			start := time.Now().UnixMilli()
			readDelay := start - msgs[0].rawMsg.WriteTime

			if err = writeAllWithTotalTimeout(conn, outBuf, BinlogOutTimeout, &endFlag); err != nil {
				logger.Get().Infof("tid=%s, eventId=%d,err:%v", tid, msgs[0].rawMsg.EventId, err)
				return err
			}
			cost := start - time.Now().UnixMilli()
			totalCost += cost
			if count%conf.LogSample == 0 {
				logger.Get().Infof("master to slave:tid=%s, eventId=%d,count=%d, delay time=%dms,readDelay=%dms,total cost=%dms", tid, msgs[0].rawMsg.EventId, count, readDelay+cost, readDelay, totalCost)
			}
			count++
			continue
		}
		if err == standard.WaitNewTimeoutErr {
			err = nets.OutAlive(conn, BinlogOutTimeout)
			logger.Get().Infof("tid=%s,sub wait new data timeout, send alive:%v", tid, err)
			if err != nil {
				return err
			}
			continue
		}

		return nets.OutputRecoverErr(conn, err.Error(), BinlogOutTimeout)
	}
}

func peerCloseMonitor(conn net.Conn, recvChan chan int, endFlag *atomic.Bool, tid string) {
	f := func(v int) {
		select {
		case recvChan <- v:
		case <-time.After(time.Second):

		}
	}
	buf := make([]byte, 20)
	for {
		err := nets.ReadAll(conn, buf, time.Second*5)
		if endFlag.Load() {
			break
		}
		if err != nil && nets.IsTimeoutError(err) {
			continue
		}
		if err != nil {
			logger.Get().Infof("tid=%s,peerCloseMonitor met err,and exit monitor:%v", tid, err)
			f(protocol.ConnPeerClosed)
			endFlag.Store(true)
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
		n, err := conn.Write(buf)
		if err != nil {
			if !nets.IsTimeoutError(err) || timeout <= 0 {
				return err
			}
		}
		if endFlag.Load() {
			return errors.New("conn end by flag")
		}
		if n == l {
			return nil
		}
		buf = buf[n:]
	}
}
