package nets

import (
	"errors"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/standard"
	"io"
	"net"
	"sync/atomic"
	"time"
)

func longtimeReadMonitor(biz string, conn net.Conn, recvChan chan int, endFlag *atomic.Bool, tid string) {
	f := func(v int) {
		select {
		case recvChan <- v:
		case <-time.After(time.Second):

		}
	}
	for {
		code, err := InputAck(conn, time.Second*5)
		if endFlag.Load() {
			break
		}
		if err != nil && IsTimeoutError(err) {
			continue
		}
		if err != nil {
			logger.Get().Infof("tid=%s,longtimeReadMonitor of %s met err,and exit monitor:%v", tid, biz, err)
			f(protocol.ConnPeerClosed)
			break
		}
		f(code)
	}

}

type LongtimeReader[T any] interface {
	Read(endNotify <-chan int) ([]*T, error)
	Output(conn net.Conn, msgs []*T) error
	io.Closer
}

func LongTimeRun[T any](conn net.Conn, biz, tid string, ackTimeout, writeTimeout time.Duration, reader LongtimeReader[T]) error {
	var err error
	recvCh := make(chan int, 1)
	var endFlag atomic.Bool
	go longtimeReadMonitor(biz, conn, recvCh, &endFlag, tid)

	defer func() {
		reader.Close()
		endFlag.Store(true)
	}()
	for {
		var msgs []*T
		msgs, err = reader.Read(recvCh)

		if err == nil {
			if err = reader.Output(conn, msgs); err != nil {
				return err
			}
			var ackCode int
			select {
			case ackCode = <-recvCh:
			case <-time.After(ackTimeout):
				logger.Get().Infof("tid=%s,read ack timeout", tid)
				return errors.New("read ack timeout")
			}
			if ackCode == protocol.ConnPeerClosed {
				logger.Get().Infof("tid=%s,conn peer closed", tid)
				return errors.New("conn peer closed")
			}
			if ackCode == protocol.SubAckWithEnd {
				logger.Get().Infof("tid=%s,client send ack and closed", tid)
				return nil
			}
			if ackCode != protocol.SubAck {
				logger.Get().Infof("tid=%s,client send invalid ack code %d", tid, ackCode)
				return errors.New("invalid ack")
			}
			continue
		}
		if err == standard.WaitNewTimeoutErr {
			err = OutAlive(conn, writeTimeout)
			logger.Get().Infof("tid=%s,sub wait new data timeout, send alive:%v", tid, err)
			if err != nil {
				return err
			}
			continue
		}
		if err == standard.PeerClosedErr {
			logger.Get().Infof("tid=%s,PeerClosedErr:%v", tid, err)
			return err
		}
		if err == standard.MqWriterTermiteErr {
			logger.Get().Infof("tid=%s,MqWriterTermiteErr:%v", tid, err)
			return OutSubEnd(conn, writeTimeout)
		}
		return OutputRecoverErr(conn, err.Error(), writeTimeout)
	}
}
