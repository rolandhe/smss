package nets

import (
	"errors"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"io"
	"net"
	"time"
)

func longtimeReadMonitor(biz string, conn net.Conn, endNotify *store.EndNotifyEquipment, tid string) {
	f := func(v int) {
		select {
		case endNotify.EndNotify <- v:
		case <-time.After(time.Second):

		}
	}
	for {
		code, err := InputAck(conn, time.Second*5)
		if endNotify.EndFlag.Load() {
			break
		}
		if err != nil && IsTimeoutError(err) {
			continue
		}
		if err != nil {
			logger.Get().Infof("tid=%s,longtimeReadMonitor of %s met err,and exit monitor:%v", tid, biz, err)
			endNotify.EndFlag.Store(true)
			f(protocol.ConnPeerClosed)
			break
		}
		f(code)
	}

}

type LongtimeReader[T any] interface {
	Read(endNotify *store.EndNotifyEquipment) ([]*T, error)
	Output(conn net.Conn, msgs []*T) error
	io.Closer
}

func LongTimeRun[T any](conn net.Conn, biz, tid string, ackTimeout, writeTimeout time.Duration, reader LongtimeReader[T]) error {
	var err error
	endNotify := &store.EndNotifyEquipment{
		EndNotify: make(chan int, 1),
	}
	go longtimeReadMonitor(biz, conn, endNotify, tid)

	defer func() {
		reader.Close()
		endNotify.EndFlag.Store(true)
	}()
	var logCounter int64 = 0
	for {
		var msgs []*T
		msgs, err = reader.Read(endNotify)

		if logCounter%conf.LogSample == 0 {
			logger.Get().Infof("tid=%s,after-sub-read,err:%v", tid, err)
		}
		logCounter++

		if err == nil {
			if endNotify.EndFlag.Load() {
				return errors.New("conn peer closed")
			}
			if err = reader.Output(conn, msgs); err != nil {
				return err
			}
			var ackCode int
			select {
			case ackCode = <-endNotify.EndNotify:
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
		if errors.Is(err, standard.WaitNewTimeoutErr) {
			err = OutAlive(conn, writeTimeout)
			logger.Get().Infof("tid=%s,sub wait new data timeout, send alive:%v", tid, err)
			if err != nil {
				return err
			}
			continue
		}
		if errors.Is(err, standard.PeerClosedErr) {
			logger.Get().Infof("tid=%s,PeerClosedErr:%v", tid, err)
			return err
		}
		if errors.Is(err, standard.TopicWriterTermiteErr) {
			logger.Get().Infof("tid=%s,TopicWriterTermiteErr:%v", tid, err)
			return OutSubEnd(conn, writeTimeout)
		}

		return OutputRecoverErr(conn, err.Error(), writeTimeout)
	}
}
