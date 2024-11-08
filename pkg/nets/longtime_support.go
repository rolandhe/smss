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

func longtimeReadMonitor(biz string, conn net.Conn, resultChan chan int, clientClosedNotify *store.ClientClosedNotifyEquipment, tid string) {
	for {
		code, err := InputAck(conn, time.Second*5)
		if clientClosedNotify.ClientClosedFlag.Load() {
			break
		}
		if err != nil && IsTimeoutError(err) {
			continue
		}
		if err != nil {
			logger.Infof("tid=%s,longtimeReadMonitor of %s met err,and exit monitor:%v", tid, biz, err)
			clientClosedNotify.ClientClosedFlag.Store(true)
			close(clientClosedNotify.ClientClosedNotifyChan)
			break
		}
		waitFunc := func() bool {
			timer := time.NewTimer(time.Millisecond * 100)
			defer timer.Stop()
			select {
			case resultChan <- code:
				return true
			case <-timer.C:
				logger.Infof("tid=%s,longtimeReadMonitor write ack code timeout,100 ms", tid)
				return false
			}
		}
		if !waitFunc() {
			break
		}
	}

}

type LongtimeReader[T any] interface {
	Read(clientTermiteNotify *store.ClientClosedNotifyEquipment) ([]*T, error)
	Output(conn net.Conn, msgs []*T) error
	io.Closer
}

func LongTimeRun[T any](conn net.Conn, biz, tid string, ackTimeout, writeTimeout time.Duration, reader LongtimeReader[T]) error {
	var err error
	clientClosedNotify := &store.ClientClosedNotifyEquipment{
		ClientClosedNotifyChan: make(chan struct{}),
	}
	resultChan := make(chan int, 1)
	go longtimeReadMonitor(biz, conn, resultChan, clientClosedNotify, tid)

	defer func() {
		reader.Close()
		clientClosedNotify.ClientClosedFlag.Store(true)
	}()
	var logCounter int64 = 0

	for {
		var msgs []*T
		msgs, err = reader.Read(clientClosedNotify)

		if logCounter%conf.LogSample == 0 {
			logger.Infof("tid=%s,after-sub-read,err:%v", tid, err)
		}
		logCounter++

		if err == nil {
			if clientClosedNotify.ClientClosedFlag.Load() {
				return errors.New("conn peer closed")
			}
			if err = reader.Output(conn, msgs); err != nil {
				return err
			}
			var ackCode int

			waitFunc := func() error {
				timerAck := time.NewTimer(ackTimeout)
				defer timerAck.Stop()
				select {
				case <-clientClosedNotify.ClientClosedNotifyChan:
					logger.Infof("tid=%s,conn peer closed", tid)
					return errors.New("conn peer closed")
				case ackCode = <-resultChan:
				case <-timerAck.C:
					logger.Infof("tid=%s,read ack timeout", tid)
					return errors.New("read ack timeout")
				}
				return nil
			}

			if err = waitFunc(); err != nil {
				return err
			}

			if ackCode == protocol.SubAckWithEnd {
				logger.Infof("tid=%s,client send ack and closed", tid)
				return nil
			}
			if ackCode != protocol.SubAck {
				logger.Infof("tid=%s,client send invalid ack code %d", tid, ackCode)
				return errors.New("invalid ack")
			}
			continue
		}
		if errors.Is(err, standard.WaitNewTimeoutErr) {
			err = OutAlive(conn, writeTimeout)
			logger.Infof("tid=%s,sub wait new data timeout, send alive:%v", tid, err)
			if err != nil {
				return err
			}
			continue
		}
		if errors.Is(err, standard.PeerClosedErr) {
			logger.Infof("tid=%s,PeerClosedErr:%v", tid, err)
			return err
		}
		if errors.Is(err, standard.TopicWriterTermiteErr) {
			logger.Infof("tid=%s,TopicWriterTermiteErr:%v", tid, err)
			return OutSubEnd(conn, writeTimeout)
		}

		return OutputRecoverErr(conn, err.Error(), writeTimeout)
	}
}
