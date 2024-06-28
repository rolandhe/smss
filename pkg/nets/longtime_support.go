package nets

import (
	"errors"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/standard"
	"io"
	"log"
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
			log.Printf("tid=%s,longtimeReadMonitor of %s met err,and exit monitor:%v\n", tid, biz, err)
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

func LongTimeRun[T any](conn net.Conn, biz, tid string, ackTimeout time.Duration, reader LongtimeReader[T]) error {
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
				log.Printf("tid=%s,read ack timeout\n", tid)
				return errors.New("read ack timeout")
			}
			if ackCode == protocol.ConnPeerClosed {
				log.Printf("tid=%s,conn peer closed\n", tid)
				return errors.New("conn peer closed")
			}
			if ackCode == protocol.SubAckWithEnd {
				log.Printf("tid=%s,client send ack and closed\n", tid)
				return nil
			}
			if ackCode != protocol.SubAck {
				log.Printf("tid=%s,client send invalid ack code %d\n", tid, ackCode)
				return errors.New("invalid ack")
			}
			continue
		}
		if err == standard.WaitNewTimeoutErr {
			err = OutAlive(conn, protocol.LongtimeBlockWriteTimeout)
			log.Printf("tid=%s,sub wait new data timeout, send alive:%v\n", tid, err)
			if err != nil {
				return err
			}
			continue
		}
		if err == standard.PeerClosedErr {
			log.Printf("tid=%s,PeerClosedErr:%v\n", tid, err)
			return err
		}
		if err == standard.MqWriterTermiteErr {
			log.Printf("tid=%s,MqWriterTermiteErr:%v\n", tid, err)
			return OutSubEnd(conn, protocol.LongtimeBlockWriteTimeout)
		}
		return OutputRecoverErr(conn, err.Error())
	}
}
