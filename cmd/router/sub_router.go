package router

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"log"
	"net"
	"sync/atomic"
	"time"
)

const (
	subCommonReadTimeout  = time.Millisecond * 2000
	subCommonWriteTimeout = time.Millisecond * 3000
)

const (
	PayloadSizePosInHeader = 4
	OneMsgHeaderSize       = 32
)

const (
	ConnPeerClosed = -100
)

type subRouter struct {
	fstore store.Store
	noBinlog
}

func subReadMonitor(conn net.Conn, recvChan chan int, endFlag *atomic.Bool, subTraceId string) {
	f := func(v int) {
		select {
		case recvChan <- v:
		case <-time.After(time.Second):

		}
	}
	for {
		code, err := inputAck(conn, time.Second*5)
		if endFlag.Load() {
			break
		}
		if err != nil && nets.IsTimeoutError(err) {
			continue
		}
		if err != nil {
			log.Printf("tid=%s,subReadMonitor met err,and exit monitor:%v\n", subTraceId, err)
			f(-100)
			break
		}
		f(code)
	}

}

func (r *subRouter) Router(conn net.Conn, commHeader *protocol.CommonHeader, worker MessageWorking) error {
	header := &protocol.SubHeader{
		CommonHeader: commHeader,
	}

	conn.SetReadDeadline(time.Now().Add(subCommonReadTimeout))
	info, err := readSubInfo(conn, header)
	if err != nil {
		return err
	}
	if info.BatchSize <= 0 {
		info.BatchSize = protocol.DefaultSubBatchSize
	}

	var mqInfo *store.MqInfo
	mqInfo, err = r.fstore.GetMqInfoReader().GetMQInfo(header.MQName)
	if err != nil {
		return nets.OutputRecoverErr(conn, err.Error())
	}
	if mqInfo == nil || mqInfo.IsInvalid() {
		log.Printf("mq not exist:%s\n", header.MQName)
		return nets.OutputRecoverErr(conn, "mq not exist")
	}

	reader, err := r.fstore.GetReader(header.MQName, info.Who, info.FileId, info.Pos, info.BatchSize)
	if err != nil {
		return nets.OutputRecoverErr(conn, err.Error())
	}

	subTraceId := fmt.Sprintf("%s-%s", header.MQName, info.Who)

	recvCh := make(chan int, 1)
	var endFlag atomic.Bool
	go subReadMonitor(conn, recvCh, &endFlag, subTraceId)

	defer func() {
		reader.Close()
		endFlag.Store(true)
	}()
	for {
		var msgs []*store.ReadMessage
		msgs, err = reader.Read(recvCh)

		if err == nil {
			if err = batchMessageOut(conn, msgs); err != nil {
				return err
			}
			var ackCode int
			select {
			case ackCode = <-recvCh:
			case <-time.After(info.AckTimeout):
				log.Printf("tid=%s,read ack timeout\n", subTraceId)
				return errors.New("read ack timeout")
			}
			if ackCode == ConnPeerClosed {
				log.Printf("tid=%s,conn peer closed\n", subTraceId)
				return errors.New("conn peer closed")
			}
			if ackCode == protocol.SubAckWithEnd {
				log.Printf("tid=%s,client send ack and closed\n", subTraceId)
				return nil
			}
			if ackCode != protocol.SubAck {
				log.Printf("tid=%s,client send invalid ack code %d\n", subTraceId, ackCode)
				return errors.New("invalid ack")
			}

			continue
		}
		if err == standard.WaitNewTimeoutErr {
			err = nets.OutAlive(conn, subCommonWriteTimeout)
			log.Printf("tid=%s,sub wait new data timeout, send alive:%v\n", subTraceId, err)
			if err != nil {
				return err
			}
			continue
		}
		if err == standard.PeerClosedErr {
			log.Printf("tid=%s,PeerClosedErr:%v\n", subTraceId, err)
			return err
		}
		if err == standard.MqWriterTermiteErr {
			log.Printf("tid=%s,MqWriterTermiteErr:%v\n", subTraceId, err)
			return nets.OutSubEnd(conn, subCommonWriteTimeout)
		}
		return nets.OutputRecoverErr(conn, err.Error())
	}
}

// readSubInfo, sub 格式
// 20 个字节的头：see SubHeader
// who am i, 变长字符串，4 字节表示长度，紧跟着是这个长度的字节，字符串
// ack timeout, 如果 SubHeader HasAckTimeoutFlag is true
// sub position, 订阅位点，16字节
func readSubInfo(conn net.Conn, header *protocol.SubHeader) (*protocol.SubInfo, error) {
	buf := make([]byte, 24)
	if err := nets.ReadAll(conn, buf[:4]); err != nil {
		return nil, err
	}
	l := int(binary.LittleEndian.Uint32(buf[:4]))
	whoBuff := make([]byte, l)
	if err := nets.ReadAll(conn, whoBuff); err != nil {
		return nil, err
	}

	readBuf := buf[8:]
	iBuf := buf[8:]
	if header.HasAckTimeoutFlag() {
		readBuf = buf
	}
	if err := nets.ReadAll(conn, readBuf); err != nil {
		return nil, err
	}
	ackTimeout := protocol.AckDefaultTimeout
	if header.HasAckTimeoutFlag() {
		ackTimeout = time.Duration(binary.LittleEndian.Uint64(buf[:8]))
	}
	if ackTimeout <= 0 {
		ackTimeout = protocol.AckDefaultTimeout
	}

	return &protocol.SubInfo{
		Who:        string(whoBuff),
		FileId:     int64(binary.LittleEndian.Uint64(iBuf)),
		Pos:        int64(binary.LittleEndian.Uint64(iBuf[8:])),
		BatchSize:  header.GetBatchSize(),
		AckTimeout: ackTimeout,
	}, nil
}

func batchMessageOut(conn net.Conn, messages []*store.ReadMessage) error {
	buff := packageMessages(messages)
	conn.SetWriteDeadline(time.Now().Add(subCommonWriteTimeout))
	return nets.WriteAll(conn, buff)
}

func packageMessages(messages []*store.ReadMessage) []byte {
	size := calPackageSize(messages)
	buf := make([]byte, size)
	binary.LittleEndian.PutUint16(buf[:2], protocol.OkCode)
	buf[2] = byte(len(messages))
	nextBuf := buf[nets.RespHeaderSize:]

	payloadSize := 0
	for _, msg := range messages {
		binary.LittleEndian.PutUint64(nextBuf, uint64(msg.Ts))
		binary.LittleEndian.PutUint64(nextBuf[8:], uint64(msg.Id))
		binary.LittleEndian.PutUint64(nextBuf[16:], uint64(msg.NextPos.FileId))
		binary.LittleEndian.PutUint64(nextBuf[24:], uint64(msg.NextPos.Pos))
		n := copy(nextBuf[OneMsgHeaderSize:], msg.PayLoad)

		msgSize := n + OneMsgHeaderSize
		payloadSize += msgSize
		nextBuf = nextBuf[msgSize:]
	}
	binary.LittleEndian.PutUint32(buf[PayloadSizePosInHeader:], uint32(payloadSize))
	return buf
}

func calPackageSize(messages []*store.ReadMessage) int {
	bodySize := 0
	for _, msg := range messages {
		bodySize += OneMsgHeaderSize + len(msg.PayLoad)
	}
	return nets.RespHeaderSize + bodySize
}

func inputAck(conn net.Conn, timeout time.Duration) (int, error) {
	conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 2)
	if err := nets.ReadAll(conn, buf); err != nil {
		return 0, err
	}

	code := binary.LittleEndian.Uint16(buf)

	return int(code), nil
}
