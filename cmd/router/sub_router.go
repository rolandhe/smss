package router

import (
	"encoding/binary"
	"fmt"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/cmd/repair"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"log"
	"net"
	"time"
)

const (
	PayloadSizePosInHeader = 4
	OneMsgHeaderSize       = 32
)

type subRouter struct {
	fstore store.Store
	noBinlog
}

type subLongtimeReader struct {
	store.MqBlockReader
}

func (lr *subLongtimeReader) Output(conn net.Conn, msgs []*store.ReadMessage) error {
	return batchMessageOut(conn, msgs)
}

func (r *subRouter) Router(conn net.Conn, commHeader *protocol.CommonHeader, worker standard.MessageWorking) error {
	header := &protocol.SubHeader{
		CommonHeader: commHeader,
	}

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
		return nets.OutputRecoverErr(conn, err.Error(), NetWriteTimeout)
	}
	if mqInfo == nil || mqInfo.IsInvalid() {
		log.Printf("mq not exist:%s\n", header.MQName)
		return nets.OutputRecoverErr(conn, "mq not exist", NetWriteTimeout)
	}

	var fileId, pos int64
	mqPath := r.fstore.GetMqPath(header.MQName)
	if fileId, pos, err = getSubPos(info.MessageId, mqPath); err != nil {
		log.Printf("message id not found:%s, %d\n", header.MQName, info.MessageId)
		return nets.OutputRecoverErr(conn, "message id not found", NetWriteTimeout)
	}

	reader, err := r.fstore.GetReader(header.MQName, info.Who, fileId, pos, info.BatchSize)
	if err != nil {
		return nets.OutputRecoverErr(conn, err.Error(), NetWriteTimeout)
	}

	tid := fmt.Sprintf("%s-%s", header.MQName, info.Who)

	return nets.LongTimeRun[store.ReadMessage](conn, "sub", tid, info.AckTimeout, NetWriteTimeout, &subLongtimeReader{
		MqBlockReader: reader,
	})
}

func getSubPos(messageId int64, mqPath string) (int64, int64, error) {
	var fileId int64
	var err error

	if messageId == 0 {
		fileId, err = standard.ReadFirstFileId(mqPath)
		if err != nil {
			return 0, 0, err
		}
		return fileId, 0, nil
	}
	//if messageId == -1 {
	//	fileId, err = standard.ReadMaxFileId(mqPath)
	//	if err != nil {
	//		return 0, 0, err
	//	}
	//	// todo
	//	return fileId - 1, 0, nil
	//}

	return repair.FindMqPosByMessageId(mqPath, messageId)
}

// readSubInfo, sub 格式
// 20 个字节的头：see SubHeader
// sub position, 订阅位点，8字节
// ack timeout, 如果 SubHeader HasAckTimeoutFlag is true
// who am i, 变长字符串，4 字节表示长度，紧跟着是这个长度的字节，字符串
func readSubInfo(conn net.Conn, header *protocol.SubHeader) (*protocol.SubInfo, error) {
	buf := make([]byte, 20)
	n := 12
	if header.HasAckTimeoutFlag() {
		n += 8
	}
	if err := nets.ReadAll(conn, buf[:n], NetReadTimeout); err != nil {
		return nil, err
	}

	messageId := int64(binary.LittleEndian.Uint64(buf))

	n = 8
	ackTimeout := protocol.AckDefaultTimeout
	if header.HasAckTimeoutFlag() {
		ackTimeout = time.Duration(binary.LittleEndian.Uint64(buf[n:]))
		n += 8
	}
	if ackTimeout <= 0 {
		ackTimeout = protocol.AckDefaultTimeout
	}
	l := int(binary.LittleEndian.Uint32(buf[n:]))
	whoBuff := make([]byte, l)
	if err := nets.ReadAll(conn, whoBuff, NetReadTimeout); err != nil {
		return nil, err
	}

	return &protocol.SubInfo{
		Who:        string(whoBuff),
		MessageId:  messageId,
		BatchSize:  header.GetBatchSize(),
		AckTimeout: ackTimeout,
	}, nil
}

func batchMessageOut(conn net.Conn, messages []*store.ReadMessage) error {
	buff := packageMessages(messages)
	return nets.WriteAll(conn, buff, NetWriteTimeout)
}

func packageMessages(messages []*store.ReadMessage) []byte {
	size := calPackageSize(messages)
	buf := make([]byte, size)
	binary.LittleEndian.PutUint16(buf[:2], protocol.OkCode)
	buf[2] = byte(len(messages))
	nextBuf := buf[protocol.RespHeaderSize:]

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
	return protocol.RespHeaderSize + bodySize
}
