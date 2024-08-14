package router

import (
	"encoding/binary"
	"fmt"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/cmd/repair"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
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
	store.TopicBlockReader
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
	tid := fmt.Sprintf("%s-%s", header.TopicName, info.Who)

	logger.Get().Infof("tid=%s,recv subinfo,eventId: %d", tid, info.EventId)
	var topicInfo *store.TopicInfo
	topicInfo, err = r.fstore.GetTopicInfoReader().GetTopicInfo(header.TopicName)
	if err != nil {
		return nets.OutputRecoverErr(conn, err.Error(), NetWriteTimeout)
	}
	if topicInfo == nil || topicInfo.IsInvalid() {
		logger.Get().Infof("tid=%s,topic not exist:%s", tid, header.TopicName)
		return nets.OutputRecoverErr(conn, "topic not exist", NetWriteTimeout)
	}

	var fileId, pos int64
	topicPath := r.fstore.GetTopicPath(header.TopicName)
	if fileId, pos, err = getSubPos(info.EventId, topicPath); err != nil {
		logger.Get().Infof("tid=%s,event id not found: %d,err:%v", tid, info.EventId, err)
		return nets.OutputRecoverErr(conn, "event id not found", NetWriteTimeout)
	}

	reader, err := r.fstore.GetReader(header.TopicName, info.Who, fileId, pos, info.BatchSize)
	if err != nil {
		logger.Get().Infof("tid=%s,eventId=%d,get reader err:%v", tid, info.EventId, err)
		return nets.OutputRecoverErr(conn, err.Error(), NetWriteTimeout)
	}

	logger.Get().Infof("tid=%s,subinfo check ok,start to send messages,eventId: %d", tid, info.EventId)
	return nets.LongTimeRun[store.ReadMessage](conn, "sub", tid, info.AckTimeout, NetWriteTimeout, &subLongtimeReader{
		TopicBlockReader: reader,
	})
}

func getSubPos(eventId int64, topicPath string) (int64, int64, error) {
	var fileId int64
	var err error

	if eventId == 0 {
		fileId, err = standard.ReadFirstFileId(topicPath)
		if err != nil {
			return 0, 0, err
		}
		return fileId, 0, nil
	}

	return repair.FindTopicPosByEventId(topicPath, eventId)
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

	eventId := int64(binary.LittleEndian.Uint64(buf))

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
		EventId:    eventId,
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
		binary.LittleEndian.PutUint64(nextBuf[8:], uint64(msg.EventId))
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
