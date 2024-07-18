package protocol

import (
	"encoding/binary"
	"github.com/rolandhe/smss/pkg/logger"
	"sync"
	"time"
)

// response code
const (
	OkCode     = 200
	AliveCode  = 201
	SubEndCode = 255
	ErrCode    = 400
)

const (
	AckDefaultTimeout   = time.Millisecond * 3000
	DefaultSubBatchSize = 1

	SubAck        = 0
	SubAckWithEnd = 1
)

const (
	HeaderSize     int = 20
	RespHeaderSize     = 10
	ConnPeerClosed     = -100
)

const (
	CommandSub      CommandEnum = 0
	CommandPub         CommandEnum = 1
	CommandCreateTopic CommandEnum = 2
	CommandDeleteTopic CommandEnum = 3

	CommandDelay CommandEnum = 16
	CommandAlive CommandEnum = 17

	CommandReplica   CommandEnum = 64
	CommandValidList CommandEnum = 99
	CommandList      CommandEnum = 100

	CommandDelayApply CommandEnum = 101
)

const (
	RawMessageBase    RawMessageSourceEnum = 0
	RawMessageReplica RawMessageSourceEnum = 1
)

type CommonHeader struct {
	buf []byte
	// topic name
	TopicName string
	TraceId   string
}

func NewCommonHeader(buf []byte) *CommonHeader {
	return &CommonHeader{
		buf: buf,
	}
}

func (h *CommonHeader) GetCmd() CommandEnum {
	return CommandEnum(h.buf[0])
}
func (h *CommonHeader) GetMqNameLen() int {
	size := binary.LittleEndian.Uint16(h.buf[1:])
	return int(size)
}

func (h *CommonHeader) GetTraceIdLen() int {
	return int(h.buf[19])
}

type PubProtoHeader struct {
	// 20字节
	// cmd 1 byte
	// topic name len, 2
	// payloadSize 4
	// reserve 12
	// traceId len 1
	*CommonHeader
}

func (ph *PubProtoHeader) GetPayloadSize() int {
	size := binary.LittleEndian.Uint32(ph.buf[3:])
	return int(size)
}

type SubHeader struct {
	// 20字节
	// pub/sub 1 byte
	// topic name len, 2
	// batchSize 1
	// ack timeout flag 1
	// reserve 14
	// traceId len 1

	// next:
	// pos, 8
	// ack timeout(optional),8,
	// who am i, var string

	*CommonHeader
}

func (sh *SubHeader) GetBatchSize() int {
	size := sh.buf[3]
	return int(size)
}

func (sh *SubHeader) HasAckTimeoutFlag() bool {
	flag := sh.buf[4]
	return flag == 1
}

type SubInfo struct {
	Who        string
	EventId    int64
	BatchSize  int
	AckTimeout time.Duration
}

type CommandEnum uint8

func (ce CommandEnum) Int() int {
	return int(ce)
}

func (ce CommandEnum) Byte() byte {
	return byte(ce)
}

type RawMessageSourceEnum uint8

func (ce RawMessageSourceEnum) Int() int {
	return int(ce)
}

func (ce RawMessageSourceEnum) Byte() byte {
	return byte(ce)
}

type ReplicaHeader struct {
	// 20字节
	// pub/sub 1 byte
	// topic name len, 2
	// reserve 17

	// next:
	// messageId,8

	*CommonHeader
}

type RawMessage struct {
	Src       RawMessageSourceEnum
	WriteTime int64
	Command   CommandEnum
	TopicName string
	// 服务端收到pub信息时的时间戳
	Timestamp int64
	EventId   int64
	TraceId   string
	Body      any
	Skip      bool
}

func (rm *RawMessage) GetDelay() int64 {
	return time.Now().UnixMilli() - rm.WriteTime
}

func (rm *RawMessage) Cost() int64 {
	return time.Now().UnixMilli() - rm.Timestamp
}

type DecodedRawMessage struct {
	RawMessage
	PayloadLen int
}

type PubPayload struct {
	Payload   []byte
	BatchSize int
}

type DDLPayload struct {
	Payload []byte
}

type DelayPayload struct {
	Payload []byte
}

type DelayApplyPayload struct {
	// delay time + eventId  + pub message
	Payload []byte
}

type DelFileLock struct {
	sync.Map
}

type lockerHolder struct {
	ch      chan struct{}
	who     string
	traceId string
}

func (l *DelFileLock) Lock(name, who string, traceId string) (func(), func(d time.Duration) bool) {
	holder := &lockerHolder{
		ch:      make(chan struct{}),
		who:     who,
		traceId: traceId,
	}

	elem, loaded := l.Map.LoadOrStore(name, holder)
	if loaded {
		oldHolder := elem.(*lockerHolder)
		return nil, func(d time.Duration) bool {
			select {
			case <-oldHolder.ch:
				return true
			case <-time.After(d):
				logger.Get().Infof("tid=%s,wait lock %s timeout,i am %s, by %s(src-tid=%s) locked", traceId, name, who, oldHolder.who, oldHolder.traceId)
				return false
			}
		}
	}
	return func() {
		l.Map.Delete(name)
		close(holder.ch)
	}, nil
}

type DelMqFileExecutor interface {
	Submit(topicName, who string, traceId string) func(d time.Duration) bool
	GetDeleteFileLocker() *DelFileLock
}
