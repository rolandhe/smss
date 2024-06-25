package protocol

import (
	"encoding/binary"
	"log"
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
	HeaderSize int = 20

	//Pub = 0
	//Sub = 1
)

const (
	CommandSub      CommandEnum = 0
	CommandPub      CommandEnum = 1
	CommandCreateMQ CommandEnum = 2
	CommandDeleteMQ CommandEnum = 3
	CommandChangeLf CommandEnum = 4

	CommandDelay CommandEnum = 16
	CommandAlive CommandEnum = 17
	CommandList  CommandEnum = 100

	CommandDelayApply CommandEnum = 101
)

type CommonHeader struct {
	buf []byte
	// mq name
	MQName  string
	TraceId string
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
	// pub/sub 1 byte
	// mq name len, 2
	// batchSize 4
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
	// mq name len, 2
	// batchSize 1
	// ack timeout flag 1
	// reserve 14
	// traceId len 1
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
	FileId     int64
	Pos        int64
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

type RawMessage struct {
	Command   CommandEnum
	MqName    string
	Timestamp int64

	TraceId string

	Body any
}

type DecodedRawMessage struct {
	RawMessage
	PayloadLen int
}

type PubPayload struct {
	Payload []byte
}

type DDLPayload struct {
	Payload []byte
}

type DelayPayload struct {
	Payload   []byte
	DelayTime int64
}

type DelayApplyPayload struct {
	// delay id + delay time + pub message
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
	ch := make(chan struct{})

	holder := &lockerHolder{
		ch:      make(chan struct{}),
		who:     who,
		traceId: traceId,
	}

	old, loaded := l.Map.LoadOrStore(name, holder)
	oldHolder := old.(*lockerHolder)
	if loaded {
		return nil, func(d time.Duration) bool {
			select {
			case <-oldHolder.ch:
				return true
			case <-time.After(d):
				log.Printf("tid=%s,wait lock timeout, by %s(src-tid=%s) locked\n", traceId, oldHolder.who, oldHolder.traceId)
				return false
			}
		}
	}
	return func() {
		l.Map.Delete(name)
		close(ch)
	}, nil
}

type DelMqFileExecutor interface {
	Submit(mqName, who string, traceId string) func(d time.Duration) bool
	GetDeleteFileLocker() *DelFileLock
}
