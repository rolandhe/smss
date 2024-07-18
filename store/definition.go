package store

import (
	"io"
	"time"
)

type TopicStateEnum int8

const (
	TopicStateNormal  TopicStateEnum = 0
	TopicStateDeleted TopicStateEnum = 1
)

type TopicInfo struct {
	Name               string         `json:"name" :"name"`
	CreateTimeStamp    int64          `json:"createTimeStamp"`
	ExpireAt           int64          `json:"expireAt"`
	CreateEventId      int64          `json:"createEventId"`
	StateChangeTime    int64          `json:"stateChangeTime"`
	StateChangeEventId int64          `json:"stateChangeEventId"`
	State              TopicStateEnum `json:"state"`
}

func (info *TopicInfo) IsTemp() bool {
	return info.ExpireAt > 0
}

func (info *TopicInfo) IsInvalid() bool {
	return info.State == TopicStateDeleted || (info.IsTemp() && time.Now().UnixMilli() >= info.ExpireAt)
}

type DelayItem struct {
	Payload []byte
	Key     []byte
	MqName  string
}

type ManagerMeta interface {
	SetInstanceRole(role InstanceRoleEnum) error
	GetInstanceRole() (InstanceRoleEnum, error)

	RemoveDelay(key []byte) error
	RemoveDelayByName(data []byte, topicName string) error
	ExistDelay(key []byte) (bool, error)

	CopyCreateTopic(info *TopicInfo) error
	DeleteTopic(topicName string, force bool) (bool, error)
}

type TopicInfoReader interface {
	GetTopicSimpleInfoList() ([]*TopicInfo, error)
	GetTopicInfo(topicName string) (*TopicInfo, error)
}

type Scanner interface {
	ScanExpireMqs() ([]string, int64, error)
	ScanDelays(batchSize int) ([]*DelayItem, int64, error)
}

type InstanceRoleEnum byte

func (role InstanceRoleEnum) AsBytes() []byte {
	return []byte{byte(role)}
}

const (
	Master InstanceRoleEnum = 0
	Slave  InstanceRoleEnum = 1
	Unset  InstanceRoleEnum = 2
)

const (
	BinlogDir = "binlog"
	TopicDir  = "topic"
	MetaDir   = "meta"
)

type Meta interface {
	CreateTopic(topicName string, defaultLifetime int64, eventId int64) (*TopicInfo, error)

	SaveDelay(topicName string, payload []byte) error

	io.Closer
	ManagerMeta
	TopicInfoReader
	Scanner
}

type BlockReader[T any] interface {
	Read(endNotify <-chan int) ([]*T, error)
	Init(fileId, pos int64) error
	io.Closer
}

type TopicBlockReader interface {
	BlockReader[ReadMessage]
}

type Store interface {
	io.Closer
	Save(topicName string, messages []*MQMessage) error

	GetReader(topicName, who string, fileId, pos int64, batchSize int) (TopicBlockReader, error)

	CreateTopic(topicName string, life int64, eventId int64) error

	ForceDeleteTopic(topicName string, cb func() error) error

	SaveDelayMsg(topicName string, payload []byte) error

	GetManagerMeta() ManagerMeta

	GetTopicInfoReader() TopicInfoReader

	GetScanner() Scanner

	GetTopicPath(topicName string) string
}

type MsgHeader struct {
	Name  string
	Value string
}

type MQMessage struct {
	SeqId     int64
	SrcFileId int64
	SrcPos    int64
	Content   []byte
}

type ReadMessage struct {
	Ts      int64
	Id      int64
	PayLoad []byte
	NextPos struct {
		FileId int64
		Pos    int64
	}
}
