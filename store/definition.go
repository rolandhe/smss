package store

import (
	"io"
	"time"
)

type MqStateEnum int8

const (
	MqStateNormal  MqStateEnum = 0
	MqStateDeleted MqStateEnum = 1
)

type MqInfo struct {
	Name               string      `json:"name" :"name"`
	CreateTimeStamp    int64       `json:"createTimeStamp"`
	ExpireAt           int64       `json:"expireAt"`
	CreateEventId      int64       `json:"createEventId"`
	StateChangeTime    int64       `json:"stateChangeTime"`
	StateChangeEventId int64       `json:"stateChangeEventId"`
	State              MqStateEnum `json:"state"`
}

func (info *MqInfo) IsTemp() bool {
	return info.ExpireAt > 0
}

func (info *MqInfo) IsInvalid() bool {
	return info.State == MqStateDeleted || (info.IsTemp() && time.Now().UnixMilli() >= info.ExpireAt)
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
	RemoveDelayByName(data []byte, mqName string) error
	ExistDelay(key []byte) (bool, error)

	CopyCreateMq(info *MqInfo) error
	DeleteMQ(mqName string, force bool) (bool, error)
}

type MqInfoReader interface {
	GetMQSimpleInfoList() ([]*MqInfo, error)
	GetMQInfo(mqName string) (*MqInfo, error)
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
	MQDir     = "mq"
	MetaDir   = "meta"
)

type Meta interface {
	CreateMQ(mqName string, defaultLifetime int64, eventId int64) (*MqInfo, error)
	//ChangeMQLife(mqName string, life int64, eventId int64) error

	SaveDelay(mqName string, payload []byte) error

	io.Closer
	ManagerMeta
	MqInfoReader
	Scanner
}

type BlockReader[T any] interface {
	Read(endNotify <-chan int) ([]*T, error)
	Init(fileId, pos int64) error
	io.Closer
}

type MqBlockReader interface {
	BlockReader[ReadMessage]
}

type Store interface {
	io.Closer
	Save(mqName string, messages []*MQMessage) error

	GetReader(mqName, who string, fileId, pos int64, batchSize int) (MqBlockReader, error)

	CreateMq(mqName string, life int64, eventId int64) error

	ForceDeleteMQ(mqName string, cb func() error) error

	SaveDelayMsg(mqName string, payload []byte) error

	//ChangeMqLife(mqName string, life int64, eventId int64) error

	GetManagerMeta() ManagerMeta

	GetMqInfoReader() MqInfoReader

	GetScanner() Scanner

	GetMqPath(mqName string) string
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
