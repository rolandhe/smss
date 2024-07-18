package badger_meta

import (
	"encoding/binary"
	"github.com/rolandhe/smss/store"
)

var (
	lifePrefix      = []byte("lf@")
	normalPrefix    = []byte("norm@")
	delayPrefix     = []byte("delay@")
	lifeValueHolder = []byte{0}
)

const (
	roleKey = "global@role"
)

func topicLifetimeName(topicName string, expireAt int64) []byte {
	buf := make([]byte, len(lifePrefix)+8+len(topicName))
	n := copy(buf, lifePrefix)
	binary.LittleEndian.PutUint64(buf[n:], uint64(expireAt))
	copy(buf[n+8:], topicName)
	return buf
}

type topicMetaValue struct {
	createTime         int64
	expireAtTime       int64
	createEventId      int64
	stateChangeTime    int64
	stateChangeEventId int64
	state              store.TopicStateEnum
}

func (tmv *topicMetaValue) toBytes() []byte {
	buf := make([]byte, 41)
	binary.LittleEndian.PutUint64(buf, uint64(tmv.createTime))
	binary.LittleEndian.PutUint64(buf[8:], uint64(tmv.expireAtTime))
	binary.LittleEndian.PutUint64(buf[16:], uint64(tmv.createEventId))
	binary.LittleEndian.PutUint64(buf[24:], uint64(tmv.stateChangeTime))
	binary.LittleEndian.PutUint64(buf[32:], uint64(tmv.stateChangeEventId))
	buf[40] = byte(tmv.state)
	return buf
}

func (tmv *topicMetaValue) fromBytes(buf []byte) {
	tmv.createTime = int64(binary.LittleEndian.Uint64(buf))
	tmv.expireAtTime = int64(binary.LittleEndian.Uint64(buf[8:]))
	tmv.createEventId = int64(binary.LittleEndian.Uint64(buf[16:]))
	tmv.stateChangeTime = int64(binary.LittleEndian.Uint64(buf[24:]))
	tmv.stateChangeEventId = int64(binary.LittleEndian.Uint64(buf[32:]))
	tmv.state = store.TopicStateEnum(buf[40])

}

func (tmv *topicMetaValue) toTopicInfo(topicName string) *store.TopicInfo {
	return &store.TopicInfo{
		Name:               topicName,
		CreateTimeStamp:    tmv.createTime,
		ExpireAt:           tmv.expireAtTime,
		CreateEventId:      tmv.createEventId,
		StateChangeTime:    tmv.stateChangeTime,
		StateChangeEventId: tmv.stateChangeEventId,
		State:              tmv.state,
	}
}

func (tmv *topicMetaValue) fromTopicInfo(info *store.TopicInfo) {
	tmv.createTime = info.CreateTimeStamp
	tmv.expireAtTime = info.ExpireAt
	tmv.createEventId = info.CreateEventId
	tmv.stateChangeTime = info.StateChangeTime
	tmv.stateChangeEventId = info.StateChangeEventId
	tmv.state = info.State
}
