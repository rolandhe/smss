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

func mqLifetimeName(mqName string, expireAt int64) []byte {
	buf := make([]byte, len(lifePrefix)+8+len(mqName))
	n := copy(buf, lifePrefix)
	binary.LittleEndian.PutUint64(buf[n:], uint64(expireAt))
	copy(buf[n+8:], mqName)
	return buf
}

type mqMetaValue struct {
	createTime         int64
	expireAtTime       int64
	createEventId      int64
	stateChangeTime    int64
	stateChangeEventId int64
	state              store.MqStateEnum
}

func (mmv *mqMetaValue) toBytes() []byte {
	buf := make([]byte, 41)
	binary.LittleEndian.PutUint64(buf, uint64(mmv.createTime))
	binary.LittleEndian.PutUint64(buf[8:], uint64(mmv.expireAtTime))
	binary.LittleEndian.PutUint64(buf[16:], uint64(mmv.createEventId))
	binary.LittleEndian.PutUint64(buf[24:], uint64(mmv.stateChangeTime))
	binary.LittleEndian.PutUint64(buf[32:], uint64(mmv.stateChangeEventId))
	buf[40] = byte(mmv.state)
	return buf
}

func (mmv *mqMetaValue) fromBytes(buf []byte) {
	mmv.createTime = int64(binary.LittleEndian.Uint64(buf))
	mmv.expireAtTime = int64(binary.LittleEndian.Uint64(buf[8:]))
	mmv.createEventId = int64(binary.LittleEndian.Uint64(buf[16:]))
	mmv.stateChangeTime = int64(binary.LittleEndian.Uint64(buf[24:]))
	mmv.stateChangeEventId = int64(binary.LittleEndian.Uint64(buf[32:]))
	mmv.state = store.MqStateEnum(buf[40])

}

func (mmv *mqMetaValue) toMqInfo(mqName string) *store.MqInfo {
	return &store.MqInfo{
		Name:               mqName,
		CreateTimeStamp:    mmv.createTime,
		ExpireAt:           mmv.expireAtTime,
		CreateEventId:      mmv.createEventId,
		StateChangeTime:    mmv.stateChangeTime,
		StateChangeEventId: mmv.stateChangeEventId,
		State:              mmv.state,
	}
}

func (mmv *mqMetaValue) fromMqInfo(info *store.MqInfo) {
	mmv.createTime = info.CreateTimeStamp
	mmv.expireAtTime = info.ExpireAt
	mmv.createEventId = info.CreateEventId
	mmv.stateChangeTime = info.StateChangeTime
	mmv.stateChangeEventId = info.StateChangeEventId
	mmv.state = info.State
}

//func toMqMainValue(createTime, exportAt int64, eventId int64) []byte {
//	buf := make([]byte, 41)
//
//	binary.LittleEndian.PutUint64(buf, uint64(createTime))
//	binary.LittleEndian.PutUint64(buf[8:], uint64(exportAt))
//	// 状态改变时间
//	binary.LittleEndian.PutUint64(buf[16:], uint64(createTime))
//	// 状态
//	buf[24] = 0
//	// 创建事件
//	binary.LittleEndian.PutUint64(buf[25:], uint64(eventId))
//	// 修改状态的事件id
//	binary.LittleEndian.PutUint64(buf[33:], uint64(eventId))
//	return buf
//}
//func fromMqMainValue(buf []byte) (int64, int64, int64, int8) {
//
//	createTime := binary.LittleEndian.Uint64(buf)
//	expireAt := binary.LittleEndian.Uint64(buf[8:])
//	stateChange := binary.LittleEndian.Uint64(buf[16:])
//
//	return int64(createTime), int64(expireAt), int64(stateChange), int8(buf[24])
//}
