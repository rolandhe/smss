package badger_meta

import (
	"encoding/binary"
	"fmt"
	"time"
)

var (
	lifePrefix      = []byte("lf@")
	normalPrefix    = []byte("norm@")
	delayPrefix     = []byte("delay@")
	lifeValueHolder = []byte{0}
)

const (
	globalIdName = "global@id"

	roleKey = "global@role"
)

func mqIdName(mqName string) string {
	return fmt.Sprintf("%s@ID", mqName)
}

func mqLifetimeName(mqName string, expireAt int64) []byte {
	buf := make([]byte, len(lifePrefix)+8+len(mqName))
	n := copy(buf, lifePrefix)
	binary.LittleEndian.PutUint64(buf[n:], uint64(expireAt))
	copy(buf[n+8:], mqName)
	return buf
}

func toMqMainValue(createTime, exportAt int64) []byte {
	buf := make([]byte, 25)

	binary.LittleEndian.PutUint64(buf, uint64(createTime))
	binary.LittleEndian.PutUint64(buf[8:], uint64(exportAt))
	binary.LittleEndian.PutUint64(buf[16:], uint64(time.Now().UnixMilli()))
	buf[24] = 0
	return buf
}
func fromMqMainValue(buf []byte) (int64, int64, int64, int8) {

	createTime := binary.LittleEndian.Uint64(buf)
	expireAt := binary.LittleEndian.Uint64(buf[8:])
	stateChange := binary.LittleEndian.Uint64(buf[16:])

	return int64(createTime), int64(expireAt), int64(stateChange), int8(buf[24])
}
