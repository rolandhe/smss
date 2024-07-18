package protocol

import (
	"encoding/binary"
	"github.com/rolandhe/smss/pkg/dir"
	"github.com/rolandhe/smss/store"
)

func ParsePayload(payload []byte, fileId, pos int64, startEventId int64) ([]*store.TopicMessage, error) {
	var ret []*store.TopicMessage
	if len(payload) <= 8 {
		return nil, dir.NewBizError("invalid message format")
	}
	for {
		contentSize := int(binary.LittleEndian.Uint32(payload))
		if contentSize <= 0 {
			return nil, dir.NewBizError("invalid message format")
		}
		oneMsgLen := contentSize + 8
		restLen := len(payload)
		if restLen < oneMsgLen {
			return nil, dir.NewBizError("invalid message format")
		}
		content := make([]byte, oneMsgLen)
		copy(content, payload[:oneMsgLen])

		ret = append(ret, &store.TopicMessage{
			EventId:   startEventId,
			SrcFileId: fileId,
			SrcPos:    pos,
			Content:   content,
		})

		startEventId++

		if restLen == oneMsgLen {
			break
		}

		if restLen-oneMsgLen <= 8 {
			return nil, dir.NewBizError("invalid message format")
		}
		payload = payload[oneMsgLen:]
	}

	return ret, nil
}

func CheckPayload(payload []byte) (bool, int) {
	if len(payload) <= 8 {
		return false, 0
	}
	count := 0
	for {
		contentSize := int(binary.LittleEndian.Uint32(payload))
		if contentSize <= 0 {
			return false, 0
		}
		payload = payload[8:]
		restLen := len(payload)
		if restLen < contentSize {
			return false, 0
		}
		count++

		if restLen == contentSize {
			break
		}

		if restLen-contentSize <= 8 {
			return false, 0
		}
		payload = payload[contentSize:]
	}

	return true, count
}
