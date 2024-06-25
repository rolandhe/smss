package protocol

import (
	"encoding/binary"
	"github.com/rolandhe/smss/pkg"
	"github.com/rolandhe/smss/store"
)

func ParsePayload(payload []byte, fileId, pos int64) ([]*store.MQMessage, error) {
	var ret []*store.MQMessage
	if len(payload) <= 8 {
		return nil, pkg.NewBizError("invalid message format")
	}
	for {
		contentSize := int(binary.LittleEndian.Uint32(payload))
		if contentSize <= 0 {
			return nil, pkg.NewBizError("invalid message format")
		}
		oneMsgLen := contentSize + 8
		restLen := len(payload)
		if restLen < oneMsgLen {
			return nil, pkg.NewBizError("invalid message format")
		}
		content := make([]byte, oneMsgLen)
		copy(content, payload[:oneMsgLen])

		ret = append(ret, &store.MQMessage{
			SrcFileId: fileId,
			SrcPos:    pos,
			Content:   content,
		})

		if restLen == oneMsgLen {
			break
		}

		if restLen-oneMsgLen <= 8 {
			return nil, pkg.NewBizError("invalid message format")
		}
		payload = payload[oneMsgLen:]
	}

	return ret, nil
}

func CheckPayload(payload []byte) bool {
	if len(payload) <= 8 {
		return false
	}
	for {
		contentSize := int(binary.LittleEndian.Uint32(payload))
		if contentSize <= 0 {
			return false
		}
		payload = payload[8:]
		restLen := len(payload)
		if restLen < contentSize {
			return false
		}

		if restLen == contentSize {
			break
		}

		if restLen-contentSize <= 8 {
			return false
		}
		payload = payload[contentSize:]
	}

	return true
}
