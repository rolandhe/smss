package binlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/rolandhe/smss/cmd/protocol"
	"strconv"
	"strings"
)

func PubEncoder(msg *protocol.RawMessage) *bytes.Buffer {
	var buff bytes.Buffer
	buff.Write([]byte{0, 0, 0, 0})
	// worker单线程开始处理消息时的时间戳， 与 msg.Timestamp 相比，可以看到单线程积压的时间差
	buff.WriteString(fmt.Sprintf("%d", msg.WriteTime))
	buff.WriteRune('\t')

	buff.WriteString(fmt.Sprintf("%d", msg.Command.Int()))
	buff.WriteRune('\t')

	buff.WriteString(fmt.Sprintf("%d", msg.MessageSeqId))
	buff.WriteRune('\t')

	buff.WriteString(fmt.Sprintf("%d", msg.Timestamp))
	buff.WriteRune('\t')

	buff.WriteString(msg.MqName)
	buff.WriteRune('\t')

	storeMsg := msg.Body.(*protocol.PubPayload)

	buff.WriteString(fmt.Sprintf("%d\n", len(storeMsg.Payload)+1))

	binary.LittleEndian.PutUint32(buff.Bytes(), uint32(buff.Len()-4))

	buff.Write(storeMsg.Payload)
	buff.WriteRune('\n')
	return &buff
}

func DelayApplyEncoder(msg *protocol.RawMessage) *bytes.Buffer {
	var buff bytes.Buffer
	buff.Write([]byte{0, 0, 0, 0})
	buff.WriteString(fmt.Sprintf("%d", msg.WriteTime))
	buff.WriteRune('\t')

	buff.WriteString(fmt.Sprintf("%d", msg.Command.Int()))
	buff.WriteRune('\t')

	buff.WriteString(fmt.Sprintf("%d", msg.MessageSeqId))
	buff.WriteRune('\t')

	buff.WriteString(fmt.Sprintf("%d", msg.Timestamp))
	buff.WriteRune('\t')
	buff.WriteString(msg.MqName)
	buff.WriteRune('\t')

	storeMsg := msg.Body.(*protocol.DelayApplyPayload)

	buff.WriteString(fmt.Sprintf("%d\n", len(storeMsg.Payload)+1))

	binary.LittleEndian.PutUint32(buff.Bytes(), uint32(buff.Len()-4))

	buff.Write(storeMsg.Payload)
	buff.WriteRune('\n')
	return &buff
}

func DDLEncoder(msg *protocol.RawMessage) *bytes.Buffer {
	var buff bytes.Buffer
	buff.Write([]byte{0, 0, 0, 0})
	buff.WriteString(fmt.Sprintf("%d", msg.WriteTime))
	buff.WriteRune('\t')

	buff.WriteString(fmt.Sprintf("%d", msg.Command.Int()))
	buff.WriteRune('\t')

	buff.WriteString(fmt.Sprintf("%d", msg.MessageSeqId))
	buff.WriteRune('\t')

	buff.WriteString(fmt.Sprintf("%d", msg.Timestamp))
	buff.WriteRune('\t')
	buff.WriteString(msg.MqName)

	buff.WriteRune('\t')
	var payloadBuf []byte
	payloadLen := 0
	if msg.Body == nil {
		buff.WriteString("0")
	} else {
		payload := msg.Body.(*protocol.DDLPayload)
		payloadBuf = payload.Payload
		payloadLen = len(payloadBuf)
		if payloadLen > 0 {
			payloadLen++
		}
		buff.WriteString(strconv.Itoa(payloadLen))
	}
	buff.WriteRune('\n')

	binary.LittleEndian.PutUint32(buff.Bytes(), uint32(buff.Len()-4))

	if payloadLen > 0 {
		buff.Write(payloadBuf)
		buff.WriteRune('\n')
	}
	return &buff
}

func DelayEncode(msg *protocol.RawMessage) *bytes.Buffer {
	var buff bytes.Buffer
	buff.Write([]byte{0, 0, 0, 0})
	buff.WriteString(fmt.Sprintf("%d", msg.WriteTime))
	buff.WriteRune('\t')

	buff.WriteString(fmt.Sprintf("%d", msg.Command.Int()))
	buff.WriteRune('\t')

	buff.WriteString(fmt.Sprintf("%d", msg.MessageSeqId))
	buff.WriteRune('\t')

	buff.WriteString(fmt.Sprintf("%d", msg.Timestamp))
	buff.WriteRune('\t')
	buff.WriteString(msg.MqName)
	buff.WriteRune('\t')

	storeMsg := msg.Body.(*protocol.DelayPayload)

	buff.WriteString(fmt.Sprintf("%d\n", len(storeMsg.Payload)+1))

	binary.LittleEndian.PutUint32(buff.Bytes(), uint32(buff.Len()-4))

	buff.Write(storeMsg.Payload)
	buff.WriteRune('\n')
	return &buff
}

func CmdDecoder(buf []byte) *protocol.DecodedRawMessage {
	cmdLine := string(buf[:len(buf)-1])
	items := strings.Split(cmdLine, "\t")
	var msg protocol.DecodedRawMessage

	msg.WriteTime, _ = strconv.ParseInt(items[0], 10, 64)

	v, _ := strconv.Atoi(items[1])
	msg.Command = protocol.CommandEnum(v)

	id, _ := strconv.Atoi(items[2])
	msg.MessageSeqId = int64(id)

	msg.Timestamp, _ = strconv.ParseInt(items[3], 10, 64)

	msg.MqName = items[4]
	msg.PayloadLen, _ = strconv.Atoi(items[5])

	return &msg
}
