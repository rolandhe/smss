package binlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/rolandhe/smss/cmd/protocol"
	"strconv"
	"strings"
	"time"
)

func PubEncoder(msg *protocol.RawMessage) *bytes.Buffer {
	var buff bytes.Buffer
	buff.Write([]byte{0, 0, 0, 0})
	buff.WriteString(fmt.Sprintf("%d", time.Now().UnixMilli()))
	buff.WriteRune('\t')

	buff.WriteString(fmt.Sprintf("%d", msg.Command.Int()))
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
	buff.WriteString(fmt.Sprintf("%d", time.Now().UnixMilli()))
	buff.WriteRune('\t')

	buff.WriteString(fmt.Sprintf("%d", msg.Command.Int()))
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
	buff.WriteString(fmt.Sprintf("%d", time.Now().UnixMilli()))
	buff.WriteRune('\t')

	buff.WriteString(fmt.Sprintf("%d", msg.Command.Int()))
	buff.WriteRune('\t')

	buff.WriteString(fmt.Sprintf("%d", msg.Timestamp))
	buff.WriteRune('\t')
	buff.WriteString(msg.MqName)

	buff.WriteRune('\t')
	var payloadBuf []byte
	if msg.Body == nil {
		buff.WriteString("0")
	} else {
		payload := msg.Body.(*protocol.DDLPayload)
		payloadBuf = payload.Payload
		buff.WriteString(strconv.Itoa(len(payloadBuf)))
	}
	buff.WriteRune('\n')

	binary.LittleEndian.PutUint32(buff.Bytes(), uint32(buff.Len()-4))

	if len(payloadBuf) > 0 {
		buff.Write(payloadBuf)
	}
	return &buff
}

func DelayEncode(msg *protocol.RawMessage) *bytes.Buffer {
	var buff bytes.Buffer
	buff.Write([]byte{0, 0, 0, 0})
	buff.WriteString(fmt.Sprintf("%d", time.Now().UnixMilli()))
	buff.WriteRune('\t')

	buff.WriteString(fmt.Sprintf("%d", msg.Command.Int()))
	buff.WriteRune('\t')

	buff.WriteString(fmt.Sprintf("%d", msg.Timestamp))
	buff.WriteRune('\t')
	buff.WriteString(msg.MqName)
	buff.WriteRune('\t')

	storeMsg := msg.Body.(*protocol.DelayPayload)

	buff.WriteString(fmt.Sprintf("%d\n", len(storeMsg.Payload)))

	binary.LittleEndian.PutUint32(buff.Bytes(), uint32(buff.Len()-4))

	buff.Write(storeMsg.Payload)
	return &buff
}

func CmdDecoder(buf []byte) *protocol.DecodedRawMessage {
	cmdLine := string(buf[:len(buf)-1])
	items := strings.Split(cmdLine, "\t")
	var msg protocol.DecodedRawMessage

	v, _ := strconv.Atoi(items[1])
	msg.Command = protocol.CommandEnum(v)
	msg.MqName = items[3]
	msg.PayloadLen, _ = strconv.Atoi(items[4])

	return &msg
}
