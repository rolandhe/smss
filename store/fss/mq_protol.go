package fss

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/rolandhe/smss/cmd/protocol"
	"strconv"
	"strings"
	"time"
)

var (
	lenHolder = []byte{0, 0, 0, 0}
)

type MqMessageCommand struct {
	ts           int64
	id           int64
	sendTime     int64
	payLoadSize  int
	IndexOfBatch int

	SrcFileId int64
	SrcPos    int64
	CmdLen    int
}

func (mc *MqMessageCommand) GetPayloadSize() int {
	return mc.payLoadSize
}
func (mc *MqMessageCommand) GetCmd() protocol.CommandEnum {
	return protocol.CommandPub
}
func (mc *MqMessageCommand) GetId() int64 {
	return mc.id
}

func buildCommandsAndCalcSize(amsg *asyncMsg) ([][]byte, int) {
	var builder bytes.Buffer

	size := 0

	ret := make([][]byte, len(amsg.messages))

	for i, msg := range amsg.messages {
		builder.Write(lenHolder)
		builder.WriteString(strconv.FormatInt(time.Now().UnixMilli(), 10))
		builder.WriteRune('\t')

		builder.WriteString(strconv.FormatInt(msg.SeqId, 10))
		builder.WriteRune('\t')

		builder.WriteString(strconv.FormatInt(amsg.saveTime, 10))
		builder.WriteRune('\t')

		payloadSize := len(msg.Content) + 1
		builder.WriteString(strconv.Itoa(payloadSize))
		builder.WriteRune('\t')

		builder.WriteString(strconv.Itoa(i))
		builder.WriteRune('\t')

		builder.WriteString(fmt.Sprintf("%d", msg.SrcFileId))
		builder.WriteRune('\t')

		builder.WriteString(fmt.Sprintf("%d", msg.SrcPos))
		builder.WriteRune('\n')

		binary.LittleEndian.PutUint32(builder.Bytes(), uint32(builder.Len())-4)

		ret[i] = append(ret[i], builder.Bytes()...)

		builder.Reset()
		size += len(ret[i])
		size += payloadSize
	}

	return ret, size
}

func ReadMqMessageCmd(buf []byte, msg *MqMessageCommand) error {
	var err error
	cmd := string(buf)
	items := strings.Split(cmd, "\t")
	if len(items) < 7 {
		return errors.New("invalid cmd line")
	}
	msg.CmdLen = len(buf) + 5

	if msg.ts, err = strconv.ParseInt(items[0], 10, 64); err != nil {
		return err
	}
	if msg.id, err = strconv.ParseInt(items[1], 10, 64); err != nil {
		return err
	}
	if msg.sendTime, err = strconv.ParseInt(items[2], 10, 64); err != nil {
		return err
	}
	if msg.payLoadSize, err = strconv.Atoi(items[3]); err != nil {
		return err
	}
	if msg.IndexOfBatch, err = strconv.Atoi(items[4]); err != nil {
		return err
	}

	if msg.SrcFileId, err = strconv.ParseInt(items[5], 10, 64); err != nil {
		return err
	}
	if msg.SrcPos, err = strconv.ParseInt(items[6], 10, 64); err != nil {
		return err
	}
	return nil
}
