package fss

import (
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
)

type blockReader struct {
	topic string
	*standard.StdMsgBlockReader[store.ReadMessage]
}

type msgParser struct {
	msg MqMessageCommand
	//cmdBuf []byte
}

func (p *msgParser) ToMessage(payload []byte, fileId, pos int64) *store.ReadMessage {
	payloadLen := len(payload)
	// 去除最后的\n
	content := payload[:payloadLen-1]
	v := &store.ReadMessage{
		Ts:      p.msg.ts,
		EventId: p.msg.id,
		PayLoad: content,
	}
	v.NextPos.FileId = fileId
	v.NextPos.Pos = pos
	return v
}
func (p *msgParser) Reset() {
	p.msg.ts = 0
	p.msg.id = 0
	p.msg.sendTime = 0
	p.msg.payLoadSize = 0
}

func (p *msgParser) ParseCmd(cmdBuf []byte) (standard.CmdLine, error) {
	err := ReadMqMessageCmd(cmdBuf[:len(cmdBuf)-1], &p.msg)
	if err != nil {
		return nil, err
	}
	return &p.msg, nil
}

func (p *msgParser) ChangeMessagePos(ins *store.ReadMessage, fileId, pos int64) {
	ins.NextPos.FileId = fileId
	ins.NextPos.Pos = pos
}

type TopicNotifyRegister struct {
	fs        *fileStore
	topicName string
	whoami    string
}

func (reg *TopicNotifyRegister) RegisterReaderNotify(notify *standard.NotifyDevice) (standard.LogFileInfoGet, error) {
	return reg.fs.registerReaderNotify(reg.topicName, reg.whoami, notify)
}
func (reg *TopicNotifyRegister) UnRegisterReaderNotify() {
	reg.fs.unRegisterReaderNotify(reg.topicName, reg.whoami)
}

func newBlockReader(root string, whoami string, topic string, maxBatch int, register standard.NotifyRegister) store.TopicBlockReader {
	r := standard.NewStdMsgBlockReader[store.ReadMessage](topic, root, whoami, maxBatch, conf.MaxLogSize, register, &msgParser{})
	return &blockReader{
		topic:             topic,
		StdMsgBlockReader: r,
	}
}
