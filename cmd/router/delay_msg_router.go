package router

import (
	"encoding/binary"
	"github.com/rolandhe/smss/binlog"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/pkg"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/pkg/tc"
	"github.com/rolandhe/smss/store"
	"net"
	"os"
	"time"
)

// delayRouter  原始的 payload = delayTime + pub message
// 写binlog是需要提前生成delay id，变成 payload = delayId + payload, 从库复制时可以直接复用该delayId,
// 并把从库的delay Id seq更新成当前的 delayId
// 延迟消息被存储到 db中，key 包含 delayId，需要从payload中读取

type delayRouter struct {
	fstore   store.Store
	delayCtl *tc.TimeTriggerControl
}

func (r *delayRouter) Router(conn net.Conn, header *protocol.CommonHeader, worker MessageWorking) error {
	pubHeader := &protocol.PubProtoHeader{
		CommonHeader: header,
	}
	// delayTime + pub message
	// 最终存储在binlog的格式
	// delayId + delayTime + pub message
	buf := make([]byte, 8+pubHeader.GetPayloadSize())
	if err := nets.ReadAll(conn, buf); err != nil {
		return err
	}
	if pubHeader.GetPayloadSize() <= 8 {
		return nets.OutputRecoverErr(conn, "invalid delay request")
	}
	delayTime := int64(binary.LittleEndian.Uint64(buf))
	if delayTime < 1000 {
		return nets.OutputRecoverErr(conn, "delay time must be more than 1 second")
	}
	if !protocol.CheckPayload(buf[8:]) {
		return nets.OutputRecoverErr(conn, "invalid delay request")
	}

	triggerTime := time.Now().Add(time.Millisecond * time.Duration(delayTime)).UnixMilli()
	msg := &protocol.RawMessage{
		Command:   header.GetCmd(),
		MqName:    header.MQName,
		Timestamp: time.Now().UnixMilli(),
		TraceId:   header.TraceId,
		Body: &protocol.DelayPayload{
			// 原始的带delayTime的数据
			Payload:   buf,
			DelayTime: triggerTime,
		},
	}
	err := worker.Work(msg)
	if err != nil {
		return nets.OutputRecoverErr(conn, err.Error())
	}
	return nets.OutputOk(conn)
}

func (r *delayRouter) DoBinlog(f *os.File, msg *protocol.RawMessage) (int64, error) {
	info, err := r.fstore.GetMqInfoReader().GetMQInfo(msg.MqName)
	if err != nil {
		return 0, err
	}
	if info == nil || info.IsInvalid() {
		return 0, pkg.NewBizError("mq not exist")
	}

	delayId, err := r.fstore.GetScanner().GetDelayId()
	if err != nil {
		return 0, err
	}
	storeMsg := msg.Body.(*protocol.DelayPayload)
	payload := storeMsg.Payload
	newPayload := make([]byte, 8+len(payload))
	binary.LittleEndian.PutUint64(newPayload, delayId)
	copy(newPayload[8:], payload)
	storeMsg.Payload = newPayload
	buff := binlog.DelayEncode(msg)

	return buff.WriteTo(f)
}

func (r *delayRouter) AfterBinlog(msg *protocol.RawMessage, fileId, pos int64) error {
	storeMsg := msg.Body.(*protocol.DelayPayload)

	err := r.fstore.SaveDelayMsg(msg.MqName, storeMsg.DelayTime, storeMsg.Payload)
	if err != nil {
		return err
	}
	r.delayCtl.Set(storeMsg.DelayTime, true)
	return nil
}
