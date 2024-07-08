package router

import (
	"encoding/binary"
	"github.com/rolandhe/smss/binlog"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/pkg/dir"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/pkg/tc"
	"github.com/rolandhe/smss/standard"
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

func (r *delayRouter) Router(conn net.Conn, header *protocol.CommonHeader, worker standard.MessageWorking) error {
	pubHeader := &protocol.PubProtoHeader{
		CommonHeader: header,
	}
	// delayTime + pub message
	// 最终存储在binlog的格式
	// delayTime + delayId  + pub message
	buf := make([]byte, 8+pubHeader.GetPayloadSize())
	if err := nets.ReadAll(conn, buf, NetReadTimeout); err != nil {
		return err
	}

	if curInsRole != store.Master {
		return nets.OutputRecoverErr(conn, "just master can pub message", NetWriteTimeout)
	}

	if pubHeader.GetPayloadSize() <= 8 {
		return nets.OutputRecoverErr(conn, "invalid delay request", NetWriteTimeout)
	}
	delayTime := int64(binary.LittleEndian.Uint64(buf))
	if delayTime < 1000 {
		return nets.OutputRecoverErr(conn, "delay time must be more than 1 second", NetWriteTimeout)
	}
	ok, _ := protocol.CheckPayload(buf[8:])
	if !ok {
		return nets.OutputRecoverErr(conn, "invalid delay request", NetWriteTimeout)
	}

	triggerTime := time.Now().Add(time.Millisecond * time.Duration(delayTime)).UnixMilli()
	// 把时间间隔给出具体的执行时间
	binary.LittleEndian.PutUint64(buf, uint64(triggerTime))
	msg := &protocol.RawMessage{
		Command:   header.GetCmd(),
		MqName:    header.MQName,
		Timestamp: time.Now().UnixMilli(),
		TraceId:   header.TraceId,
		Body: &protocol.DelayPayload{
			// 原始的带delayTime的数据, 是毫秒，不是一个具体触发的时间戳
			Payload: buf,
		},
	}
	err := worker.Work(msg)
	if err != nil {
		return nets.OutputRecoverErr(conn, err.Error(), NetWriteTimeout)
	}
	return nets.OutputOk(conn, NetWriteTimeout)
}

func (r *delayRouter) DoBinlog(f *os.File, msg *protocol.RawMessage) (int64, error) {
	info, err := r.fstore.GetMqInfoReader().GetMQInfo(msg.MqName)
	if err != nil {
		return 0, err
	}
	if info == nil || info.IsInvalid() {
		if msg.Src == protocol.RawMessageReplica {
			msg.Skip = true
			return r.outBinlog(f, msg)
		}
		return 0, dir.NewBizError("mq not exist")
	}

	return r.outBinlog(f, msg)
}

func (r *delayRouter) outBinlog(f *os.File, msg *protocol.RawMessage) (int64, error) {
	setupRawMessageEventIdAndWriteTime(msg, 1)
	if msg.Src != protocol.RawMessageReplica {
		storeMsg := msg.Body.(*protocol.DelayPayload)
		payload := storeMsg.Payload
		newPayload := make([]byte, 8+len(payload))
		// copy 时间戳
		copy(newPayload[:8], payload)
		// delayId, 直接来自binlog的seq id
		binary.LittleEndian.PutUint64(newPayload[8:], uint64(msg.EventId))
		copy(newPayload[16:], payload[8:])
		storeMsg.Payload = newPayload
	}

	buff := binlog.DelayEncode(msg)

	return buff.WriteTo(f)
}

func (r *delayRouter) AfterBinlog(msg *protocol.RawMessage, fileId, pos int64) error {
	if msg.Src == protocol.RawMessageReplica && msg.Skip {
		return nil
	}
	storeMsg := msg.Body.(*protocol.DelayPayload)

	err := r.fstore.SaveDelayMsg(msg.MqName, storeMsg.Payload)
	if err != nil {
		return err
	}
	if msg.Src != protocol.RawMessageReplica {
		triggerTime := binary.LittleEndian.Uint64(storeMsg.Payload)
		r.delayCtl.Set(int64(triggerTime), true)
	}
	return nil
}
