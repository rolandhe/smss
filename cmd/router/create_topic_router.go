package router

import (
	"encoding/binary"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/pkg/dir"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/pkg/tc"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"net"
	"os"
	"strings"
	"time"
)

type createMqRouter struct {
	fstore store.Store
	lc     *tc.TimeTriggerControl
	ddlRouter
}

func (r *createMqRouter) Router(conn net.Conn, header *protocol.CommonHeader, worker standard.MessageWorking) error {
	buf := make([]byte, 8)
	if err := nets.ReadAll(conn, buf, NetReadTimeout); err != nil {
		return err
	}
	if len(header.TopicName) > 128 || strings.ContainsFunc(header.TopicName, func(r rune) bool {
		return r == ' ' || r == '\n' || r == '\t'
	}) {
		logger.Get().Infof("tid=%s,create %s error, topic name MUST be less than 128 char and NOT contains space/enter/tab", header.TraceId, header.TopicName)
		return nets.OutputRecoverErr(conn, "topic name MUST be less than 128 char and NOT contains space/enter/tab", NetWriteTimeout)
	}
	if curInsRole != store.Master {
		return nets.OutputRecoverErr(conn, "just master can manage topic", NetWriteTimeout)
	}

	expireAt := int64(binary.LittleEndian.Uint64(buf))

	if expireAt < 0 || (expireAt > 0 && expireAt-time.Now().UnixMilli() < 10000) {
		return nets.OutputRecoverErr(conn, "expire MUST more than 10s", NetWriteTimeout)
	}

	msg := &protocol.RawMessage{
		Command:   header.GetCmd(),
		TopicName: header.TopicName,
		Timestamp: time.Now().UnixMilli(),
		TraceId:   header.TraceId,
		Body: &protocol.DDLPayload{
			// 生命周期，unix时间戳，即在什么时候过期
			Payload: buf,
		},
	}
	return r.router(conn, msg, worker)
}

func (r *createMqRouter) DoBinlog(f *os.File, msg *protocol.RawMessage) (int64, error) {
	info, err := r.fstore.GetTopicInfoReader().GetTopicInfo(msg.TopicName)
	if err != nil {
		return 0, err
	}

	if info != nil {
		if msg.Src == protocol.RawMessageReplica {
			msg.Skip = true
			return r.doBinlog(f, msg)
		}
		return 0, dir.NewBizError("topic exist")
	}

	if msg.Src == protocol.RawMessageReplica {
		payload := msg.Body.(*protocol.DDLPayload)
		expireAt := int64(binary.LittleEndian.Uint64(payload.Payload))
		if expireAt != 0 && expireAt <= time.Now().UnixMilli() {
			msg.Skip = true
			return r.doBinlog(f, msg)
		}
	}

	setupRawMessageEventIdAndWriteTime(msg, 1)
	return r.doBinlog(f, msg)
}
func (r *createMqRouter) AfterBinlog(msg *protocol.RawMessage, fileId, pos int64) error {
	if msg.Src == protocol.RawMessageReplica && msg.Skip {
		return nil
	}
	payload := msg.Body.(*protocol.DDLPayload)
	buf := payload.Payload
	lf := binary.LittleEndian.Uint64(buf)
	err := r.fstore.CreateTopic(msg.TopicName, int64(lf), msg.EventId)
	if err == nil && msg.Src != protocol.RawMessageReplica && lf > 0 {
		r.lc.Set(int64(lf), true)
	}

	return err
}
