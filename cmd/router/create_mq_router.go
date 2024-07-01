package router

import (
	"encoding/binary"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/pkg"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/pkg/tc"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"log"
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
	if len(header.MQName) > 128 || strings.ContainsFunc(header.MQName, func(r rune) bool {
		return r == ' ' || r == '\n' || r == '\t'
	}) {
		log.Printf("tid=%s,create %s error, mq name MUST be less than 128 char and NOT contains space/enter/tab\n", header.TraceId, header.MQName)
		return nets.OutputRecoverErr(conn, "mq name MUST be less than 128 char and NOT contains space/enter/tab", NetWriteTimeout)
	}
	if curInsRole != store.Master {
		return nets.OutputRecoverErr(conn, "just master can manage mq", NetWriteTimeout)
	}
	msg := &protocol.RawMessage{
		Command:   header.GetCmd(),
		MqName:    header.MQName,
		Timestamp: time.Now().UnixMilli(),
		TraceId:   header.TraceId,
		Body: &protocol.DDLPayload{
			Payload: buf,
		},
	}
	return r.router(conn, msg, worker)
}

func (r *createMqRouter) DoBinlog(f *os.File, msg *protocol.RawMessage) (int64, error) {
	info, err := r.fstore.GetMqInfoReader().GetMQInfo(msg.MqName)
	if err != nil {
		return 0, err
	}

	if info != nil {
		if msg.Src == protocol.RawMessageReplica {
			msg.Skip = true
			return r.doBinlog(f, msg)
		}
		return 0, pkg.NewBizError("mq exist")
	}
	setupRawMessageSeqIdAndWriteTime(msg, 1)
	return r.doBinlog(f, msg)
}
func (r *createMqRouter) AfterBinlog(msg *protocol.RawMessage, fileId, pos int64) error {
	if msg.Src == protocol.RawMessageReplica && msg.Skip {
		return nil
	}

	payload := msg.Body.(*protocol.DDLPayload)
	buf := payload.Payload
	lf := binary.LittleEndian.Uint64(buf)
	err := r.fstore.CreateMq(msg.MqName, int64(lf), msg.MessageSeqId)
	if err == nil && lf > 0 {
		r.lc.Set(int64(lf), true)
	}
	return err
}
