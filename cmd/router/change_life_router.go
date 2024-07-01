package router

import (
	"encoding/binary"
	"errors"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/pkg/tc"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"net"
	"os"
	"time"
)

type changeLifeRouter struct {
	fstore store.Store
	lc     *tc.TimeTriggerControl
	ddlRouter
}

func (r *changeLifeRouter) Router(conn net.Conn, header *protocol.CommonHeader, worker standard.MessageWorking) error {
	buf := make([]byte, 8)
	if err := nets.ReadAll(conn, buf); err != nil {
		return err
	}
	if curInsRole != store.Master {
		return nets.OutputRecoverErr(conn, "just master can manage mq")
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

func (r *changeLifeRouter) DoBinlog(f *os.File, msg *protocol.RawMessage) (int64, error) {
	info, err := r.fstore.GetMqInfoReader().GetMQInfo(msg.MqName)
	if err != nil {
		return 0, err
	}
	if info == nil || info.IsInvalid() {
		if msg.Src == protocol.RawMessageReplica {
			return 0, nil
		}
		return 0, errors.New("mq not exist")
	}
	if msg.Body == nil {
		return 0, errors.New("need change lifetime")
	}
	cmdPayload, ok := msg.Body.(*protocol.DDLPayload)
	if !ok || len(cmdPayload.Payload) < 8 {
		return 0, errors.New("need change lifetime")
	}
	setupRawMessageSeqIdAndWriteTime(msg, 1)
	return r.doBinlog(f, msg)
}
func (r *changeLifeRouter) AfterBinlog(msg *protocol.RawMessage, fileId, pos int64) error {
	cmdPayload := msg.Body.(*protocol.DDLPayload)
	lf := binary.LittleEndian.Uint64(cmdPayload.Payload)
	err := r.fstore.ChangeMqLife(msg.MqName, int64(lf), msg.MessageSeqId)
	if err == nil && lf > 0 {
		r.lc.Set(int64(lf), true)
	}
	return err
}
