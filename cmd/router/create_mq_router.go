package router

import (
	"encoding/binary"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/pkg"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/pkg/tc"
	"github.com/rolandhe/smss/store"
	"net"
	"os"
	"time"
)

type createMqRouter struct {
	fstore store.Store
	lc     *tc.TimeTriggerControl
	ddlRouter
}

func (r *createMqRouter) Router(conn net.Conn, header *protocol.CommonHeader, worker MessageWorking) error {
	buf := make([]byte, 8)
	if err := nets.ReadAll(conn, buf); err != nil {
		return err
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
		return 0, pkg.NewBizError("mq exist")
	}
	return r.doBinlog(f, msg)
}
func (r *createMqRouter) AfterBinlog(msg *protocol.RawMessage, fileId, pos int64) error {
	payload := msg.Body.(*protocol.DDLPayload)
	buf := payload.Payload
	lf := binary.LittleEndian.Uint64(buf)
	err := r.fstore.CreateMq(msg.MqName, int64(lf))
	if err == nil && lf > 0 {
		r.lc.Set(int64(lf), true)
	}
	return err
}
