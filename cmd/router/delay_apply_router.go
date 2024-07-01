package router

import (
	"errors"
	"github.com/rolandhe/smss/binlog"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/pkg"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"log"
	"net"
	"os"
)

type delayApplyRouter struct {
	fstore store.Store
}

func (r *delayApplyRouter) Router(conn net.Conn, header *protocol.CommonHeader, worker standard.MessageWorking) error {
	return errors.New("don't support this action")
}

func (r *delayApplyRouter) DoBinlog(f *os.File, msg *protocol.RawMessage) (int64, error) {
	info, err := r.fstore.GetMqInfoReader().GetMQInfo(msg.MqName)
	if err != nil {
		log.Printf("tid=%s,delayApplyRouter.DoBinlog call mq %s info error:%v\n", msg.TraceId, msg.MqName, err)
		return 0, err
	}
	if info == nil || info.IsInvalid() {
		if msg.Src == protocol.RawMessageReplica {
			return 0, nil
		}
		log.Printf("tid=%s,delayApplyRouter.DoBinlog  %s not exist\n", msg.TraceId, msg.MqName)
		return 0, pkg.NewBizError("mq not exist")
	}
	payload := msg.Body.(*protocol.DelayApplyPayload)
	_, count := protocol.CheckPayload(payload.Payload[16:])

	setupRawMessageSeqIdAndWriteTime(msg, count)

	buff := binlog.DelayApplyEncoder(msg)

	var n int64
	n, err = buff.WriteTo(f)
	log.Printf("tid=%s,delayApplyRouter.DoBinlog  %s finish:%v\n", msg.TraceId, msg.MqName, err)
	return n, err
}

func (r *delayApplyRouter) AfterBinlog(msg *protocol.RawMessage, fileId, pos int64) error {
	payload := msg.Body.(*protocol.DelayApplyPayload)
	// 去除前面的 delayTime+delayId
	messages, _ := protocol.ParsePayload(payload.Payload[16:], fileId, pos, msg.MessageSeqId)
	err := r.fstore.Save(msg.MqName, messages)
	log.Printf("tid=%s,delayApplyRouter.AfterBinlog  %s finish:%v\n", msg.TraceId, msg.MqName, err)
	return err
}
