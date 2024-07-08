package router

import (
	"errors"
	"github.com/rolandhe/smss/binlog"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/pkg/dir"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"net"
	"os"
)

type delayApplyRouter struct {
	fstore store.Store
	*routerSampleLogger
}

func (r *delayApplyRouter) Router(conn net.Conn, header *protocol.CommonHeader, worker standard.MessageWorking) error {
	return errors.New("don't support this action")
}

func (r *delayApplyRouter) DoBinlog(f *os.File, msg *protocol.RawMessage) (int64, error) {
	info, err := r.fstore.GetMqInfoReader().GetMQInfo(msg.MqName)
	if err != nil {
		logger.Get().Infof("tid=%s,delayApplyRouter.DoBinlog call mq %s info error:%v", msg.TraceId, msg.MqName, err)
		return 0, err
	}
	if info == nil || info.IsInvalid() {
		if msg.Src == protocol.RawMessageReplica {
			msg.Skip = true
			return r.outBinlog(f, msg)
		}
		logger.Get().Infof("tid=%s,delayApplyRouter.DoBinlog  %s not exist", msg.TraceId, msg.MqName)
		return 0, dir.NewBizError("mq not exist")
	}

	return r.outBinlog(f, msg)
}

func (r *delayApplyRouter) outBinlog(f *os.File, msg *protocol.RawMessage) (int64, error) {
	payload := msg.Body.(*protocol.DelayApplyPayload)
	_, count := protocol.CheckPayload(payload.Payload[16:])

	setupRawMessageEventIdAndWriteTime(msg, count)

	buff := binlog.DelayApplyEncoder(msg)

	return buff.WriteTo(f)
}

func (r *delayApplyRouter) AfterBinlog(msg *protocol.RawMessage, fileId, pos int64) error {
	if msg.Src == protocol.RawMessageReplica && msg.Skip {
		return nil
	}
	payload := msg.Body.(*protocol.DelayApplyPayload)
	// 去除前面的 delayTime+delayId
	messages, _ := protocol.ParsePayload(payload.Payload[16:], fileId, pos, msg.EventId)
	err := r.fstore.Save(msg.MqName, messages)
	r.sampleLog("delayApplyRouter.AfterBinlog", msg, err)
	return err
}
