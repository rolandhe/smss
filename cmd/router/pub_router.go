package router

import (
	"github.com/rolandhe/smss/binlog"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/pkg/dir"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"net"
	"os"
	"time"
)

type pubRouter struct {
	fstore store.Store
	*routerSampleLogger
}

func (r *pubRouter) Router(conn net.Conn, header *protocol.CommonHeader, worker standard.MessageWorking) error {
	pubPayload, err := readPubPayload(conn, &protocol.PubProtoHeader{
		CommonHeader: header,
	})
	if err != nil {
		logger.Infof("tid=%s,readPubPayload err:%v", header.TraceId, err)
		return err
	}

	if curInsRole != store.Master {
		return nets.OutputRecoverErr(conn, "just master can pub message", NetWriteTimeout)
	}

	msg := &protocol.RawMessage{
		Command:   header.GetCmd(),
		TopicName: header.TopicName,
		TraceId:   header.TraceId,
		Timestamp: time.Now().UnixMilli(),
		Body:      pubPayload,
	}

	if err = worker.Work(msg); err != nil {
		logger.Infof("tid=%s,pub to call Work err:%v", header.TraceId, err)
		return nets.OutputRecoverErr(conn, err.Error(), NetWriteTimeout)
	}

	return nets.OutputOk(conn, NetWriteTimeout)
}

func (r *pubRouter) DoBinlog(f *os.File, msg *protocol.RawMessage) (int64, error) {
	info, err := r.fstore.GetTopicInfoReader().GetTopicInfo(msg.TopicName)
	if err != nil {
		logger.Infof("tid=%s,pubRouter.DoBinlog call topic %s info error:%v", msg.TraceId, msg.TopicName, err)
		return 0, err
	}
	if info == nil || info.IsInvalid() {
		if msg.Src == protocol.RawMessageReplica {
			msg.Skip = true
			return r.outputBinlog(f, msg)
		}
		logger.Infof("tid=%s,pubRouter.DoBinlog %s not exist", msg.TraceId, msg.TopicName)
		return 0, dir.NewBizError("topic not exist")
	}

	return r.outputBinlog(f, msg)
}

func (r *pubRouter) outputBinlog(f *os.File, msg *protocol.RawMessage) (int64, error) {
	payload := msg.Body.(*protocol.PubPayload)

	setupRawMessageEventIdAndWriteTime(msg, payload.BatchSize)
	buff := binlog.PubEncoder(msg)

	return buff.WriteTo(f)
}

func (r *pubRouter) AfterBinlog(msg *protocol.RawMessage, fileId, pos int64) (int, error) {
	if msg.Src == protocol.RawMessageReplica && msg.Skip {
		return standard.SyncFdIgnore, nil
	}
	payload := msg.Body.(*protocol.PubPayload)
	messages, _ := protocol.ParsePayload(payload.Payload, fileId, pos, msg.EventId)
	syncFd, err := r.fstore.Save(msg.TopicName, messages)

	r.sampleLog("pubRouter.AfterBinlog", msg, err)

	return syncFd, err
}

func readPubPayload(conn net.Conn, header *protocol.PubProtoHeader) (*protocol.PubPayload, error) {
	payloadSize := header.GetPayloadSize()
	if payloadSize <= 8 {
		logger.Infof("tid=%s,invalid request, payload size must be more than 8", header.TraceId)
		if e := nets.OutputRecoverErr(conn, "invalid request, payload size must be more than 8", NetWriteTimeout); e != nil {
			return nil, e
		}
		return nil, dir.NewBizError("invalid pub payload")
	}

	buf := make([]byte, payloadSize)
	var err error

	if err = nets.ReadAll(conn, buf, NetReadTimeout); err != nil {
		return nil, err
	}

	ok, count := protocol.CheckPayload(buf)
	if !ok {
		if e := nets.OutputRecoverErr(conn, "invalid pub payload", NetWriteTimeout); e != nil {
			return nil, e
		}

		return nil, dir.NewBizError("invalid pub payload")
	}

	return &protocol.PubPayload{
		Payload:   buf,
		BatchSize: count,
	}, nil
}
