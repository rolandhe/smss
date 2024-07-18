package router

import (
	"errors"
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

type deleteMqRouter struct {
	fstore store.Store
	ddlRouter
	delExecutor protocol.DelMqFileExecutor
}

func (r *deleteMqRouter) Router(conn net.Conn, header *protocol.CommonHeader, worker standard.MessageWorking) error {
	if curInsRole != store.Master {
		return nets.OutputRecoverErr(conn, "just master can manage topic", NetWriteTimeout)
	}
	msg := &protocol.RawMessage{
		Command:   header.GetCmd(),
		TopicName: header.TopicName,
		TraceId:   header.TraceId,
		Timestamp: time.Now().UnixMilli(),
	}
	return r.router(conn, msg, worker)
}

func (r *deleteMqRouter) DoBinlog(f *os.File, msg *protocol.RawMessage) (int64, error) {
	info, err := r.fstore.GetTopicInfoReader().GetTopicInfo(msg.TopicName)
	if err != nil {
		return 0, err
	}
	if info == nil {
		if msg.Src == protocol.RawMessageReplica {
			msg.Skip = true
			return r.doBinlog(f, msg)
		}
		return 0, dir.NewBizError("topic not exist")
	}
	setupRawMessageEventIdAndWriteTime(msg, 1)
	return r.doBinlog(f, msg)
}
func (r *deleteMqRouter) AfterBinlog(msg *protocol.RawMessage, fileId, pos int64) error {
	if msg.Src == protocol.RawMessageReplica && msg.Skip {
		return nil
	}
	err := deleteTopicRoot(msg.TopicName, "deleteMqRouter", r.fstore, r.delExecutor, msg.TraceId)
	logger.Get().Infof("tid=%s,deleteMqRouter.AfterBinlog, topic=%s,eventId=%d, err:%v", msg.TraceId, msg.TopicName, msg.EventId, err)
	return err
}

func deleteTopicRoot(topicName, who string, fstore store.Store, delExecutor protocol.DelMqFileExecutor, traceId string) error {
	return fstore.ForceDeleteTopic(topicName, func() error {
		waiter := delExecutor.Submit(topicName, who, traceId)
		if !waiter(time.Second * 2) {
			logger.Get().Infof("tid=%s,delete %s topic file failed", traceId, topicName)
			return errors.New("delete topic file failed")
		}

		logger.Get().Infof("tid=%s,deleteTopicRoot %s ok", traceId, topicName)

		return nil
	})
}
