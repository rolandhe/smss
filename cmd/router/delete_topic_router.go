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

type deleteTopicRouter struct {
	fstore store.Store
	ddlRouter
	delExecutor protocol.DelTopicFileExecutor
}

func (r *deleteTopicRouter) Router(conn net.Conn, header *protocol.CommonHeader, worker standard.MessageWorking) error {
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

func (r *deleteTopicRouter) DoBinlog(f *os.File, msg *protocol.RawMessage) (int64, error) {
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
func (r *deleteTopicRouter) AfterBinlog(msg *protocol.RawMessage, fileId, pos int64) (int, error) {
	if msg.Src == protocol.RawMessageReplica && msg.Skip {
		return standard.SyncFdIgnore, nil
	}
	err := deleteTopicRoot(msg.TopicName, "deleteTopicRouter", r.fstore, r.delExecutor, msg.TraceId)
	logger.Get().Infof("tid=%s,deleteTopicRouter.AfterBinlog, topic=%s,eventId=%d, err:%v", msg.TraceId, msg.TopicName, msg.EventId, err)
	return standard.SyncFdIgnore, err
}

func deleteTopicRoot(topicName, who string, fstore store.Store, delExecutor protocol.DelTopicFileExecutor, traceId string) error {
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
