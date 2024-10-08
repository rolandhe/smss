package backgroud

import (
	"fmt"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/pkg/tc"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"time"
)

func StartLife(fstore store.Store, worker standard.MessageWorking) *tc.TimeTriggerControl {
	lc := tc.NewTimeTriggerControl(fstore, "life", conf.FistExecDelaySecond*1000, func(fstore store.Store) int64 {
		traceId := fmt.Sprintf("life-%d", time.Now().UnixMilli())
		return doLife(fstore, worker, traceId)
	})
	go lc.Process()

	return lc
}

func doLife(fstore store.Store, worker standard.MessageWorking, traceId string) int64 {
	topicList, next, err := fstore.GetScanner().ScanExpireTopics()
	if err != nil {
		logger.Infof("tid=%s,doLife err:%v", traceId, err)
		return 0
	}
	logger.Infof("tid=%s,ScanExpireTopics %d,next scan time from db:%d", traceId, len(topicList), next)

	for _, topicName := range topicList {
		if err = deleteTopic(worker, topicName, traceId); err != nil {
			logger.Infof("tid=%s,doLife deleteTopic %s err:%v", traceId, topicName, err)
			return 0
		}
		logger.Infof("tid=%s,doLife deleteTopic %s ok", traceId, topicName)
	}

	now := time.Now().UnixMilli()
	isDefault := false
	if next <= 0 {
		isDefault = true
		next = now + conf.DefaultScanSecond*1000
	}
	logger.Infof("tid=%s,doLife ok, isDefault=%v", traceId, isDefault)
	return next
}

func deleteTopic(worker standard.MessageWorking, topicName string, traceId string) error {
	msg := &protocol.RawMessage{
		Command:   protocol.CommandDeleteTopic,
		TopicName: topicName,
		TraceId:   traceId,
		Timestamp: time.Now().UnixMilli(),
	}
	return worker.Work(msg)
}
