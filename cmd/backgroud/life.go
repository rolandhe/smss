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
	lc := tc.NewTimeTriggerControl(fstore, "life", conf.LifeDefaultScanSec*1000, func(fstore store.Store) int64 {
		traceId := fmt.Sprintf("life-%d", time.Now().UnixMilli())
		return doLife(fstore, worker, traceId)
	})
	go lc.Process()

	return lc
}

func doLife(fstore store.Store, worker standard.MessageWorking, traceId string) int64 {
	topicList, next, err := fstore.GetScanner().ScanExpireMqs()
	if err != nil {
		logger.Get().Infof("tid=%s,doLife err:%v", traceId, err)
		return 0
	}

	for _, topicName := range topicList {
		if err = removeTopic(worker, topicName, traceId); err != nil {
			logger.Get().Infof("tid=%s,doLife removeTopic %s err:%v", traceId, topicName, err)
			return 0
		}
	}

	now := time.Now().UnixMilli()
	if next <= 0 {
		next = now + conf.LifeDefaultScanSec*1000
	}
	logger.Get().Infof("tid=%s,doLife ok", traceId)
	return next
}

func removeTopic(worker standard.MessageWorking, topicName string, traceId string) error {
	msg := &protocol.RawMessage{
		Command:   protocol.CommandDeleteTopic,
		TopicName: topicName,
		TraceId:   traceId,
		Timestamp: time.Now().UnixMilli(),
	}
	return worker.Work(msg)
}
