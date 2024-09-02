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

const (
	DelayBatchSize = 20
	//DefaultDelayTimeout = 180 * 60 * 1000
)

func StartDelay(fstore store.Store, worker standard.MessageWorking) *tc.TimeTriggerControl {
	f := func(fstore store.Store) int64 {
		return doDelay(fstore, worker)
	}

	lc := tc.NewTimeTriggerControl(fstore, "delay", conf.FistExecDelaySecond*1000, f)

	go lc.Process()

	return lc
}

func doDelay(fstore store.Store, worker standard.MessageWorking) int64 {
	tid := fmt.Sprintf("delay-%d", time.Now().UnixMilli())
	var ret int64
	for {
		delays, next, err := fstore.GetScanner().ScanDelays(DelayBatchSize)
		if err != nil {
			logger.Infof("tid=%s,doDelay err:%v", tid, err)
			return 0
		}

		logger.Infof("tid=%s,doDelay to process %d msg", tid, len(delays))
		for _, item := range delays {
			err = procOneDelayMsg(fstore, worker, item, tid)
			if err != nil {
				logger.Infof("tid=%s,procOneDelayMsg err:%v", tid, err)
				return 0
			}
		}

		if next > 0 {
			ret = next
			break
		}
		if len(delays) < DelayBatchSize {
			break
		}
	}
	isDefault := false
	if ret == 0 {
		isDefault = true
		ret = time.Now().UnixMilli() + conf.DefaultScanSecond*1000
	}

	logger.Infof("tid=%s,doDelay ok, isDefualt %v", tid, isDefault)
	return ret
}

func procOneDelayMsg(fstore store.Store, worker standard.MessageWorking, item *store.DelayItem, tid string) error {
	info, err := fstore.GetTopicInfoReader().GetTopicInfo(item.TopicName)
	if err != nil {
		logger.Infof("tid=%s,procOneDelayMsg get topic info %s  error:%v", tid, item.TopicName, err)
		return err
	}
	if info == nil || info.IsInvalid() {
		logger.Infof("tid=%s,procOneDelayMsg,topic is ivalid %s", tid, item.TopicName)
		return fstore.GetManagerMeta().RemoveDelay(item.Key)
	}
	pp := &protocol.DelayApplyPayload{
		Payload: item.Payload,
	}
	msg := &protocol.RawMessage{
		Command:   protocol.CommandDelayApply,
		TopicName: item.TopicName,
		Timestamp: time.Now().UnixMilli(),
		Body:      pp,
		TraceId:   tid,
	}
	if err = worker.Work(msg); err != nil {
		return err
	}

	return fstore.GetManagerMeta().RemoveDelay(item.Key)
}
