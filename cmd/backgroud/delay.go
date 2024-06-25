package backgroud

import (
	"fmt"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/cmd/router"
	"github.com/rolandhe/smss/pkg/tc"
	"github.com/rolandhe/smss/store"
	"log"
	"time"
)

const (
	DelayBatchSize      = 20
	DefaultDelayTimeout = 180 * 60 * 1000
)

func StartDelay(fstore store.Store, worker router.MessageWorking) *tc.TimeTriggerControl {
	f := func(fstore store.Store) int64 {
		return doDelay(fstore, worker)
	}

	lc := tc.NewTimeTriggerControl(fstore, "delay", DefaultDelayTimeout, f)

	go lc.Process()

	return lc
}

func doDelay(fstore store.Store, worker router.MessageWorking) int64 {
	tid := fmt.Sprintf("delay-%d", time.Now().UnixMilli())
	var ret int64
	for {
		delays, next, err := fstore.GetScanner().ScanDelays(DelayBatchSize)
		if err != nil {
			log.Printf("tid=%s,doDelay err:%v\n", tid, err)
			return 0
		}

		log.Printf("tid=%s,doDelay to process %d msg\n", tid, len(delays))
		for _, item := range delays {
			err = procOneDelayMsg(fstore, worker, item, tid)
			if err != nil {
				log.Printf("tid=%s,procOneDelayMsg err:%v\n", tid, err)
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
		ret = time.Now().UnixMilli() + DefaultDelayTimeout
	}
	log.Printf("tid=%s,doDelay ok,next time is %d, isDefualt %v\n", tid, ret, isDefault)
	return ret
}

func procOneDelayMsg(fstore store.Store, worker router.MessageWorking, item *store.DelayItem, tid string) error {
	info, err := fstore.GetMqInfoReader().GetMQInfo(item.MqName)
	if err != nil {
		log.Printf("tid=%s,procOneDelayMsg get mq info %s  error:%v\n", tid, item.MqName, err)
		return err
	}
	if info == nil || info.IsInvalid() {
		log.Printf("tid=%s,procOneDelayMsg,mq is ivalid  %s \n", tid, item.MqName)
		return fstore.GetScanner().RemoveDelay(item.Key)
	}
	pp := &protocol.DelayApplyPayload{
		Payload: item.Payload,
	}
	msg := &protocol.RawMessage{
		Command:   protocol.CommandDelayApply,
		MqName:    item.MqName,
		Timestamp: time.Now().UnixMilli(),
		Body:      pp,
		TraceId:   tid,
	}
	if err = worker.Work(msg); err != nil {
		return err
	}

	return fstore.GetScanner().RemoveDelay(item.Key)
}
