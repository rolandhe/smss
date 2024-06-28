package backgroud

import (
	"fmt"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/pkg/tc"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"log"
	"time"
)

//const (
//	DefaultScanLifeTime int64 = 120 * 60 * 1000
//)

func StartLife(fstore store.Store, worker standard.MessageWorking) *tc.TimeTriggerControl {
	lc := tc.NewTimeTriggerControl(fstore, "life", conf.LifeDefaultScanSec*1000, func(fstore store.Store) int64 {
		traceId := fmt.Sprintf("life-%d", time.Now().UnixMilli())
		return doLife(fstore, worker, traceId)
	})
	go lc.Process()

	return lc
}

func doLife(fstore store.Store, worker standard.MessageWorking, traceId string) int64 {
	mqs, next, err := fstore.GetScanner().ScanExpireMqs()
	if err != nil {
		log.Printf("tid=%s,doLife err:%v\n", traceId, err)
		return 0
	}

	for _, mqName := range mqs {
		if err = removeMq(worker, mqName, traceId); err != nil {
			log.Printf("tid=%s,doLife DeleteMqRoot %s err:%v\n", traceId, mqName, err)
			return 0
		}
	}

	now := time.Now().UnixMilli()
	if next <= 0 {
		next = now + conf.LifeDefaultScanSec*1000
	}
	log.Printf("tid=%s,doLife ok\n", traceId)
	return next
}

func removeMq(worker standard.MessageWorking, mqName string, traceId string) error {
	msg := &protocol.RawMessage{
		Command:   protocol.CommandDeleteMQ,
		MqName:    mqName,
		TraceId:   traceId,
		Timestamp: time.Now().UnixMilli(),
	}
	return worker.Work(msg)
}
