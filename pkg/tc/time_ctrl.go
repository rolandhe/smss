package tc

import (
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/store"
	"math"
	"sync"
	"time"
)

const (
	DefaultFirstDelay = 5000
)

func NewTimeTriggerControl(fstore store.Store, name string, firstRunDelayMs int64, doBiz func(fstore store.Store) int64) *TimeTriggerControl {
	if firstRunDelayMs <= 0 {
		firstRunDelayMs = DefaultFirstDelay
	}
	return &TimeTriggerControl{
		quickAlive: make(chan struct{}, 1),
		recent:     time.Now().UnixMilli() + firstRunDelayMs,
		fstore:     fstore,
		doBiz:      doBiz,
		name:       name,
	}
}

type TimeTriggerControl struct {
	sync.Mutex
	quickAlive chan struct{}
	recent     int64
	fstore     store.Store

	name  string
	doBiz func(fstore store.Store) int64
}

func (lc *TimeTriggerControl) Set(expireAt int64, needNotify bool) bool {
	lc.Lock()
	defer lc.Unlock()
	if expireAt >= lc.recent {
		return false
	}
	lc.recent = expireAt
	if needNotify {
		lc.notify()
	} else {
		lc.dry()
	}
	return true
}

func (lc *TimeTriggerControl) since() (int64, int64) {
	lc.Lock()
	defer lc.Unlock()
	curTs := time.Now().UnixMilli()
	v := lc.recent - curTs
	if v < 0 {
		v = 0
		lc.recent = math.MaxInt64
	}
	lc.dry()
	return v, lc.recent
}

func (lc *TimeTriggerControl) notify() {
	select {
	case lc.quickAlive <- struct{}{}:
	default:

	}
}
func (lc *TimeTriggerControl) dry() {
	select {
	case <-lc.quickAlive:
	default:

	}
}
func (lc *TimeTriggerControl) Process() {
	for {
		waitDurationMs, currentRecent := lc.since()
		if waitDurationMs == 0 {
			logger.Get().Infof("%s, immediate to doBiz", lc.name)
			nextTimeStamp := lc.doBiz(lc.fstore)
			lc.Set(nextTimeStamp, false)
			logger.Get().Infof("%s, after immediate, and next wait timeout from doBiz is %d(%v)", lc.name, nextTimeStamp, time.UnixMilli(nextTimeStamp).Local())
			continue
		}
		waitNextStamp := waitedRealTime(waitDurationMs)
		logger.Get().Infof("%s, wait duration %d ms, next time is %v", lc.name, waitDurationMs, waitNextStamp)
		bizWakeup := lc.waitNext(waitDurationMs)
		if bizWakeup {
			logger.Get().Infof("%s, wake up by front biz, to reset wait timeout, last wait is %d", lc.name, waitDurationMs)
			continue
		}

		logger.Get().Infof("%s, wake up by wait timeout %d(%v), to doBiz", lc.name, waitDurationMs, waitNextStamp)
		lc.consumeRecent(currentRecent)
		nextTimeStamp := lc.doBiz(lc.fstore)
		lc.Set(nextTimeStamp, false)
		logger.Get().Infof("%s, after doBiz, and next wait timeout from biz is %d(%v)", lc.name, nextTimeStamp, time.UnixMilli(nextTimeStamp).Local())
	}
}
func (lc *TimeTriggerControl) consumeRecent(currentRecent int64) {
	lc.Lock()
	defer lc.Unlock()
	if lc.recent <= currentRecent {
		lc.recent = math.MaxInt64
	}
}

func (lc *TimeTriggerControl) waitNext(waitTimeMs int64) bool {
	select {
	case <-lc.quickAlive:
		return true
	case <-time.After(time.Millisecond * time.Duration(waitTimeMs)):
		return false
	}
}

func waitedRealTime(waitTimeMs int64) time.Time {
	return time.Now().Add(time.Millisecond * time.Duration(waitTimeMs))
}
