package tc

import (
	"github.com/rolandhe/smss/store"
	"log"
	"math"
	"sync"
	"time"
)

const (
	FirstExecDelay          = 5
	DefaultWaitTimeoutMills = 10
)

func NewTimeTriggerControl(fstore store.Store, name string, maxDefaultWait int64, doBiz func(fstore store.Store) int64) *TimeTriggerControl {
	return &TimeTriggerControl{
		quickAlive:     make(chan struct{}, 1),
		recent:         math.MaxInt64,
		fstore:         fstore,
		doBiz:          doBiz,
		name:           name,
		maxDefaultWait: maxDefaultWait,
	}
}

type TimeTriggerControl struct {
	quickAlive chan struct{}
	sync.Mutex
	recent int64
	fstore store.Store

	name string

	maxDefaultWait int64

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

func (lc *TimeTriggerControl) since() time.Duration {
	lc.Lock()
	defer lc.Unlock()
	if lc.recent == math.MaxInt64 {
		return time.Millisecond * time.Duration(lc.maxDefaultWait)
	}
	v := lc.recent - time.Now().UnixMilli()
	if v <= 0 {
		v = DefaultWaitTimeoutMills
	}
	lc.recent = math.MaxInt64
	return time.Millisecond * time.Duration(v)
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
	d := time.Second * FirstExecDelay
	for {
		select {
		case <-lc.quickAlive:
			d = lc.since()
			log.Printf("delay message notify for %s of %d\n", lc.name, d.Milliseconds())
		case <-time.After(d):
			log.Printf("wakeup for %s of %dms\n", lc.name, d.Milliseconds())
			next := lc.doBiz(lc.fstore)
			if next <= 0 {
				next = time.Now().Add(time.Millisecond * DefaultWaitTimeoutMills).UnixMilli()
			}
			lc.Set(next, false)
			d = lc.since()
		}
	}
}
