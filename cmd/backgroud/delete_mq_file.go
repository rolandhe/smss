package backgroud

import (
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/store"
	"os"
	"time"
)

type task struct {
	name    string
	notify  chan bool
	traceId string
	who     string
}

type mqDelExecutor struct {
	deleteList chan *task
	locker     *protocol.DelFileLock
	fstore     store.Store
}

func (de *mqDelExecutor) Submit(mqName, who string, traceId string) func(d time.Duration) bool {
	ch := make(chan bool, 1)
	t := &task{
		name:    mqName,
		notify:  ch,
		traceId: traceId,
		who:     who,
	}
	de.deleteList <- t
	return func(d time.Duration) bool {
		if d == 0 {
			return <-ch
		}
		select {
		case ok := <-ch:
			return ok
		case <-time.After(d):
			return false
		}
	}
}

func (de *mqDelExecutor) GetDeleteFileLocker() *protocol.DelFileLock {
	return de.locker
}

func (de *mqDelExecutor) run() {
	for {
		t := <-de.deleteList
		unlocker, waiter := de.locker.Lock(t.name, t.who, t.traceId)
		if waiter != nil {
			if !waiter(conf.WaitFileDeleteLockerTimeout) {
				logger.Get().Infof("waiter delete mq %s file locker failed", t.name)
				t.notify <- false
				continue
			}
			unlocker, _ = de.locker.Lock(t.name, t.name, t.traceId)
		}
		if unlocker == nil {
			t.notify <- false
			continue
		}
		p := de.fstore.GetMqPath(t.name)
		if err := removeMqPath(p, unlocker, t.traceId); err != nil {
			t.notify <- false
			continue
		}
		t.notify <- true
	}
}

func StartMqFileDelete(fstore store.Store, role store.InstanceRoleEnum) protocol.DelMqFileExecutor {
	exec := &mqDelExecutor{
		deleteList: make(chan *task, 128),
		fstore:     fstore,
		locker:     &protocol.DelFileLock{},
	}

	if role == store.Master {
		go exec.run()
	}

	return exec
}

func removeMqPath(p string, unlocker func(), traceId string) error {
	defer unlocker()
	_, err := os.Stat(p)
	if os.IsNotExist(err) {
		return nil
	}
	err = os.RemoveAll(p)
	logger.Get().Infof("tid=%s,deleteMqRoot %s error:%v", traceId, p, err)
	if err != nil {
		return err
	}

	return nil
}
