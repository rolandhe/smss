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

type topicDelExecutor struct {
	deleteList chan *task
	locker     *protocol.DelFileLock
	fstore     store.Store
}

func (de *topicDelExecutor) Submit(topicName, who string, traceId string) func(d time.Duration) bool {
	ch := make(chan bool, 1)
	t := &task{
		name:    topicName,
		notify:  ch,
		traceId: traceId,
		who:     who,
	}
	de.deleteList <- t
	return func(d time.Duration) bool {
		if d == 0 {
			return <-ch
		}
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case ok := <-ch:
			return ok
		case <-timer.C:
			return false
		}
	}
}

func (de *topicDelExecutor) GetDeleteFileLocker() *protocol.DelFileLock {
	return de.locker
}

func (de *topicDelExecutor) run() {
	for {
		t := <-de.deleteList
		unlocker, waiter := de.locker.Lock(t.name, t.who, t.traceId)
		if waiter != nil {
			if !waiter(conf.WaitFileDeleteLockerTimeout) {
				logger.Infof("waiter delete topic %s file locker failed", t.name)
				t.notify <- false
				continue
			}
			unlocker, _ = de.locker.Lock(t.name, t.name, t.traceId)
		}
		if unlocker == nil {
			t.notify <- false
			continue
		}
		p := de.fstore.GetTopicPath(t.name)
		if err := deleteTopicPath(p, unlocker, t.traceId); err != nil {
			t.notify <- false
			continue
		}
		t.notify <- true
	}
}

func StartTopicFileDelete(fstore store.Store) protocol.DelTopicFileExecutor {
	exec := &topicDelExecutor{
		deleteList: make(chan *task, 128),
		fstore:     fstore,
		locker:     &protocol.DelFileLock{},
	}

	go exec.run()

	return exec
}

func deleteTopicPath(p string, unlocker func(), traceId string) error {
	defer unlocker()
	_, err := os.Stat(p)
	if os.IsNotExist(err) {
		return nil
	}
	err = os.RemoveAll(p)
	logger.Infof("tid=%s,deleteTopicPath %s error:%v", traceId, p, err)
	if err != nil {
		return err
	}

	return nil
}
