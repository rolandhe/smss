package cmd

import (
	"github.com/rolandhe/smss/binlog"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/cmd/router"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/pkg/dir"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"github.com/rolandhe/smss/store/fss"
	"os"
	"path"
	"sync"
	"syscall"
	"time"
)

const secondMills = 1000

type backWorker struct {
	c      chan *standard.FutureMsg[protocol.RawMessage]
	writer *binlog.WalWriter[protocol.RawMessage]
	store  store.Store
	logger.SampleLoggerSupport
}

func newWriter(root string, meta store.Meta) (*binlog.WalWriter[protocol.RawMessage], store.Store, error) {
	binlogRoot := path.Join(root, store.BinlogDir)
	dir.EnsurePathExist(binlogRoot)

	fstore, err := fss.NewFileStore(root, meta)
	if err != nil {
		return nil, nil, err
	}

	binlogWriter := standard.NewMsgWriter[protocol.RawMessage](store.BinlogDir, binlogRoot, conf.MaxLogSize, func(f *os.File, msg *protocol.RawMessage) (int64, error) {
		handler := router.GetRouter(msg.Command)
		return handler.DoBinlog(f, msg)
	})

	topicWriterFunc := func(msg *protocol.RawMessage, fileId, pos int64) (int, error) {
		handler := router.GetRouter(msg.Command)
		return handler.AfterBinlog(msg, fileId, pos)
	}
	return binlog.NewWalWriter[protocol.RawMessage](binlogWriter, topicWriterFunc), fstore, nil

}

func startBack(buffSize int, w *binlog.WalWriter[protocol.RawMessage], store store.Store) (*backWorker, error) {
	worker := &backWorker{
		c:                   make(chan *standard.FutureMsg[protocol.RawMessage], buffSize),
		writer:              w,
		store:               store,
		SampleLoggerSupport: logger.NewSampleLoggerSupport(conf.WorkerWaitMsgTimeoutLogSample),
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		worker.process()
	}()
	wg.Wait()

	return worker, nil
}

func (worker *backWorker) process() {
	syncCtrl := buildFsyncControl()
	waitNext := conf.WorkerWaitMsgTimeout
	syncWake := false
	for {
		msg := worker.waitMsg(waitNext, syncWake)
		if msg == nil {
			if syncWake {
				syncCtrl.sync(0, 0, nil, true)
				syncWake = false
				waitNext = conf.WorkerWaitMsgTimeout
			}
			continue
		}
		binlogSyncFd, dataSyncFd, err := worker.writer.Write(msg.Msg)

		if msg.Msg.Command == protocol.CommandDeleteTopic {
			syncCtrl.rmTopic(msg.Msg.TopicName)
		}

		nextTime := syncCtrl.sync(binlogSyncFd, dataSyncFd, msg.Msg, false)
		msg.Complete(err)
		if nextTime == 0 {
			syncWake = false
			waitNext = conf.WorkerWaitMsgTimeout
		} else {
			syncWake = true
			waitNext = time.Duration(nextTime) * time.Millisecond
		}
	}
}

func buildFsyncControl() fsyncControl {
	if conf.FlushLevel == 0 {
		return &noneFsyncControl{}
	}
	if conf.FlushLevel == 2 {
		return &everyFsyncControl{}
	}
	return &secondaryFsyncControl{
		topicFdMap:          map[string]int{},
		SampleLoggerSupport: logger.NewSampleLoggerSupport(10),
	}
}

func (worker *backWorker) Work(msg *protocol.RawMessage) error {
	fmsg := standard.NewFutureMsg(msg)
	worker.c <- fmsg
	fmsg.Wait()
	return fmsg.GetErr()
}

func (worker *backWorker) waitMsg(timeout time.Duration, syncWake bool) *standard.FutureMsg[protocol.RawMessage] {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case msg := <-worker.c:
		return msg
	case <-timer.C:
		if !syncWake {
			if worker.CanLogger() {
				logger.Infof("get future task tomeout")
			}
		}
		return nil
	}
}

type fsyncControl interface {
	sync(blFd, dataFd int, msg *protocol.RawMessage, force bool) int64
	rmTopic(name string)
}

type everyFsyncControl struct {
}

func (efs *everyFsyncControl) sync(blFd, dataFd int, msg *protocol.RawMessage, force bool) int64 {
	if blFd > standard.SyncFdNone {
		if err := syscall.Fsync(blFd); err != nil {
			logger.Errorf("sync binlog fsync err: %v", err)
		}
	}
	if dataFd > standard.SyncFdNone {
		if err := syscall.Fsync(dataFd); err != nil {
			logger.Errorf("sync topic data fsync err: %v", err)
		}
	}
	return 0
}

func (efs *everyFsyncControl) rmTopic(name string) {}

type noneFsyncControl struct {
}

func (nfs *noneFsyncControl) sync(blFd, dataFd int, msg *protocol.RawMessage, force bool) int64 {
	return 0
}
func (nfs *noneFsyncControl) rmTopic(name string) {}

type secondaryFsyncControl struct {
	binlogFd   int
	topicFdMap map[string]int
	lastTime   int64
	logger.SampleLoggerSupport
}

func (sfs *secondaryFsyncControl) rmTopic(name string) {
	delete(sfs.topicFdMap, name)
}

func (sfs *secondaryFsyncControl) sync(blFd, dataFd int, msg *protocol.RawMessage, force bool) int64 {
	if force {
		sfs.syncFd(true)
		return 0
	}
	if sfs.lastTime == 0 {
		if blFd <= standard.SyncFdNone && dataFd <= standard.SyncFdNone {
			return 0
		}
		if blFd > standard.SyncFdNone {
			sfs.binlogFd = blFd
		}
		if dataFd > standard.SyncFdNone {
			sfs.topicFdMap[msg.TopicName] = dataFd
		}
		sfs.lastTime = msg.WriteTime
		return secondMills
	}

	if blFd != standard.SyncFdIgnore {
		sfs.binlogFd = blFd
	}
	if dataFd != standard.SyncFdIgnore {
		sfs.topicFdMap[msg.TopicName] = dataFd
	}

	interval := msg.WriteTime - sfs.lastTime

	if interval >= secondMills {
		sfs.syncFd(false)
		return 0
	}

	return interval
}

func (sfs *secondaryFsyncControl) syncFd(force bool) {
	count := 0
	if sfs.binlogFd > standard.SyncFdNone {
		if err := syscall.Fsync(sfs.binlogFd); err != nil {
			logger.Errorf("sync binlog fsync err: %v", err)
		}
		count++
	}

	for k, v := range sfs.topicFdMap {
		if v > standard.SyncFdNone {
			if err := syscall.Fsync(v); err != nil {
				logger.Errorf("sync topic fsync err: %v", err)
			}
			count++
		}
		if !force {
			sfs.topicFdMap[k] = standard.SyncFdNone
		}
	}

	if count > 0 {
		logger.Infof("syncFd: %d files", count)
	}
	sfs.lastTime = 0
	sfs.binlogFd = standard.SyncFdNone
	if force {
		sfs.topicFdMap = map[string]int{}
	}
}
