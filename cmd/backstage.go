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
	"time"
)

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

	topicWriterFunc := func(msg *protocol.RawMessage, fileId, pos int64) error {
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
	for {
		msg := worker.waitMsg(conf.WorkerWaitMsgTimeout)
		if msg == nil {
			continue
		}
		err := worker.writer.Write(msg.Msg)
		msg.Complete(err)
	}
}

func (worker *backWorker) Work(msg *protocol.RawMessage) error {
	fmsg := standard.NewFutureMsg(msg)
	worker.c <- fmsg
	fmsg.Wait()
	return fmsg.GetErr()
}

func (worker *backWorker) waitMsg(timeout time.Duration) *standard.FutureMsg[protocol.RawMessage] {
	select {
	case msg := <-worker.c:
		return msg
	case <-time.After(timeout):
		if worker.CanLogger() {
			logger.Get().Infof("get future task tomeout")
		}
		return nil
	}
}
