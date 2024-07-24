package binlog

import (
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/standard"
)

type CompleteHandlerFunc[T any] func(ins *T, fileId, pos int64) (int, error)

func NewWalWriter[T any](mWriter *standard.StdMsgWriter[T], completeHandler CompleteHandlerFunc[T]) *WalWriter[T] {
	return &WalWriter[T]{
		StdMsgWriter:    mWriter,
		completeHandler: completeHandler,
	}
}

type WalWriter[T any] struct {
	*standard.StdMsgWriter[T]
	completeHandler CompleteHandlerFunc[T]
}

func (w *WalWriter[T]) Write(msg *T) (int, int, error) {
	var err error
	var binlogSyncFd int
	var dataSyncFd int
	if binlogSyncFd, dataSyncFd, err = w.StdMsgWriter.Write(msg, func(fileId, pos int64) (int, error) {
		syncFd, e := w.completeHandler(msg, fileId, pos)
		if e != nil {
			logger.Get().Infof("to handle msg after writing binlog error,rollback")
		}
		return syncFd, e
	}); err != nil {
		return standard.SyncFdIgnore, standard.SyncFdIgnore, err
	}

	return binlogSyncFd, dataSyncFd, nil
}
