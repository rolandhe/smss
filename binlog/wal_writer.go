package binlog

import (
	"github.com/rolandhe/smss/standard"
	"log"
)

type CompleteHandlerFunc[T any] func(ins *T, fileId, pos int64) error

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

func (w *WalWriter[T]) Write(msg *T) error {
	var err error

	if err = w.StdMsgWriter.Write(msg, func(fileId, pos int64) error {
		e := w.completeHandler(msg, fileId, pos)
		if e != nil {
			log.Printf("to handle msg after writing binlog error,rollback")
		}
		return e
	}); err != nil {
		return err
	}

	return nil
}
