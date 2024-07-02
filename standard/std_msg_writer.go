package standard

import (
	"github.com/rolandhe/smss/pkg/logger"
	"io"
	"os"
	"time"
)

func NewMsgWriter[T any](subject, root string, maxLogSize int64, outputFunc OutputMsgFunc[T]) *StdMsgWriter[T] {
	fc, _ := NewLogFileControl(subject, root)
	return &StdMsgWriter[T]{
		root:           root,
		maxLogSize:     maxLogSize,
		LogFileControl: fc,
		outputFunc:     outputFunc,
	}
}

type StdMsgWriter[T any] struct {
	root       string
	maxLogSize int64

	LogFileControl
	outputFunc OutputMsgFunc[T]

	curFs         *os.File
	lastWriteTime int64
}

func (w *StdMsgWriter[T]) GetRoot() string {
	return w.root
}

func (w *StdMsgWriter[T]) Close() error {
	if w.curFs != nil {
		w.curFs.Close()
		w.curFs = nil
	}
	return nil
}

func (w *StdMsgWriter[T]) Write(msg *T, cb AfterWriteCallback) error {
	var err error

	if err = w.ensureFs(); err != nil {
		return err
	}

	outSize, err := w.outputFunc(w.curFs, msg)

	if err != nil {
		return err
	}
	// 如果没有写出任何东西，也就没必要进入下一步了
	if outSize == 0 {
		return nil
	}

	fid, size := w.LogFileControl.Get()

	if cb != nil {
		if err = cb(fid, size); err != nil {
			var e error
			var n int64
			if n, e = w.curFs.Seek(-outSize, io.SeekCurrent); e != nil {
				logger.Get().Infof("rollback to seek error,then panic:%v", e)
				panic("rollback error")
			}
			if e = w.curFs.Truncate(n); e != nil {
				logger.Get().Infof("rollback to truncate error,then panic:%v", e)
				panic("rollback error")
			}
			return err
		}
	}

	w.lastWriteTime = time.Now().UnixMilli()

	allSize := size + outSize

	if allSize >= w.maxLogSize {
		w.curFs.Close()
		w.curFs = nil
		w.LogFileControl.Set(fid+1, 0)
	} else {
		w.LogFileControl.Set(fid, allSize)
	}
	w.LogFileControl.Notify()
	return nil
}

func (w *StdMsgWriter[T]) ensureFs() error {
	if w.curFs != nil {
		return nil
	}

	fPath, err := GenLogFileFullPath(w.root, w.LogFileControl)
	if err != nil {
		return err
	}
	if w.curFs, err = os.Create(fPath); err != nil {
		return err
	}

	return nil
}
