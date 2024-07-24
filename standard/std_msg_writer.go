package standard

import (
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/pkg/logger"
	"io"
	"os"
	"runtime"
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
		w.curFs.Sync()
		w.curFs.Close()
		w.curFs = nil
	}
	return nil
}

func (w *StdMsgWriter[T]) Write(msg *T, cb AfterWriteCallback) (int, int, error) {
	var err error

	if err = w.ensureFs(); err != nil {
		return SyncFdIgnore, SyncFdIgnore, err
	}

	outSize, err := w.outputFunc(w.curFs, msg)

	if err != nil {
		return SyncFdIgnore, SyncFdIgnore, err
	}
	// 如果没有写出任何东西，也就没必要进入下一步了
	if outSize == 0 {
		return SyncFdIgnore, SyncFdIgnore, nil
	}

	fid, size := w.LogFileControl.Get()

	cbSyncFd := SyncFdIgnore
	if cb != nil {
		if cbSyncFd, err = cb(fid, size); err != nil {
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
			return SyncFdIgnore, SyncFdIgnore, err
		}
	}

	w.lastWriteTime = time.Now().UnixMilli()

	allSize := size + outSize

	syncFd := int(w.curFs.Fd())
	if allSize >= w.maxLogSize {
		w.curFs.Close()
		w.curFs = nil
		syncFd = SyncFdNone
		w.LogFileControl.Set(fid+1, 0)
	} else {
		w.LogFileControl.Set(fid, allSize)
	}
	w.LogFileControl.Notify()
	return syncFd, cbSyncFd, nil
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
	if conf.NoCache {
		if runtime.GOOS == "linux" {
			if err = posixFadvise(int(w.curFs.Fd())); err != nil {
				logger.Get().Infof("call posixFadvise err:%v", err)
				return nil
			}
		}
	}

	return nil
}
