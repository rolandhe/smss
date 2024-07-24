package standard

import "time"

const (
	SyncFdIgnore = -1
	SyncFdNone   = 0
)

const MMapWindow int64 = 1024 * 512

type NotifyRegister interface {
	RegisterReaderNotify(notify *NotifyDevice) (LogFileInfoGet, error)
	UnRegisterReaderNotify()
}

func NewFutureMsg[T any](msg *T) *FutureMsg[T] {
	return &FutureMsg[T]{
		Msg:    msg,
		waiter: make(chan struct{}),
	}
}

type FutureMsg[T any] struct {
	Msg    *T
	waiter chan struct{}
	err    error
}

func (f *FutureMsg[T]) Wait() {
	<-f.waiter
}

func (f *FutureMsg[T]) Complete(err error) {
	f.err = err
	close(f.waiter)
}

func (f *FutureMsg[T]) WaitTimeout(timeout time.Duration) bool {
	select {
	case <-f.waiter:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (f *FutureMsg[T]) GetErr() error {
	return f.err
}
