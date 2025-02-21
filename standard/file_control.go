package standard

import (
	"errors"
	"fmt"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/pkg/logger"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type OutputMsgFunc[T any] func(f *os.File, msg *T) (int64, error)
type LogFileInfoGet func() (int64, int64)

type AfterWriteCallback func(fileId, pos int64) (int, error)

type WaitNotifyResult int

const (
	WaitNotifyResultOK       WaitNotifyResult = 0
	WaitNotifyTopicDeleted   WaitNotifyResult = 1
	WaitNotifyResultTimeout  WaitNotifyResult = 2
	WaitNotifyByClientClosed WaitNotifyResult = 3
)

type NotifyDevice struct {
	notify           chan struct{}
	wg               atomic.Pointer[sync.WaitGroup]
	deleteTopicState atomic.Bool
}

func NewNotifyDevice() *NotifyDevice {
	return &NotifyDevice{
		notify: make(chan struct{}, 1),
	}
}

func (nd *NotifyDevice) DeleteTopicNotify() {
	nd.deleteTopicState.Store(true)
	close(nd.notify)
}

func (nd *NotifyDevice) Notify() bool {
	select {
	case nd.notify <- struct{}{}:
		return true
	default:
		return false
	}
}
func (nd *NotifyDevice) Wait(clientClosedNotifyChan <-chan struct{}) WaitNotifyResult {
	timer := time.NewTimer(conf.ServerAliveTimeout)
	defer timer.Stop()
	select {
	case <-clientClosedNotifyChan:
		return WaitNotifyByClientClosed
	case <-nd.notify:
		if nd.IsDeleteTopic() {
			return WaitNotifyTopicDeleted
		}
		return WaitNotifyResultOK
	case <-timer.C:
		return WaitNotifyResultTimeout
	}
}

func (nd *NotifyDevice) IsDeleteTopic() bool {
	return nd.deleteTopicState.Load()
}

type LogFileControl interface {
	Set(fileId, currentSize int64)
	Get() (int64, int64)
	RegNotify(name string, notify *NotifyDevice) (LogFileInfoGet, error)
	UnRegNotify(name string)
	Notify()
	InvalidByDeleteTopic()
	IsInvalid() bool
}

func NewLogFileControl(subject, ppath string) (LogFileControl, error) {
	maxId, err := ReadMaxFileId(ppath)
	if err != nil {
		return nil, err
	}
	return &logFileCtrl{
		fileId: maxId,
		notify: &notifier{
			subject: subject,
			waiters: map[string]*NotifyDevice{},
		},
	}, nil
}

type logFileCtrl struct {
	sync.RWMutex
	fileId      int64
	curFileSize int64
	notify      *notifier
	invalid     atomic.Bool
}
type notifier struct {
	sync.Mutex
	// 主题名称, 标示为哪个对象处理日志文件
	subject string
	// key： 我是谁
	waiters  map[string]*NotifyDevice
	logCount int64
}

func (n *notifier) notifyAll(closed bool) {
	n.Lock()
	defer n.Unlock()
	n.logCount++
	for k, c := range n.waiters {
		if closed {
			c.DeleteTopicNotify()
			return
		}
		if c.Notify() {
			if conf.LogSample > 0 && n.logCount%conf.LogSample == 0 {
				logger.Infof("writer of %s notify to %s,count=%d", n.subject, k, n.logCount)
			}
		}
	}
}

func (fc *logFileCtrl) Set(fileId, fileSize int64) {
	fc.Lock()
	defer fc.Unlock()
	fc.fileId = fileId
	fc.curFileSize = fileSize
}

func (fc *logFileCtrl) Get() (int64, int64) {
	fc.RLock()
	defer fc.RUnlock()
	return fc.fileId, fc.curFileSize
}

func (fc *logFileCtrl) RegNotify(name string, notify *NotifyDevice) (LogFileInfoGet, error) {
	fc.notify.Lock()
	defer fc.notify.Unlock()
	_, ok := fc.notify.waiters[name]
	if ok {
		return nil, errors.New(fmt.Sprintf("%s regiester exist", name))
	}
	fc.notify.waiters[name] = notify

	return func() (int64, int64) {
		return fc.Get()
	}, nil
}

func (fc *logFileCtrl) UnRegNotify(name string) {
	fc.notify.Lock()
	defer fc.notify.Unlock()
	delete(fc.notify.waiters, name)
}

func (fc *logFileCtrl) Notify() {
	fc.notify.notifyAll(false)
}

func (fc *logFileCtrl) InvalidByDeleteTopic() {
	fc.invalid.Store(true)
	fc.notify.notifyAll(true)
}
func (fc *logFileCtrl) IsInvalid() bool {
	return fc.invalid.Load()
}
