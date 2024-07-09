package standard

import (
	"errors"
	"fmt"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/pkg/dir"
	"github.com/rolandhe/smss/pkg/logger"
	"os"
	"path"
)

var WaitNewTimeoutErr = errors.New("wait timeout")
var PeerClosedErr = errors.New("peer closed error")
var MqWriterTermiteErr = dir.NewBizError("mq writer closed,maybe mq deleted")

type CmdLine interface {
	GetPayloadSize() int
	GetCmd() protocol.CommandEnum
	GetId() int64
}

type MsgParser[T any] interface {
	ToMessage(payload []byte, fileId, pos int64) T
	Reset()
	ParseCmd(cmdBuf []byte) (CmdLine, error)
	ChangeMessagePos(ins T, fileId, pos int64)
}

func NewStdMsgBlockReader[T any](subject, root, whoami string, maxBatch int, maxLogFileSize int64, register NotifyRegister, parser MsgParser[*T]) *StdMsgBlockReader[T] {
	return &StdMsgBlockReader[T]{
		notify:   NewNotifyDevice(),
		root:     root,
		whoami:   whoami,
		subject:  subject,
		register: register,
		maxBatch: maxBatch,
		ctrl: &readerCtrl{
			maxLogFileSize: maxLogFileSize,
		},
		parser: parser,
	}
}

type StdMsgBlockReader[T any] struct {
	notify *NotifyDevice
	root   string
	whoami string
	// 主题名称
	subject string

	register NotifyRegister

	maxBatch int

	ctrl *readerCtrl

	parser MsgParser[*T]

	infoGet LogFileInfoGet

	logCount int64
}

func (r *StdMsgBlockReader[T]) Read(endNotify <-chan int) ([]*T, error) {
	if r.notify.IsTermite() {
		return nil, errors.New("mq not exist")
	}
	if err := r.waitFs(endNotify); err != nil {
		return nil, err
	}
	// read data
	return r.readCore(endNotify)
}

func (r *StdMsgBlockReader[T]) waitFs(endNotify <-chan int) error {
	for {
		if r.ctrl.curFs != nil {
			break
		}
		if r.ctrl.pos == 0 {
			if err := r.ctrl.ensureFs(r.root, r.infoGet); err != nil {
				return err
			}
			if r.ctrl.curFs != nil {
				// 可能是空文件，直接跳过到下一个
				if r.ctrl.isEOF() {
					r.ctrl.reset()
					continue
				}
				break
			}
		}
		waitRet := r.notify.Wait(endNotify)
		if waitRet == WaitNotifyByInput {
			logger.Get().Infof("%s waited file %d,notify to %s,ret=WaitNotifyByInput", r.subject, r.ctrl.fileId, r.whoami)
			return PeerClosedErr
		}
		if waitRet == WaitNotifyResultTermite {
			logger.Get().Infof("%s waited file %d,notify to %s, ret=WaitNotifyResultTermite", r.subject, r.ctrl.fileId, r.whoami)
			return MqWriterTermiteErr
		}
		if waitRet == WaitNotifyResultTimeout {
			logger.Get().Infof("%s waited file %d,notify to %s, ret=WaitNotifyResultTimeout", r.subject, r.ctrl.fileId, r.whoami)
			return WaitNewTimeoutErr
		}
		if r.logCount%conf.LogSample == 0 {
			logger.Get().Infof("%s waited file %d ok,notify to %s,count=%d", r.subject, r.ctrl.fileId, r.whoami, r.logCount)
		}
		r.logCount++

		err := r.ctrl.ensureFs(r.root, r.infoGet)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *StdMsgBlockReader[T]) Close() error {
	r.register.UnRegisterReaderNotify()
	return nil
}

func (r *StdMsgBlockReader[T]) Init(fileId, pos int64) error {
	if fileId < 0 || pos < 0 {
		return dir.NewBizError("invalid pos")
	}

	p := path.Join(r.root, fmt.Sprintf("%d.log", fileId))
	info, err := os.Stat(p)

	if err != nil && os.IsNotExist(err) && pos > 0 {
		return dir.NewBizError("invalid pos")
	}

	r.infoGet, err = r.register.RegisterReaderNotify(r.notify)
	if err != nil {
		return dir.NewBizError(err.Error())
	}

	lastFileId, lastPos := r.infoGet()

	if fileId > lastFileId || (fileId == lastFileId && pos > lastPos) {
		return dir.NewBizError("invalid pos")
	}

	if lastFileId > fileId && pos == info.Size() {
		fileId++
		pos = 0
	}

	r.ctrl.fileId = fileId
	r.ctrl.pos = pos
	err = r.ctrl.ensureFs(r.root, r.infoGet)
	if err != nil {
		logger.Get().Infof("init reader err:%v", err)
		r.register.UnRegisterReaderNotify()
		return dir.NewBizError(err.Error())
	}

	if pos > r.ctrl.fileSize {
		r.register.UnRegisterReaderNotify()
		return dir.NewBizError("invalid pos")
	}

	return nil
}

func (r *StdMsgBlockReader[T]) waitPos(endNotify <-chan int) error {
	for {
		if r.ctrl.pos < r.ctrl.fileSize {
			break
		}
		waitRet := r.notify.Wait(endNotify)
		if waitRet == WaitNotifyByInput {
			logger.Get().Infof("%s waited notify %d.%d %s,but notified by endNotify, ret=WaitNotifyByInput(conn closed)", r.subject, r.ctrl.fileId, r.ctrl.pos, r.whoami)
			return PeerClosedErr
		}
		if waitRet == WaitNotifyResultTermite {
			logger.Get().Infof("%s waited pos,notify %d.%d %s,ret=WaitNotifyResultTermite", r.subject, r.ctrl.fileId, r.ctrl.pos, r.whoami)
			return MqWriterTermiteErr
		}
		if waitRet == WaitNotifyResultTimeout {
			logger.Get().Infof("%s waited pos,notify %d.%d %s,ret=WaitNotifyResultTimeout", r.subject, r.ctrl.fileId, r.ctrl.pos, r.whoami)
			return WaitNewTimeoutErr
		}
		if r.logCount%conf.LogSample == 0 {
			logger.Get().Infof("%s waited pos ok,notify %d.%d %s,count=%d", r.subject, r.ctrl.fileId, r.ctrl.pos, r.whoami, r.logCount)
		}
		r.logCount++

		fid, fsize := r.infoGet()
		if fid > r.ctrl.fileId {
			p := path.Join(r.root, fmt.Sprintf("%d.log", r.ctrl.fileId))
			info, err := os.Stat(p)
			if err != nil {
				return err
			}
			r.ctrl.fileSize = info.Size()
		} else if fid == r.ctrl.fileId {
			r.ctrl.fileSize = fsize
		} else {
			return errors.New("file number decrease")
		}
	}
	return nil
}

func (r *StdMsgBlockReader[T]) readCore(endNotify <-chan int) ([]*T, error) {
	if err := r.waitPos(endNotify); err != nil {
		return nil, err
	}

	rctx := &readContext{
		pos: r.ctrl.pos,
		fd:  r.ctrl.curFs.Fd(),
	}

	defer rctx.clearMmapData()

	var readMsgs []*T
	step := 0
	var cmdStep commandStep
	var plStep payloadStep
	for {
		if r.ctrl.pos == r.ctrl.fileSize {
			break
		}
		if err := rctx.next(r.ctrl.fileSize); err != nil {
			return nil, err
		}

		if step == 0 {
			isCmd, err := cmdStep.accept(rctx)
			if err != nil {
				return nil, err
			}
			if isCmd {
				var cmdLine CmdLine
				if cmdLine, err = r.parser.ParseCmd(cmdStep.getCmdBuf()); err != nil {
					return nil, err
				}
				plStep.size = cmdLine.GetPayloadSize()
				plStep.payload = make([]byte, plStep.size)
				step = 1
			}
			continue
		}

		if step == 1 {
			ok, err := plStep.accept(rctx)
			if err != nil {
				return nil, err
			}
			if ok {
				step = 0
				msg := r.parser.ToMessage(plStep.payload, r.ctrl.fileId, rctx.pos)
				readMsgs = append(readMsgs, msg)

				r.ctrl.pos = rctx.pos
				r.parser.Reset()
				cmdStep.reset()
				plStep.reset()
			}
			if len(readMsgs) == r.maxBatch {
				break
			}
			continue
		}
	}

	if r.ctrl.isEOF() {
		logger.Get().Infof("%s-%s log file EOF", r.subject, r.whoami)
		r.ctrl.reset()
		if len(readMsgs) > 0 {
			last := readMsgs[len(readMsgs)-1]
			r.parser.ChangeMessagePos(last, r.ctrl.fileId, r.ctrl.pos)
		}
	}

	return readMsgs, nil
}

type readerCtrl struct {
	maxLogFileSize int64
	fileId         int64
	pos            int64
	curFs          *os.File
	fileSize       int64
	fixed          bool
}

func (c *readerCtrl) isEOF() bool {
	return c.pos == c.fileSize && (c.fixed || c.fileSize >= c.maxLogFileSize)
}
func (c *readerCtrl) reset() {
	c.curFs.Close()
	c.curFs = nil
	c.fixed = false
	c.pos = 0
	c.fileId++
}

func (c *readerCtrl) ensureFs(root string, posFunc LogFileInfoGet) error {
	if c.curFs != nil {
		return nil
	}
	p := path.Join(root, fmt.Sprintf("%d.log", c.fileId))
	info, err := os.Stat(p)

	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	c.fixed = false
	if info.Size() >= c.maxLogFileSize {
		c.fileSize = info.Size()
		c.fixed = true
	} else {
		fid, fsize := posFunc()
		if fid > c.fileId {
			c.fileSize = info.Size()
			c.fixed = true
		} else if fid == c.fileId {
			c.fileSize = fsize
		} else {
			return errors.New("invalid file id")
		}
	}
	if c.curFs, err = os.Open(p); err != nil {
		return err
	}

	return nil
}
