package router

import (
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/pkg/tc"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"net"
	"os"
)

var routerMap = map[protocol.CommandEnum]CmdRouter{}

type CmdRouter interface {
	Router(conn net.Conn, header *protocol.CommonHeader, worker standard.MessageWorking) error
	DoBinlog(f *os.File, msg *protocol.RawMessage) (int64, error)
	AfterBinlog(msg *protocol.RawMessage, fileId, pos int64) error
}

type noBinlog struct {
}

func (h *noBinlog) DoBinlog(f *os.File, msg *protocol.RawMessage) (int64, error) {
	return 0, nil
}

func (h *noBinlog) AfterBinlog(msg *protocol.RawMessage, fileId, pos int64) error {
	return nil
}

func Init(fstore store.Store, lc *tc.TimeTriggerControl, delExec protocol.DelMqFileExecutor) {
	sampleLogger := &routerSampleLogger{
		SampleLoggerSupport: logger.NewSampleLoggerSupport(conf.LogSample),
	}
	routerMap[protocol.CommandSub] = &subRouter{
		fstore: fstore,
	}
	routerMap[protocol.CommandPub] = &pubRouter{
		fstore:             fstore,
		routerSampleLogger: sampleLogger,
	}

	routerMap[protocol.CommandCreateTopic] = &createMqRouter{
		fstore: fstore,
		lc:     lc,
	}

	routerMap[protocol.CommandDeleteTopic] = &deleteMqRouter{
		fstore:      fstore,
		delExecutor: delExec,
	}

	routerMap[protocol.CommandList] = &topicListRouter{
		fstore: fstore,
	}

	routerMap[protocol.CommandValidList] = &validListRouter{
		fstore: fstore,
	}

	routerMap[protocol.CommandDelayApply] = &delayApplyRouter{
		fstore:             fstore,
		routerSampleLogger: sampleLogger,
	}
}
func InitDelay(fstore store.Store, delayCtrl *tc.TimeTriggerControl) {
	routerMap[protocol.CommandDelay] = &delayRouter{
		fstore:   fstore,
		delayCtl: delayCtrl,
	}
}

func InitReplica(binlogWriter *standard.StdMsgWriter[protocol.RawMessage]) {
	routerMap[protocol.CommandReplica] = &replicaRouter{
		binlogWriter: binlogWriter,
	}
}

func GetRouter(cmd protocol.CommandEnum) CmdRouter {
	return routerMap[cmd]
}
