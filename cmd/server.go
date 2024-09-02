package cmd

import (
	"fmt"
	"github.com/rolandhe/smss/cmd/backgroud"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/cmd/repair"
	"github.com/rolandhe/smss/cmd/router"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/pkg/dir"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/pkg/tc"
	"github.com/rolandhe/smss/replica"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"github.com/rolandhe/smss/store/fss"
	"net"
)

type InstanceRole struct {
	Role     store.InstanceRoleEnum
	FromHost string
	FromPort int
	EventId  int64
}

func StartServer(root string, insRole *InstanceRole) {
	meta, err := fss.NewMeta(root)
	if err != nil {
		logger.Infof("meta err:%v", err)
		return
	}

	var storedRole store.InstanceRoleEnum
	storedRole, err = meta.GetInstanceRole()
	if err != nil {
		logger.Infof("get role error:%v", err)
		return
	}

	if storedRole != insRole.Role {
		if err = meta.SetInstanceRole(insRole.Role); err != nil {
			logger.Infof("set role error:%v", err)
			return
		}
	}

	var nextEventId int64
	if nextEventId, err = repair.CheckLogAndFix(root, meta); err != nil {
		meta.Close()
		return
	}
	router.InitCommonInfo(nextEventId, insRole.Role)
	w, fstore, err := newWriter(root, meta)
	if err != nil {
		meta.Close()
		logger.Infof("create writer err:%v", err)
		return
	}

	worker, err := startBack(conf.WorkerBuffSize, w, fstore)
	if err != nil {
		return
	}

	startBgAndInitRouter(root, fstore, worker, insRole.Role)
	if insRole.Role == store.Master {
		router.InitReplica(w.StdMsgWriter)
	} else {
		eventId := insRole.EventId
		needSync := true
		if nextEventId-1 > eventId {
			eventId = nextEventId - 1
			needSync = false
		}
		if err = replica.SlaveReplica(insRole.FromHost, insRole.FromPort, eventId, needSync, worker, fstore); err != nil {
			logger.Infof("SlaveReplica err:%v", err)
			return
		}
	}

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", conf.Port))
	if err != nil {
		logger.Errorf("listen err:%v", err)
		return
	}
	logger.Infof("started server:%d", conf.Port)
	for {
		var conn net.Conn
		conn, err = ln.Accept()
		if err != nil {
			logger.Infof("accept conn err,end server:%v", err)
			return
		}
		go handleConnection(conn, worker)
	}
	backgroud.StopClear()
}

func startBgAndInitRouter(root string, fstore store.Store, worker standard.MessageWorking, role store.InstanceRoleEnum) {
	delExec := backgroud.StartTopicFileDelete(fstore)
	backgroud.StartClearOldFiles(root, fstore, worker, delExec)
	var lc *tc.TimeTriggerControl
	if role == store.Master {
		delayCtrl := backgroud.StartDelay(fstore, worker)
		router.InitDelay(fstore, delayCtrl)
		lc = backgroud.StartLife(fstore, worker)
	} else {
		router.InitDelay(fstore, nil)
	}

	router.Init(fstore, lc, delExec)

}

func handleConnection(conn net.Conn, worker *backWorker) {
	var cmd protocol.CommandEnum
	defer func() {
		conn.Close()
		logger.Infof("handleConnection close with cmd:%d", cmd)
	}()
	continuesTimeoutCount := 0
	for {
		header, err := router.ReadHeader(conn)
		if err != nil {
			if nets.IsTimeoutError(err) {
				continuesTimeoutCount++
				if continuesTimeoutCount == 360 {
					logger.Infof("handleConnection no cmd more than 30 mins:%v", err)
					return
				}
				logger.Infof("handleConnection times=%d, read header err:%v", continuesTimeoutCount, err)
				continue
			}
			logger.Infof("handleConnection read header err:%v", err)
			return
		}
		continuesTimeoutCount = 0
		cmd = header.GetCmd()

		if header.GetCmd() > protocol.CommandList {
			err = nets.OutputRecoverErr(conn, "don't support action", router.NetWriteTimeout)
			if err != nil && !dir.IsBizErr(err) {
				return
			}
			continue
		}

		if header.GetCmd() == protocol.CommandAlive {
			if err = nets.OutAlive(conn, conf.DefaultIoWriteTimeout); err != nil {
				logger.Infof("tid:=%s,out alive err:%v", header.TraceId, err)
				return
			}
			continue
		}

		handler := router.GetRouter(header.GetCmd())
		if handler == nil {
			logger.Infof("tid=%s,don't support action:%d", header.TraceId, header.GetCmd())
			err = nets.OutputRecoverErr(conn, "don't support action", router.NetWriteTimeout)
			if err != nil && !dir.IsBizErr(err) {
				return
			}
			continue
		}
		err = handler.Router(conn, header, worker)
		if err != nil {
			logger.Infof("tid=%s,cmd=%d,router error:%v", header.TraceId, header.GetCmd(), err)
		}
		if err != nil && !dir.IsBizErr(err) {
			return
		}
	}
}
