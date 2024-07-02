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
	SeqId    int64
}

func StartServer(root string, insRole *InstanceRole) {
	meta, err := fss.NewMeta(root)
	if err != nil {
		logger.Get().Infof("meta err:%v", err)
		return
	}

	var storedRole store.InstanceRoleEnum
	storedRole, err = meta.GetInstanceRole()
	if err != nil {
		logger.Get().Infof("get role error:%v", err)
		return
	}

	if storedRole != insRole.Role {
		if err = meta.SetInstanceRole(insRole.Role); err != nil {
			logger.Get().Infof("set role error:%v", err)
			return
		}
	}

	var nextSeq int64
	if nextSeq, err = repair.RepairLog(root, meta); err != nil {
		meta.Close()
		return
	}
	router.InitCommonInfo(nextSeq, insRole.Role)
	w, fstore, err := newWriter(root, meta)
	if err != nil {
		meta.Close()
		logger.Get().Infof("create writer err:%v", err)
		return
	}

	worker, err := startBack(conf.WorkerBuffSize, w, fstore)
	if err != nil {
		return
	}

	startBgAndInitRouter(fstore, worker, insRole.Role)
	if insRole.Role == store.Master {
		router.InitReplica(w.StdMsgWriter)
	} else {
		seqId := insRole.SeqId
		needSync := true
		if nextSeq-1 > seqId {
			seqId = nextSeq - 1
			needSync = false
		}
		if err = replica.SlaveReplica(insRole.FromHost, insRole.FromPort, seqId, needSync, worker, fstore); err != nil {
			logger.Get().Infof("SlaveReplica err:%v", err)
			return
		}
	}

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", conf.Port))
	if err != nil {
		// handle error
		return
	}
	logger.Get().Infof("finish to listen")
	for {
		var conn net.Conn
		conn, err = ln.Accept()
		if err != nil {
			logger.Get().Infof("accept conn err,end server:%v", err)
			return
		}
		go handleConnection(conn, worker)
	}
	backgroud.StopClear()
}

func startBgAndInitRouter(fstore store.Store, worker standard.MessageWorking, role store.InstanceRoleEnum) {
	delExec := backgroud.StartMqFileDelete(fstore, role)
	backgroud.StartClearOldFiles(fstore, worker, delExec)
	var lc *tc.TimeTriggerControl
	if role == store.Master {
		delayCtrl := backgroud.StartDelay(fstore, worker)
		router.InitDelay(fstore, delayCtrl)
		lc = backgroud.StartLife(fstore, worker)
	}

	router.Init(fstore, lc, delExec)

}

func handleConnection(conn net.Conn, worker *backWorker) {
	var cmd protocol.CommandEnum
	defer func() {
		conn.Close()
		logger.Get().Infof("handleConnection close with cmd:%d", cmd)
	}()
	for {
		header, err := router.ReadHeader(conn)
		if err != nil {
			logger.Get().Infof("handleConnection read header err:%v", err)
			return
		}
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
				logger.Get().Infof("tid:=%s,out alive err:%v", header.TraceId, err)
				return
			}
			continue
		}

		handler := router.GetRouter(header.GetCmd())
		if handler == nil {
			logger.Get().Infof("tid=%s,don't support action:%d", header.TraceId, header.GetCmd())
			err = nets.OutputRecoverErr(conn, "don't support action", router.NetWriteTimeout)
			if err != nil && !dir.IsBizErr(err) {
				return
			}
			continue
		}
		err = handler.Router(conn, header, worker)
		if err != nil {
			logger.Get().Infof("tid=%s,cmd=%d,router error:%v", header.TraceId, header.GetCmd(), err)
		}
		if err != nil && !dir.IsBizErr(err) {
			return
		}
	}
}
