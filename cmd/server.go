package cmd

import (
	"fmt"
	"github.com/rolandhe/smss/cmd/backgroud"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/cmd/repair"
	"github.com/rolandhe/smss/cmd/router"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/pkg"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/pkg/tc"
	"github.com/rolandhe/smss/replica"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"github.com/rolandhe/smss/store/fss"
	"log"
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
		log.Printf("meta err:%v\n", err)
		return
	}

	var storedRole store.InstanceRoleEnum
	storedRole, err = meta.GetInstanceRole()
	if err != nil {
		log.Printf("get role error:%v\n", err)
		return
	}

	if storedRole != insRole.Role {
		if err = meta.SetInstanceRole(insRole.Role); err != nil {
			log.Printf("set role error:%v\n", err)
			return
		}
	}

	var nextSeq int64
	if nextSeq, err = repair.RepairLog(root, meta); err != nil {
		meta.Close()
		return
	}
	router.InitSeqId(nextSeq, insRole.Role)
	w, fstore, err := newWriter(root, meta)
	if err != nil {
		meta.Close()
		log.Printf("create writer err:%v\n", err)
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
			log.Printf("SlaveReplica err:%v\n", err)
			return
		}
	}

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", conf.Port))
	if err != nil {
		// handle error
		return
	}
	log.Printf("finish to listen\n")
	for {
		var conn net.Conn
		conn, err = ln.Accept()
		if err != nil {
			log.Printf("accept conn err,end server:%v\n", err)
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
		log.Printf("handleConnection close with cmd:%d\n", cmd)
	}()
	for {
		header, err := router.ReadHeader(conn)
		if err != nil {
			log.Printf("handleConnection read header err:%v\n", err)
			return
		}
		cmd = header.GetCmd()

		if header.GetCmd() > protocol.CommandList {
			err = nets.OutputRecoverErr(conn, "don't support action", router.NetWriteTimeout)
			if err != nil && !pkg.IsBizErr(err) {
				return
			}
			continue
		}

		if header.GetCmd() == protocol.CommandAlive {
			if err = nets.OutAlive(conn, conf.DefaultIoWriteTimeout); err != nil {
				log.Printf("tid:=%s,out alive err:%v\n", header.TraceId, err)
				return
			}
			continue
		}

		handler := router.GetRouter(header.GetCmd())
		if handler == nil {
			log.Printf("tid=%s,don't support action:%d\n", header.TraceId, header.GetCmd())
			err = nets.OutputRecoverErr(conn, "don't support action", router.NetWriteTimeout)
			if err != nil && !pkg.IsBizErr(err) {
				return
			}
			continue
		}
		err = handler.Router(conn, header, worker)
		if err != nil {
			log.Printf("tid=%s,cmd=%d,router error:%v\n", header.TraceId, header.GetCmd(), err)
		}
		if err != nil && !pkg.IsBizErr(err) {
			return
		}
	}
}
