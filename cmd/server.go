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
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"github.com/rolandhe/smss/store/fss"
	"log"
	"net"
)

func StartServer(root string) {
	meta, err := fss.NewMeta(root)
	if err != nil {
		log.Printf("meta err:%v\n", err)
		return
	}

	var role store.InstanceRoleEnum
	role, err = meta.GetInstanceRole()
	if err != nil {
		log.Printf("get role error:%v\n", err)
		return
	}

	var nextSeq int64
	if nextSeq, err = repair.RepairLog(root, role, meta); err != nil {
		meta.Close()
		return
	}
	router.InitSeqId(nextSeq)
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

	startBgAndInitRouter(fstore, worker)
	if role == store.Master {
		router.InitReplica(w.StdMsgWriter)
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

func startBgAndInitRouter(fstore store.Store, worker standard.MessageWorking) {
	delExec := backgroud.StartMqFileDelete(fstore)
	backgroud.StartClearOldFiles(fstore, worker, delExec)
	lc := backgroud.StartLife(fstore, worker)
	delayCtrl := backgroud.StartDelay(fstore, worker)

	router.Init(fstore, lc, delExec)

	router.InitDelay(fstore, delayCtrl)
}

func handleConnection(conn net.Conn, worker *backWorker) {
	defer conn.Close()
	for {
		header, err := router.ReadHeader(conn)
		if err != nil {
			log.Printf("read header err:%v\n", err)
			return
		}

		if header.GetCmd() > protocol.CommandList {
			err = nets.OutputRecoverErr(conn, "don't support action")
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
			err = nets.OutputRecoverErr(conn, "don't support action")
			if err != nil && !pkg.IsBizErr(err) {
				return
			}
			continue
		}
		err = handler.Router(conn, header, worker)
		if err != nil {
			log.Printf("tid=%s,router error:%d\n", header.TraceId, header.GetCmd())
		}
		if err != nil && !pkg.IsBizErr(err) {
			return
		}
	}
}
