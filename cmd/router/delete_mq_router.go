package router

import (
	"errors"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/pkg"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"log"
	"net"
	"os"
	"time"
)

type deleteMqRouter struct {
	fstore store.Store
	ddlRouter
	delExecutor protocol.DelMqFileExecutor
}

func (r *deleteMqRouter) Router(conn net.Conn, header *protocol.CommonHeader, worker standard.MessageWorking) error {
	if curInsRole != store.Master {
		return nets.OutputRecoverErr(conn, "just master can manage mq")
	}
	msg := &protocol.RawMessage{
		Command:   header.GetCmd(),
		MqName:    header.MQName,
		TraceId:   header.TraceId,
		Timestamp: time.Now().UnixMilli(),
	}
	return r.router(conn, msg, worker)
}

func (r *deleteMqRouter) DoBinlog(f *os.File, msg *protocol.RawMessage) (int64, error) {
	info, err := r.fstore.GetMqInfoReader().GetMQInfo(msg.MqName)
	if err != nil {
		return 0, err
	}
	if info == nil {
		if msg.Src == protocol.RawMessageReplica {
			return 0, nil
		}
		return 0, pkg.NewBizError("mq not exist")
	}
	setupRawMessageSeqIdAndWriteTime(msg, 1)
	return r.doBinlog(f, msg)
}
func (r *deleteMqRouter) AfterBinlog(msg *protocol.RawMessage, fileId, pos int64) error {
	err := deleteMqRoot(msg.MqName, "deleteMqRouter", r.fstore, r.delExecutor, msg.TraceId)
	log.Printf("tid=%s,deleteMqRouter.AfterBinlog %s err:%v\n", msg.TraceId, msg.MqName, err)
	return err
}

func deleteMqRoot(mqName, who string, fstore store.Store, delExecutor protocol.DelMqFileExecutor, traceId string) error {
	return fstore.ForceDeleteMQ(mqName, func() error {
		waiter := delExecutor.Submit(mqName, who, traceId)
		if !waiter(0) {
			log.Printf("tid=%s,delete %s mq file failed\n", traceId, mqName)
			return errors.New("delete mq file failed")
		}

		log.Printf("tid=%s,DeleteMqRoot %s ok\n", traceId, mqName)

		return nil
	})
}
