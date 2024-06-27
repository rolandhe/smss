package router

import (
	"github.com/rolandhe/smss/binlog"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/pkg"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/store"
	"log"
	"net"
	"os"
	"time"
)

type pubRouter struct {
	fstore store.Store
}

func (r *pubRouter) Router(conn net.Conn, header *protocol.CommonHeader, worker MessageWorking) error {
	pubPayload, err := readPubPayload(conn, &protocol.PubProtoHeader{
		CommonHeader: header,
	})
	if err != nil {
		log.Printf("tid=%s,readPubPayload err:%v\n", header.TraceId, err)
		return err
	}

	msg := &protocol.RawMessage{
		Command:   header.GetCmd(),
		MqName:    header.MQName,
		TraceId:   header.TraceId,
		Timestamp: time.Now().UnixMilli(),
		Body:      pubPayload,
	}

	if err = worker.Work(msg); err != nil {
		log.Printf("tid=%s,pub to call Work err:%v\n", header.TraceId, err)
		return nets.OutputRecoverErr(conn, err.Error())
	}

	return nets.OutputOk(conn)
}

func (r *pubRouter) DoBinlog(f *os.File, msg *protocol.RawMessage) (int64, error) {
	info, err := r.fstore.GetMqInfoReader().GetMQInfo(msg.MqName)
	if err != nil {
		log.Printf("tid=%s,pubRouter.DoBinlog call mq %s info error:%v\n", msg.TraceId, msg.MqName, err)
		return 0, err
	}
	if info == nil || info.IsInvalid() {
		log.Printf("tid=%s,pubRouter.DoBinlog  %s not exist\n", msg.TraceId, msg.MqName)
		return 0, pkg.NewBizError("mq not exist")
	}

	payload := msg.Body.(*protocol.PubPayload)

	setupRawMessageSeqId(msg, payload.BatchSize)
	buff := binlog.PubEncoder(msg)

	var n int64
	n, err = buff.WriteTo(f)
	log.Printf("tid=%s,pubRouter.DoBinlog  %s finish:%v\n", msg.TraceId, msg.MqName, err)
	return n, err
}

func (r *pubRouter) AfterBinlog(msg *protocol.RawMessage, fileId, pos int64) error {
	payload := msg.Body.(*protocol.PubPayload)
	messages, _ := protocol.ParsePayload(payload.Payload, fileId, pos, msg.MessageSeqId)
	err := r.fstore.Save(msg.MqName, messages)
	log.Printf("tid=%s,pubRouter.AfterBinlog  %s finish:%v\n", msg.TraceId, msg.MqName, err)
	return err
}

func readPubPayload(conn net.Conn, header *protocol.PubProtoHeader) (*protocol.PubPayload, error) {
	payloadSize := header.GetPayloadSize()
	if payloadSize <= 8 {
		log.Printf("tid=%s,invalid request, payload size must be more than 8\n", header.TraceId)
		e := nets.OutputRecoverErr(conn, "invalid request, payload size must be more than 8")
		return nil, e
	}

	buf := make([]byte, payloadSize)
	var err error

	if err = nets.ReadAll(conn, buf); err != nil {
		return nil, err
	}

	ok, count := protocol.CheckPayload(buf)
	if !ok {
		e := nets.OutputRecoverErr(conn, "invalid pub payload")
		return nil, e
	}

	return &protocol.PubPayload{
		Payload:   buf,
		BatchSize: count,
	}, nil
}
