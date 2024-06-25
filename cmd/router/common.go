package router

import (
	"github.com/rolandhe/smss/binlog"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/pkg/nets"
	"net"
	"os"
)

func ReadHeader(conn net.Conn) (*protocol.CommonHeader, error) {
	buff := make([]byte, protocol.HeaderSize)
	if err := nets.ReadAll(conn, buff); err != nil {
		return nil, err
	}
	header := protocol.NewCommonHeader(buff)
	nameLen := header.GetMqNameLen()
	traceLen := header.GetTraceIdLen()
	if nameLen+traceLen == 0 {
		return header, nil
	}
	nextBuf := make([]byte, nameLen+traceLen)

	if err := nets.ReadAll(conn, nextBuf); err != nil {
		return nil, err
	}
	if nameLen > 0 {
		header.MQName = string(nextBuf[:nameLen])
	}
	if traceLen > 0 {
		header.TraceId = string(nextBuf[nameLen:])
	}
	return header, nil
}

type MessageWorking interface {
	Work(msg *protocol.RawMessage) error
}

type ddlRouter struct {
}

func (ddl *ddlRouter) router(conn net.Conn, msg *protocol.RawMessage, worker MessageWorking) error {
	err := worker.Work(msg)
	if err != nil {
		return nets.OutputRecoverErr(conn, err.Error())
	}
	return nets.OutputOk(conn)
}

func (ddl *ddlRouter) doBinlog(f *os.File, msg *protocol.RawMessage) (int64, error) {
	buff := binlog.DDLEncoder(msg)
	return buff.WriteTo(f)
}
