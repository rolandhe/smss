package router

import (
	"github.com/rolandhe/smss/binlog"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"log"
	"net"
	"os"
	"time"
)

const NetReadTimeout = time.Millisecond * 2000
const NetHeaderTimeout = time.Millisecond * 5000
const NetWriteTimeout = time.Millisecond * 2000

var nextEventId int64
var curInsRole store.InstanceRoleEnum

// InitCommonInfo 初始化初始event id和实例角色
func InitCommonInfo(id int64, role store.InstanceRoleEnum) {
	if role == store.Master {
		nextEventId = id
		log.Printf("init next event id:%d\n", nextEventId)
	}
	curInsRole = role
}

func setupRawMessageEventIdAndWriteTime(msg *protocol.RawMessage, count int) {
	if msg.Src == protocol.RawMessageReplica {
		return
	}
	msg.WriteTime = time.Now().UnixMilli()
	msg.EventId = nextEventId
	nextEventId += int64(count)
}

func ReadHeader(conn net.Conn) (*protocol.CommonHeader, error) {
	buff := make([]byte, protocol.HeaderSize)
	if err := nets.ReadAll(conn, buff, NetHeaderTimeout); err != nil {
		return nil, err
	}
	header := protocol.NewCommonHeader(buff)
	nameLen := header.GetMqNameLen()
	traceLen := header.GetTraceIdLen()
	if nameLen+traceLen == 0 {
		return header, nil
	}
	nextBuf := make([]byte, nameLen+traceLen)

	if err := nets.ReadAll(conn, nextBuf, NetReadTimeout); err != nil {
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

type ddlRouter struct {
}

func (ddl *ddlRouter) router(conn net.Conn, msg *protocol.RawMessage, worker standard.MessageWorking) error {
	err := worker.Work(msg)
	if err != nil {
		return nets.OutputRecoverErr(conn, err.Error(), NetWriteTimeout)
	}
	return nets.OutputOk(conn, NetWriteTimeout)
}

func (ddl *ddlRouter) doBinlog(f *os.File, msg *protocol.RawMessage) (int64, error) {
	buff := binlog.DDLEncoder(msg)
	return buff.WriteTo(f)
}
