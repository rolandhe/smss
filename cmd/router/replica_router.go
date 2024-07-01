package router

import (
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/replica"
	"github.com/rolandhe/smss/standard"
	"net"
)

type replicaRouter struct {
	binlogWriter *standard.StdMsgWriter[protocol.RawMessage]
	noBinlog
}

func (r *replicaRouter) Router(conn net.Conn, commHeader *protocol.CommonHeader, worker standard.MessageWorking) error {
	return replica.MasterHandle(conn, commHeader, r.binlogWriter, NetReadTimeout, NetWriteTimeout)
}
