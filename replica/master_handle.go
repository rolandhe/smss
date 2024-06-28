package replica

import (
	"encoding/binary"
	"github.com/google/uuid"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/cmd/repair"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/standard"
	"log"
	"net"
	"time"
)

const DefaultAckTimeout = 10 * 1000

type WalMonitorSupport interface {
	GetRoot() string
	standard.LogFileControl
}

type replicaLongtimeReader struct {
	serverBinlogBlockReader
}

func (r *replicaLongtimeReader) Output(conn net.Conn, msgs []*binlogBlock) error {
	return nets.WriteAll(conn, msgs[0].data)
}

func MasterHandle(conn net.Conn, header *protocol.CommonHeader, walMonitor WalMonitorSupport) error {
	buf := make([]byte, 8)
	err := nets.ReadAll(conn, buf)
	if err != nil {
		return err
	}
	lastPos := int64(binary.LittleEndian.Uint64(buf))

	ackTimeoutDuration := time.Duration(DefaultAckTimeout) * time.Millisecond

	uuidStr := uuid.NewString()
	fileId, pos, err := repair.FindBinlogPosByMessageId(walMonitor.GetRoot(), lastPos)
	if err != nil {
		log.Printf("tid=%s,replca server error:%v\n", header.TraceId, err)
		return nets.OutputRecoverErr(conn, err.Error())
	}

	reader := newBlockReader(uuidStr, walMonitor)

	if err = reader.Init(fileId, pos); err != nil {
		log.Printf("tid=%s,replica server init error:%v\n", header.TraceId, err)
		return nets.OutputRecoverErr(conn, err.Error())
	}

	return nets.LongTimeRun[binlogBlock](conn, "replica", header.TraceId, ackTimeoutDuration, &replicaLongtimeReader{
		serverBinlogBlockReader: reader,
	})
}
