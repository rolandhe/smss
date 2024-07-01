package replica

import (
	"encoding/binary"
	"github.com/google/uuid"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/cmd/repair"
	"github.com/rolandhe/smss/pkg"
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
	writeTimeout time.Duration
}

func (r *replicaLongtimeReader) Output(conn net.Conn, msgs []*binlogBlock) error {
	hbuf := make([]byte, protocol.RespHeaderSize+4)
	binary.LittleEndian.PutUint16(hbuf, protocol.OkCode)
	msgLen := len(msgs[0].data)
	binary.LittleEndian.PutUint32(hbuf[protocol.RespHeaderSize:], uint32(msgLen))
	if err := nets.WriteAll(conn, hbuf, r.writeTimeout); err != nil {
		return err
	}
	return nets.WriteAll(conn, msgs[0].data, r.writeTimeout)
}

func getFilePosByMessageId(root string, seqId int64) (int64, int64, error) {
	var fileId int64
	var err error
	if seqId == 0 {
		fileId, err = standard.ReadFirstFileId(root)
		if err != nil {
			return 0, 0, err
		}
		return fileId, 0, nil
	}

	return repair.FindBinlogPosByMessageId(root, seqId)
}

func MasterHandle(conn net.Conn, header *protocol.CommonHeader, walMonitor WalMonitorSupport, readTimeout, writeTimeout time.Duration) error {
	buf := make([]byte, 8)
	err := nets.ReadAll(conn, buf, readTimeout)
	if err != nil {
		return err
	}
	lastPos := int64(binary.LittleEndian.Uint64(buf))

	if lastPos < 0 {
		return pkg.NewBizError("invalid replica pos")
	}

	ackTimeoutDuration := time.Duration(DefaultAckTimeout) * time.Millisecond

	uuidStr := uuid.NewString()
	fileId, pos, err := getFilePosByMessageId(walMonitor.GetRoot(), lastPos)
	if err != nil {
		log.Printf("tid=%s,replca server error:%v\n", header.TraceId, err)
		return nets.OutputRecoverErr(conn, err.Error(), writeTimeout)
	}

	reader := newBlockReader(uuidStr, walMonitor)

	if err = reader.Init(fileId, pos); err != nil {
		log.Printf("tid=%s,replica server init error:%v\n", header.TraceId, err)
		return nets.OutputRecoverErr(conn, err.Error(), writeTimeout)
	}

	err = nets.LongTimeRun[binlogBlock](conn, "replica", header.TraceId, ackTimeoutDuration, writeTimeout, &replicaLongtimeReader{
		serverBinlogBlockReader: reader,
		writeTimeout:            writeTimeout,
	})
	log.Printf("master handle finish err:%v\n", err)
	return err
}
