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

const BinlogOutTimeout = time.Second * 120

type WalMonitorSupport interface {
	GetRoot() string
	standard.LogFileControl
}

//type replicaLongtimeReader struct {
//	serverBinlogBlockReader
//	writeTimeout time.Duration
//}
//
//func (r *replicaLongtimeReader) Output(conn net.Conn, msgs []*binlogBlock) error {
//	hbuf := make([]byte, protocol.RespHeaderSize+4)
//	binary.LittleEndian.PutUint16(hbuf, protocol.OkCode)
//	msgLen := len(msgs[0].data)
//	binary.LittleEndian.PutUint32(hbuf[protocol.RespHeaderSize:], uint32(msgLen))
//	if err := nets.WriteAll(conn, hbuf, r.writeTimeout); err != nil {
//		return err
//	}
//	return nets.WriteAll(conn, msgs[0].data, r.writeTimeout)
//}

func getFilePosByEventId(root string, eventId int64) (int64, int64, error) {
	var fileId int64
	var err error
	if eventId == 0 {
		fileId, err = standard.ReadFirstFileId(root)
		if err != nil {
			return 0, 0, err
		}
		return fileId, 0, nil
	}

	return repair.FindBinlogPosByEventId(root, eventId)
}

func MasterHandle(conn net.Conn, header *protocol.CommonHeader, walMonitor WalMonitorSupport, readTimeout, writeTimeout time.Duration) error {
	buf := make([]byte, 8)
	err := nets.ReadAll(conn, buf, readTimeout)
	if err != nil {
		return err
	}
	lastEventId := int64(binary.LittleEndian.Uint64(buf))

	if lastEventId < 0 {
		return pkg.NewBizError("invalid replica eventId")
	}

	//ackTimeoutDuration := time.Duration(DefaultAckTimeout) * time.Millisecond

	uuidStr := uuid.NewString()
	fileId, pos, err := getFilePosByEventId(walMonitor.GetRoot(), lastEventId)
	if err != nil {
		log.Printf("tid=%s,replca server,eventId=%d, error:%v\n", header.TraceId, lastEventId, err)
		return nets.OutputRecoverErr(conn, err.Error(), writeTimeout)
	}

	reader := newBlockReader(uuidStr, walMonitor)

	if err = reader.Init(fileId, pos); err != nil {
		log.Printf("tid=%s,replica server,eventId=%d, init error:%v\n", header.TraceId, lastEventId, err)
		return nets.OutputRecoverErr(conn, err.Error(), writeTimeout)
	}

	//err = nets.LongTimeRun[binlogBlock](conn, "replica", header.TraceId, ackTimeoutDuration, writeTimeout, &replicaLongtimeReader{
	//	serverBinlogBlockReader: reader,
	//	writeTimeout:            writeTimeout,
	//})
	err = noAckPush(conn, header.TraceId, reader)
	if err != nil {
		log.Printf("master handle finish,eventId=%d, err:%v\n", lastEventId, err)
	}
	return err
}

func noAckPush(conn net.Conn, tid string, reader serverBinlogBlockReader) error {
	defer reader.Close()
	recvCh := make(chan int, 1)
	var err error
	count := int64(0)
	for {
		var msgs []*binlogBlock
		msgs, err = reader.Read(recvCh)
		if err == nil {
			outBuf := make([]byte, protocol.RespHeaderSize+4+len(msgs[0].data))
			binary.LittleEndian.PutUint16(outBuf, protocol.OkCode)
			msgLen := len(msgs[0].data)
			binary.LittleEndian.PutUint32(outBuf[protocol.RespHeaderSize:], uint32(msgLen))
			copy(outBuf[protocol.RespHeaderSize+4:], msgs[0].data)

			if err = nets.WriteAll(conn, outBuf, BinlogOutTimeout); err != nil {
				log.Printf("tid=%s, eventId=%d,err:%v\n", tid, msgs[0].rawMsg.EventId, err)
				return err
			}
			if count%100 == 0 {
				log.Printf("master to slave:tid=%s, eventId=%d,count=%d, delay time=%dms\n", tid, msgs[0].rawMsg.EventId, count, msgs[0].rawMsg.GetDelay())
			}
			count++
			continue
		}
		if err == standard.WaitNewTimeoutErr {
			err = nets.OutAlive(conn, BinlogOutTimeout)
			log.Printf("tid=%s,sub wait new data timeout, send alive:%v\n", tid, err)
			if err != nil {
				return err
			}
			continue
		}

		return nets.OutputRecoverErr(conn, err.Error(), BinlogOutTimeout)
	}
}
