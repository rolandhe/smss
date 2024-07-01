package router

import (
	"encoding/binary"
	"encoding/json"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/pkg"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"github.com/rolandhe/smss/store/fss"
	"log"
	"net"
)

type mqListRouter struct {
	fstore store.Store
	noBinlog
}

type outMqInfo struct {
	*store.MqInfo
	MqRoot string `json:"mqRoot"`
}

func (r *mqListRouter) Router(conn net.Conn, commHeader *protocol.CommonHeader, worker standard.MessageWorking) error {
	infos, err := r.fstore.GetMqInfoReader().GetMQSimpleInfoList()
	if err != nil {
		log.Printf("tid=%s,GetMQSimpleInfoList err:%v\n", commHeader.TraceId, err)
		return pkg.NewBizError(err.Error())
	}
	if len(infos) == 0 {
		outBuff := make([]byte, protocol.RespHeaderSize)
		binary.LittleEndian.PutUint16(outBuff, protocol.OkCode)
		binary.LittleEndian.PutUint32(outBuff[2:], uint32(0))
		return nets.WriteAll(conn, outBuff, NetWriteTimeout)
	}
	rets := make([]*outMqInfo, 0, len(infos))
	for _, info := range infos {
		rets = append(rets, &outMqInfo{
			MqInfo: info,
			MqRoot: fss.MqPath("/", info.Name),
		})
	}
	jBuff, _ := json.Marshal(rets)
	outBuff := make([]byte, len(jBuff)+protocol.RespHeaderSize)
	binary.LittleEndian.PutUint16(outBuff, protocol.OkCode)
	binary.LittleEndian.PutUint32(outBuff[2:], uint32(len(jBuff)))
	copy(outBuff[protocol.RespHeaderSize:], jBuff)
	return nets.WriteAll(conn, outBuff, NetWriteTimeout)
}
