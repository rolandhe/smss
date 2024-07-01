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

type validListRouter struct {
	fstore store.Store
	noBinlog
}

func (r *validListRouter) Router(conn net.Conn, commHeader *protocol.CommonHeader, worker standard.MessageWorking) error {
	buf := make([]byte, 8)
	if err := nets.ReadAll(conn, buf); err != nil {
		return err
	}
	eventId := int64(binary.LittleEndian.Uint64(buf))
	infos, err := r.fstore.GetMqInfoReader().GetMQSimpleInfoList()
	if err != nil {
		log.Printf("tid=%s,GetMQSimpleInfoList err:%v\n", commHeader.TraceId, err)
		return pkg.NewBizError(err.Error())
	}
	var rets []*outMqInfo
	for _, info := range infos {
		if info.State == store.MqStateDeleted {
			continue
		}
		ok := eventId == 0 || (info.CreateEventId <= eventId && (info.CreateEventId == info.ChangeExpireAtEventId || eventId < info.ChangeExpireAtEventId))
		if ok {
			rets = append(rets, &outMqInfo{
				MqInfo: info,
				MqRoot: fss.MqPath("/", info.Name),
			})
		}
	}
	if len(rets) == 0 {
		outBuff := make([]byte, protocol.RespHeaderSize)
		binary.LittleEndian.PutUint16(outBuff, protocol.OkCode)
		binary.LittleEndian.PutUint32(outBuff[2:], uint32(0))
		return nets.WriteAll(conn, outBuff)
	}

	jBuff, _ := json.Marshal(rets)
	outBuff := make([]byte, len(jBuff)+protocol.RespHeaderSize)
	binary.LittleEndian.PutUint16(outBuff, protocol.OkCode)
	binary.LittleEndian.PutUint32(outBuff[2:], uint32(len(jBuff)))
	copy(outBuff[protocol.RespHeaderSize:], jBuff)
	return nets.WriteAll(conn, outBuff)
}
