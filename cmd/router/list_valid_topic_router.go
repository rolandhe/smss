package router

import (
	"encoding/binary"
	"encoding/json"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/pkg/dir"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"github.com/rolandhe/smss/store/fss"
	"net"
)

type validListRouter struct {
	fstore store.Store
	noBinlog
}

func (r *validListRouter) Router(conn net.Conn, commHeader *protocol.CommonHeader, worker standard.MessageWorking) error {
	buf := make([]byte, 8)
	if err := nets.ReadAll(conn, buf, NetReadTimeout); err != nil {
		return err
	}
	eventId := int64(binary.LittleEndian.Uint64(buf))
	infos, err := r.fstore.GetTopicInfoReader().GetTopicSimpleInfoList()
	if err != nil {
		logger.Get().Infof("tid=%s,GetTopicSimpleInfoList err:%v", commHeader.TraceId, err)
		return dir.NewBizError(err.Error())
	}
	var rets []*outTopicInfo
	for _, info := range infos {
		if info.State == store.TopicStateDeleted {
			continue
		}
		ok := eventId == 0 || (info.CreateEventId <= eventId && info.State == store.TopicStateNormal)
		if ok {
			rets = append(rets, &outTopicInfo{
				TopicInfo: info,
				TopicRoot: fss.TopicPath("/", info.Name),
			})
		}
	}
	if len(rets) == 0 {
		outBuff := make([]byte, protocol.RespHeaderSize)
		binary.LittleEndian.PutUint16(outBuff, protocol.OkCode)
		binary.LittleEndian.PutUint32(outBuff[2:], uint32(0))
		return nets.WriteAll(conn, outBuff, NetWriteTimeout)
	}

	jBuff, _ := json.Marshal(rets)
	outBuff := make([]byte, len(jBuff)+protocol.RespHeaderSize)
	binary.LittleEndian.PutUint16(outBuff, protocol.OkCode)
	binary.LittleEndian.PutUint32(outBuff[2:], uint32(len(jBuff)))
	copy(outBuff[protocol.RespHeaderSize:], jBuff)
	return nets.WriteAll(conn, outBuff, NetWriteTimeout)
}
