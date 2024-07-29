package router

import (
	"encoding/binary"
	"encoding/json"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"github.com/rolandhe/smss/store/fss"
	"net"
)

type topicListRouter struct {
	fstore store.Store
	noBinlog
}

type outTopicInfo struct {
	*store.TopicInfo
	TopicRoot string `json:"topicRoot"`
}

func (r *topicListRouter) Router(conn net.Conn, commHeader *protocol.CommonHeader, worker standard.MessageWorking) error {
	infos, err := r.fstore.GetTopicInfoReader().GetTopicSimpleInfoList()
	if err != nil {
		logger.Get().Infof("tid=%s,GetTopicSimpleInfoList err:%v", commHeader.TraceId, err)
		return nets.OutputRecoverErr(conn, err.Error(), NetWriteTimeout)
	}
	if len(infos) == 0 {
		outBuff := make([]byte, protocol.RespHeaderSize)
		binary.LittleEndian.PutUint16(outBuff, protocol.OkCode)
		binary.LittleEndian.PutUint32(outBuff[2:], uint32(0))
		return nets.WriteAll(conn, outBuff, NetWriteTimeout)
	}
	rets := make([]*outTopicInfo, 0, len(infos))
	for _, info := range infos {
		rets = append(rets, &outTopicInfo{
			TopicInfo: info,
			TopicRoot: fss.TopicPath("/", info.Name),
		})
	}
	jBuff, _ := json.Marshal(rets)
	outBuff := make([]byte, len(jBuff)+protocol.RespHeaderSize)
	binary.LittleEndian.PutUint16(outBuff, protocol.OkCode)
	binary.LittleEndian.PutUint32(outBuff[2:], uint32(len(jBuff)))
	copy(outBuff[protocol.RespHeaderSize:], jBuff)
	return nets.WriteAll(conn, outBuff, NetWriteTimeout)
}
