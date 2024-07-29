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

type topicInfoRouter struct {
	fstore store.Store
	noBinlog
}

func (r *topicInfoRouter) Router(conn net.Conn, commHeader *protocol.CommonHeader, worker standard.MessageWorking) error {
	if len(commHeader.TopicName) == 0 {
		logger.Get().Infof("tid=%s,GetTopicInfo err:empty topic name", commHeader.TraceId)
		return nets.OutputRecoverErr(conn, "topic name is required", NetWriteTimeout)
	}
	info, err := r.fstore.GetTopicInfoReader().GetTopicInfo(commHeader.TopicName)
	if err != nil {
		logger.Get().Infof("tid=%s,GetTopicInfo err:%v", commHeader.TraceId, err)
		return nets.OutputRecoverErr(conn, err.Error(), NetWriteTimeout)
	}
	if info == nil {
		logger.Get().Infof("tid=%s,GetTopicInfo err:%s not exist", commHeader.TraceId, commHeader.TopicName)
		return nets.OutputRecoverErr(conn, "topic not exist", NetWriteTimeout)
	}
	oInfo := &outTopicInfo{
		TopicInfo: info,
		TopicRoot: fss.TopicPath("/", info.Name),
	}

	jBuff, _ := json.Marshal(oInfo)
	outBuff := make([]byte, len(jBuff)+protocol.RespHeaderSize)
	binary.LittleEndian.PutUint16(outBuff, protocol.OkCode)
	binary.LittleEndian.PutUint32(outBuff[2:], uint32(len(jBuff)))
	copy(outBuff[protocol.RespHeaderSize:], jBuff)
	return nets.WriteAll(conn, outBuff, NetWriteTimeout)
}
