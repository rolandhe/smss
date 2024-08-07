package replica

import (
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/replica/slave"
)

type binlogBlockHandler func(cmd *protocol.DecodedRawMessage, payload []byte, work slave.DependWorker) error

var bbHandlerMap = map[protocol.CommandEnum]binlogBlockHandler{}

func init() {
	bbHandlerMap[protocol.CommandPub] = slave.PubHandler
	bbHandlerMap[protocol.CommandDelayApply] = slave.DelayApplyHandler
	bbHandlerMap[protocol.CommandDelay] = slave.DelayHandler
	bbHandlerMap[protocol.CommandCreateTopic] = slave.DDLTopicHandle
	bbHandlerMap[protocol.CommandDeleteTopic] = slave.DDLTopicHandle
}
