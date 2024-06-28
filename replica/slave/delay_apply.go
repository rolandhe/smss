package slave

import (
	"github.com/google/uuid"
	"github.com/rolandhe/smss/cmd/protocol"
)

func DelayApplyHandler(cmd *protocol.DecodedRawMessage, payload []byte, worker DependWorker) error {
	daPayload := &protocol.DelayApplyPayload{
		Payload: payload,
	}
	msg := &protocol.RawMessage{
		Src:          protocol.RawMessageReplica,
		WriteTime:    cmd.WriteTime,
		Command:      cmd.Command,
		MessageSeqId: cmd.MessageSeqId,
		MqName:       cmd.MqName,
		TraceId:      uuid.NewString(),
		Timestamp:    cmd.Timestamp,
		Body:         daPayload,
	}

	return worker.Work(msg)
}
