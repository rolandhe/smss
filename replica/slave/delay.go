package slave

import (
	"github.com/google/uuid"
	"github.com/rolandhe/smss/cmd/protocol"
)

func DelayHandler(cmd *protocol.DecodedRawMessage, payload []byte, worker DependWorker) error {
	daPayload := &protocol.DelayApplyPayload{
		Payload: payload,
	}
	msg := &protocol.RawMessage{
		Src:       protocol.RawMessageReplica,
		WriteTime: cmd.WriteTime,
		Command:   cmd.Command,
		EventId:   cmd.EventId,
		MqName:    cmd.MqName,
		TraceId:   uuid.NewString(),
		Timestamp: cmd.Timestamp,
		Body:      daPayload,
	}

	return worker.Work(msg)
}
