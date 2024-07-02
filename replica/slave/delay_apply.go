package slave

import (
	"github.com/google/uuid"
	"github.com/rolandhe/smss/cmd/protocol"
)

func DelayApplyHandler(cmd *protocol.DecodedRawMessage, payload []byte, worker DependWorker) error {
	daPayload := &protocol.DelayApplyPayload{
		Payload: payload,
	}

	if err := worker.RemoveDelayByName(payload[:16], cmd.MqName); err != nil {
		return err
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
