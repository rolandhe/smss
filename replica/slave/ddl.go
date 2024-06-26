package slave

import (
	"github.com/google/uuid"
	"github.com/rolandhe/smss/cmd/protocol"
)

func DDLMQHandle(cmd *protocol.DecodedRawMessage, payload []byte, worker DependWorker) error {
	msg := &protocol.RawMessage{
		Src:       protocol.RawMessageReplica,
		WriteTime: cmd.WriteTime,
		Command:   cmd.Command,
		MqName:    cmd.MqName,
		EventId:   cmd.EventId,
		Timestamp: cmd.Timestamp,
		TraceId:   uuid.NewString(),

		Body: &protocol.DDLPayload{
			Payload: payload,
		},
	}

	return worker.Work(msg)
}
