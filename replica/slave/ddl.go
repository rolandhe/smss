package slave

import (
	"github.com/google/uuid"
	"github.com/rolandhe/smss/cmd/protocol"
)

func DDLTopicHandle(cmd *protocol.DecodedRawMessage, payload []byte, worker DependWorker) error {
	msg := &protocol.RawMessage{
		Src:       protocol.RawMessageReplica,
		WriteTime: cmd.WriteTime,
		Command:   cmd.Command,
		TopicName: cmd.TopicName,
		EventId:   cmd.EventId,
		Timestamp: cmd.Timestamp,
		TraceId:   uuid.NewString(),

		Body: &protocol.DDLPayload{
			Payload: payload,
		},
	}

	return worker.Work(msg)
}
