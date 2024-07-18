package slave

import (
	"github.com/google/uuid"
	"github.com/rolandhe/smss/cmd/protocol"
)

func PubHandler(cmd *protocol.DecodedRawMessage, payload []byte, worker DependWorker) error {
	_, count := protocol.CheckPayload(payload)
	pubPayload := &protocol.PubPayload{
		Payload:   payload,
		BatchSize: count,
	}
	msg := &protocol.RawMessage{
		Src:       protocol.RawMessageReplica,
		WriteTime: cmd.WriteTime,
		Command:   cmd.Command,
		EventId:   cmd.EventId,
		TopicName: cmd.TopicName,
		TraceId:   uuid.NewString(),
		Timestamp: cmd.Timestamp,
		Body:      pubPayload,
	}

	return worker.Work(msg)
}
