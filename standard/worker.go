package standard

import "github.com/rolandhe/smss/cmd/protocol"

type MessageWorking interface {
	Work(msg *protocol.RawMessage) error
}
