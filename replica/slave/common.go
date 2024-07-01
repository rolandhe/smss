package slave

import (
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
)

type DependWorker interface {
	standard.MessageWorking
	store.ManagerMeta
}
