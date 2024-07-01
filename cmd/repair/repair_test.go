package repair

import (
	"github.com/rolandhe/smss/store/fss"
	"testing"
)

func TestRepairMaster(t *testing.T) {
	root := "../../mq-data"
	fstore, err := fss.NewFileStore(root, 50)
	if err != nil {
		t.Log(err)
		return
	}
	err = repairCore(root, fstore)
	t.Log(err)
}
