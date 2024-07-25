package repair

import (
	"github.com/rolandhe/smss/store"
	"os"
)

func repairDelay(lBinlog *lastBinlog, binlogFile, dataRoot string, meta store.Meta) error {
	key := store.DelayKeyFromPayload(lBinlog.topicName, lBinlog.payload)
	exist, err := meta.ExistDelay(key)
	if err != nil {
		return err
	}
	if !exist {
		if err = os.Truncate(binlogFile, lBinlog.pos); err != nil {
			return err
		}
	}
	return nil
}
