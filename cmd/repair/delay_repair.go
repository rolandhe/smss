package repair

import (
	"github.com/rolandhe/smss/store"
	"os"
)

func repairDelay(lBinlog *lastBinlog, binlogFile, dataRoot string, meta store.Meta) error {
	key := make([]byte, 16+len(lBinlog.topicName))
	copy(key, lBinlog.payload[:16])
	copy(key[8:], lBinlog.topicName)
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
