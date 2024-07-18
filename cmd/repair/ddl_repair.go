package repair

import (
	"github.com/rolandhe/smss/store"
	"github.com/rolandhe/smss/store/fss"
	"os"
)

func repairCreate(lBinlog *lastBinlog, binlogFile, dataRoot string, meta store.Meta) error {
	info, err := meta.GetTopicInfo(lBinlog.topicName)
	if err != nil {
		return err
	}
	if info == nil {
		err = os.Truncate(binlogFile, lBinlog.pos)
		return err
	}
	return nil
}
func repairDelete(lBinlog *lastBinlog, binlogFile, dataRoot string, meta store.Meta) error {
	info, err := meta.GetTopicInfo(lBinlog.topicName)
	if err != nil {
		return err
	}
	if info == nil {
		return nil
	}
	if info.State == store.TopicStateNormal {
		err = os.Truncate(binlogFile, lBinlog.pos)
		return err
	}

	topicRoot := fss.TopicPath(dataRoot, info.Name)
	_, err = os.Stat(topicRoot)
	if err == nil {
		if err = os.RemoveAll(topicRoot); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	_, err = meta.DeleteTopic(info.Name, true)
	return err
}
