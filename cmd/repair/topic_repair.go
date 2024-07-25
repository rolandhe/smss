package repair

import (
	"github.com/rolandhe/smss/store"
	"github.com/rolandhe/smss/store/fss"
	"os"
)

func repairPub(lBinlog *lastBinlog, binlogFile, dataRoot string, meta store.Meta) error {
	topicPath := fss.TopicPath(dataRoot, lBinlog.topicName)
	p, _, fileSize, err := ensureLogFile(topicPath)
	if err != nil {
		return err
	}
	if p == "" {
		return os.Truncate(binlogFile, lBinlog.pos)
	}

	extractor := &extractTopicLog{}
	lTopicLog, err := readLastLogBlock[fss.TopicMessageCommand, lastTopicLog](0, p, fileSize, extractor)
	if err != nil {
		return err
	}
	if lTopicLog.srcFileId != lBinlog.fileId || lTopicLog.srcPos != lBinlog.pos {
		return os.Truncate(binlogFile, lBinlog.pos)
	}

	return nil
}

func repairDelayApply(lBinlog *lastBinlog, binlogFile, dataRoot string, meta store.Meta) error {
	delayKey := store.DelayKeyFromPayload(lBinlog.topicName,lBinlog.payload)
	exist, err := meta.ExistDelay(delayKey)
	if err != nil {
		return err
	}
	if !exist {
		return nil
	}

	info, err := meta.GetTopicInfo(lBinlog.topicName)
	if err != nil {
		return err
	}
	if info == nil || info.IsInvalid() {
		if err = os.Truncate(binlogFile, lBinlog.pos); err != nil {
			return err
		}
		return meta.RemoveDelay(delayKey)
	}

	topicPath := fss.TopicPath(dataRoot, lBinlog.topicName)
	p, _, fileSize, err := ensureLogFile(topicPath)
	if err != nil {
		return err
	}
	if p == "" {
		return os.Truncate(binlogFile, lBinlog.pos)
	}

	extractor := &extractTopicLog{}
	lTopicLog, err := readLastLogBlock[fss.TopicMessageCommand, lastTopicLog](0, p, fileSize, extractor)
	if err != nil {
		return err
	}
	if lTopicLog.srcFileId != lBinlog.fileId || lTopicLog.srcPos != lBinlog.pos {
		if err = os.Truncate(binlogFile, lBinlog.pos); err != nil {
			return err
		}
	}

	return meta.RemoveDelay(delayKey)
}
