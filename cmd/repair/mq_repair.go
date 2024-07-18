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

	extractor := &extractMqLog{}
	lMqLog, err := readLastLogBlock[fss.MqMessageCommand, lastMqLog](0, p, fileSize, extractor)
	if err != nil {
		return err
	}
	if lMqLog.srcFileId != lBinlog.fileId || lMqLog.srcPos != lBinlog.pos {
		return os.Truncate(binlogFile, lBinlog.pos)
	}

	return nil
}

func repairDelayApply(lBinlog *lastBinlog, binlogFile, dataRoot string, meta store.Meta) error {
	delayKey := make([]byte, 16+len(lBinlog.topicName))
	copy(delayKey, lBinlog.payload[:16])
	copy(delayKey[16:], lBinlog.topicName)
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

	extractor := &extractMqLog{}
	lMqLog, err := readLastLogBlock[fss.MqMessageCommand, lastMqLog](0, p, fileSize, extractor)
	if err != nil {
		return err
	}
	if lMqLog.srcFileId != lBinlog.fileId || lMqLog.srcPos != lBinlog.pos {
		if err = os.Truncate(binlogFile, lBinlog.pos); err != nil {
			return err
		}
	}

	return meta.RemoveDelay(delayKey)
}
