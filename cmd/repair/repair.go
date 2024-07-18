package repair

import (
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/pkg/dir"
	"github.com/rolandhe/smss/store"
	"path"
)

func CheckLogAndFix(root string, meta store.Meta) (int64, error) {
	binlogRoot := path.Join(root, store.BinlogDir)
	exist, err := dir.PathExist(binlogRoot)
	if err != nil {
		return 0, err
	}
	if !exist {
		return 1, nil
	}
	dataRoot := path.Join(root, store.TopicDir)
	exist, err = dir.PathExist(dataRoot)
	if err != nil {
		return 0, err
	}
	if !exist {
		return 1, nil
	}
	p, maxLogFileId, fileSize, err := ensureLogFile(binlogRoot)
	if err != nil {
		return 0, err
	}
	if p == "" {
		return 1, nil
	}

	extractor := &extractBinlog{
		fileId: maxLogFileId,
	}

	var lBinlog *lastBinlog

	lBinlog, err = readLastLogBlock[protocol.DecodedRawMessage, lastBinlog](0, p, fileSize, extractor)
	if err != nil {
		return 0, err
	}

	handleFunc := repairHandlers[lBinlog.cmd]

	err = handleFunc(lBinlog, p, dataRoot, meta)
	if err != nil {
		return 0, err
	}

	if err = maybeRemove(p); err != nil {
		return 0, err
	}
	return getNextEventId(lBinlog), nil
}

func getNextEventId(lBinlog *lastBinlog) int64 {
	if lBinlog.cmd != protocol.CommandPub && lBinlog.cmd != protocol.CommandDelayApply {
		return lBinlog.messageEventId + 1
	}
	payload := lBinlog.payload
	if lBinlog.cmd == protocol.CommandDelayApply {
		payload = payload[16:]
	}

	ok, count := protocol.CheckPayload(payload[:len(payload)-1])
	if !ok {
		panic("invalid payload")
	}

	return lBinlog.messageEventId + int64(count)
}
