package repair

import (
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/pkg"
	"github.com/rolandhe/smss/store"
	"log"
	"path"
)

func RepairLog(root string, meta store.Meta) (int64, error) {

	binlogRoot := path.Join(root, store.BinlogDir)
	exist, err := pkg.PathExist(binlogRoot)
	if err != nil {
		return 0, err
	}
	if !exist {
		return 1, nil
	}
	dataRoot := path.Join(root, store.DataDir)
	exist, err = pkg.PathExist(dataRoot)
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
	return getNextSeq(lBinlog), nil
}

func getNextSeq(lBinlog *lastBinlog) int64 {
	if lBinlog.cmd != protocol.CommandPub && lBinlog.cmd != protocol.CommandDelayApply {
		return lBinlog.messageSeqId + 1
	}
	payload := lBinlog.payload
	ok, count := protocol.CheckPayload(payload[:len(payload)-1])
	log.Println(ok)
	return lBinlog.messageSeqId + int64(count)
}
