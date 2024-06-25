package repair

import (
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/pkg"
	"github.com/rolandhe/smss/store"
	"path"
)

func RepairLog(root string, meta store.Meta) error {
	role, err := meta.GetInstanceRole()
	if err != nil {
		return err
	}
	if role == store.Master {
		return repairMaster(root, meta)
	}
	return nil
}

func repairMaster(root string, meta store.Meta) error {
	binlogRoot := path.Join(root, store.BinlogDir)
	exist, err := pkg.PathExist(binlogRoot)
	if err != nil {
		return err
	}
	if !exist {
		return nil
	}
	dataRoot := path.Join(root, store.DataDir)
	exist, err = pkg.PathExist(dataRoot)
	if err != nil {
		return err
	}
	if !exist {
		return nil
	}
	p, maxLogFileId, fileSize, err := ensureLogFile(binlogRoot)
	if err != nil {
		return err
	}
	if p == "" {
		return nil
	}

	extractor := &extractBinlog{
		fileId: maxLogFileId,
	}

	var lBinlog *lastBinlog

	lBinlog, err = readLastLogBlock[protocol.DecodedRawMessage, lastBinlog](0, p, fileSize, extractor)
	if err != nil {
		return err
	}

	handleFunc := repairHandlers[lBinlog.cmd]

	err = handleFunc(lBinlog, p, dataRoot, meta)
	if err != nil {
		return err
	}

	return maybeRemove(p)
}
