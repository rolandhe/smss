package repair

import (
	"github.com/rolandhe/smss/store"
	"github.com/rolandhe/smss/store/fss"
	"os"
)

func repairPub(lBinlog *lastBinlog, binlogFile, dataRoot string, meta store.Meta) error {
	mqPath := fss.MqPath(dataRoot, lBinlog.mqName)
	p, _, fileSize, err := ensureLogFile(mqPath)
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
	delayKey := make([]byte, 16+len(lBinlog.mqName))
	copy(delayKey, lBinlog.payload[:16])
	copy(delayKey[16:], lBinlog.mqName)
	exist, err := meta.ExistDelay(delayKey)
	if err != nil {
		return err
	}
	if !exist {
		return nil
	}

	info, err := meta.GetMQInfo(lBinlog.mqName)
	if err != nil {
		return err
	}
	if info == nil || info.IsInvalid() {
		if err = os.Truncate(binlogFile, lBinlog.pos); err != nil {
			return err
		}
		return meta.RemoveDelay(delayKey)
	}

	mqPath := fss.MqPath(dataRoot, lBinlog.mqName)
	p, _, fileSize, err := ensureLogFile(mqPath)
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

	//buf := lBinlog.payload[16:]

	//batchSize := int(binary.LittleEndian.Uint32(buf[4:]))
	//if batchSize == lMqLog.indexOfBatch+1 {
	//	return meta.RemoveDelay(delayKey)
	//}
	//firstPos, err := readFirstMqLogPos(p, fileSize, lBinlog.fileId, lBinlog.pos)
	//if err != nil {
	//	return err
	//}
	//if err = os.Truncate(p, firstPos); err != nil {
	//	return err
	//}
	//
	//if err = maybeRemove(p); err != nil {
	//	return err
	//}
	//
	//return os.Truncate(binlogFile, lBinlog.pos)

	//return meta.RemoveDelay(delayKey)
}

//func readFirstMqLogPos(p string, fileSize int64, binlogFileId, binlogPos int64) (int64, error) {
//	f, err := os.Open(p)
//	if err != nil {
//		return 0, err
//	}
//	defer f.Close()
//
//	cmdCommonSize := 512
//
//	buf := make([]byte, cmdCommonSize)
//
//	var pos int64
//	var cmdLine fss.MqMessageCommand
//	var allSize int64
//
//	for {
//		if err = readAllBuf(buf[:4], f); err != nil {
//			return 0, err
//		}
//
//		cmdLen := int(binary.LittleEndian.Uint32(buf))
//
//		var cBuf []byte
//		if cmdLen <= cmdCommonSize {
//			cBuf = buf[:cmdLen]
//		} else {
//			cBuf = make([]byte, cmdLen)
//		}
//
//		err = readAllBuf(cBuf, f)
//		if err != nil && err != io.EOF {
//			return 0, errors.New("bad file")
//		}
//		if err = fss.ReadMqMessageCmd(cBuf, &cmdLine); err != nil {
//			return 0, err
//		}
//
//		if cmdLine.SrcFileId == binlogFileId && cmdLine.SrcPos == binlogPos {
//			return pos, nil
//		}
//
//		payloadLen := cmdLine.CmdLen
//		allSize = int64(cmdLen+4) + int64(payloadLen)
//
//		if pos+allSize == fileSize {
//			break
//		}
//
//		if payloadLen > 0 {
//			var nextPos int64
//			nextPos, err = f.Seek(int64(payloadLen), io.SeekCurrent)
//			if err != nil {
//				return 0, err
//			}
//			log.Printf("%d\n", nextPos)
//		}
//
//		pos += allSize
//
//	}
//
//	return 0, errors.New("not found")
//}
