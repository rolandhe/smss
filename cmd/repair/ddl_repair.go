package repair

import (
	"github.com/rolandhe/smss/store"
	"github.com/rolandhe/smss/store/fss"
	"os"
)

func repairCreate(lBinlog *lastBinlog, binlogFile, dataRoot string, meta store.Meta) error {
	info, err := meta.GetMQInfo(lBinlog.mqName)
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
	info, err := meta.GetMQInfo(lBinlog.mqName)
	if err != nil {
		return err
	}
	if info == nil {
		return nil
	}
	if info.State == store.MqStateNormal {
		err = os.Truncate(binlogFile, lBinlog.pos)
		return err
	}

	mqRoot := fss.MqPath(dataRoot, info.Name)
	_, err = os.Stat(mqRoot)
	if err == nil {
		if err = os.RemoveAll(mqRoot); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	_, err = meta.DeleteMQ(info.Name, true)
	return err
}

//func repairChangLf(lBinlog *lastBinlog, binlogFile, dataRoot string, meta store.Meta) error {
//	lf := int64(binary.LittleEndian.Uint64(lBinlog.payload))
//	info, err := meta.GetMQInfo(lBinlog.mqName)
//	if err != nil {
//		return err
//	}
//	if info == nil || info.ExpireAt != lf {
//		err = os.Truncate(binlogFile, lBinlog.pos)
//		return err
//	}
//	return nil
//}
