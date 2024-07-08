package badger_meta

import (
	"encoding/binary"
	"errors"
	"github.com/dgraph-io/badger/v4"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/store"
	"time"
)

type badgerMeta struct {
	db *badger.DB
}

func NewMeta(path string) (store.Meta, error) {
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return nil, err
	}
	return &badgerMeta{
		db: db,
	}, nil
}

func (bm *badgerMeta) CreateMQ(mqName string, expireAt int64, eventId int64) (*store.MqInfo, error) {
	norMqName := normalMqName(mqName)
	exist, err := bm.existMq(norMqName)
	if err != nil {
		return nil, err
	}
	if exist {
		return nil, errors.New("mq exists")
	}

	info := &store.MqInfo{
		Name:            mqName,
		CreateTimeStamp: time.Now().UnixMilli(),
		ExpireAt:        expireAt,
	}
	valMeta := mqMetaValue{
		createTime:         info.CreateTimeStamp,
		expireAtTime:       expireAt,
		createEventId:      eventId,
		stateChangeTime:    info.CreateTimeStamp,
		stateChangeEventId: eventId,
		state:              store.MqStateNormal,
	}
	err = bm.db.Update(func(txn *badger.Txn) error {
		if e := txn.Set(norMqName, valMeta.toBytes()); e != nil {
			return e
		}
		if info.IsTemp() {
			if e := txn.Set(mqLifetimeName(mqName, expireAt), lifeValueHolder); e != nil {
				return e
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return info, nil
}

func (bm *badgerMeta) CopyCreateMq(info *store.MqInfo) error {
	norMqName := normalMqName(info.Name)
	exist, err := bm.existMq(norMqName)
	if err != nil {
		return err
	}
	if exist {
		return nil
	}

	var valMeta mqMetaValue

	valMeta.fromMqInfo(info)

	err = bm.db.Update(func(txn *badger.Txn) error {
		if e := txn.Set(norMqName, valMeta.toBytes()); e != nil {
			return e
		}
		if info.IsTemp() {
			if e := txn.Set(mqLifetimeName(info.Name, info.ExpireAt), lifeValueHolder); e != nil {
				return e
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (bm *badgerMeta) DeleteMQ(mqName string, force bool) (bool, error) {
	exist := true
	var valMeta mqMetaValue
	err := bm.db.Update(func(txn *badger.Txn) error {
		k := normalMqName(mqName)
		item, err := txn.Get(k)
		if errors.Is(err, badger.ErrKeyNotFound) {
			exist = false
			return nil
		}
		if err != nil {
			return err
		}

		if err = item.Value(func(val []byte) error {
			valMeta.fromBytes(val)
			return nil
		}); err != nil {
			return err
		}
		if valMeta.expireAtTime > 0 {
			if err = txn.Delete(mqLifetimeName(mqName, valMeta.expireAtTime)); err != nil {
				if !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
			}
		}
		if !force {
			valMeta.state = store.MqStateDeleted
			if err = txn.Set(k, valMeta.toBytes()); err != nil {
				return err
			}
		}
		if err = txn.Delete(k); err != nil {
			return err
		}
		return nil
	})
	return exist, err
}

func (bm *badgerMeta) ScanExpireMqs() ([]string, int64, error) {
	now := time.Now().UnixMilli()
	var next int64
	var mqs []string

	preLen := len(lifePrefix)
	err := bm.db.View(func(txn *badger.Txn) error {
		// Create an iterator for keys with a certain prefix
		opts := badger.DefaultIteratorOptions
		opts.Prefix = lifePrefix

		it := txn.NewIterator(opts)
		defer it.Close()

		// Iterate over the keys with the prefix
		for it.Seek(opts.Prefix); it.ValidForPrefix(opts.Prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			key = key[preLen:]
			expireAt := int64(binary.LittleEndian.Uint64(key))
			if expireAt > now {
				next = expireAt
				break
			}
			mqs = append(mqs, string(key[8:]))
		}

		return nil
	})
	logger.Get().Infof("ScanExpireMqs,next scan time from db:%d", next)
	return mqs, next, err
}

func (bm *badgerMeta) ScanDelays(batchSize int) ([]*store.DelayItem, int64, error) {
	now := time.Now().UnixMilli()
	var next int64
	ret := make([]*store.DelayItem, 0, batchSize)
	preLen := len(delayPrefix)
	err := bm.db.View(func(txn *badger.Txn) error {
		// Create an iterator for keys with a certain prefix
		opts := badger.DefaultIteratorOptions
		opts.Prefix = delayPrefix

		it := txn.NewIterator(opts)
		defer it.Close()

		// Iterate over the keys with the prefix
		for it.Seek(opts.Prefix); it.ValidForPrefix(opts.Prefix); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)

			buf := key[preLen:]
			expireAt := int64(binary.LittleEndian.Uint64(buf))
			if expireAt > now {
				next = expireAt
				break
			}
			valueBuf, e := item.ValueCopy(nil)
			if e != nil {
				return e
			}
			delay := &store.DelayItem{
				Key:     key,
				Payload: valueBuf,
				MqName:  string(buf[16:]),
			}
			ret = append(ret, delay)
			if len(ret) == batchSize {
				break
			}
		}
		return nil
	})
	return ret, next, err
}

func (bm *badgerMeta) RemoveDelay(key []byte) error {
	err := bm.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	}
	return err
}
func (bm *badgerMeta) RemoveDelayByName(data []byte, mqName string) error {
	preLen := len(delayPrefix)
	key := make([]byte, preLen+16+len(mqName))
	copy(key, delayPrefix)

	buf := key[preLen:]
	// copy 时间戳
	copy(buf, data)
	buf = buf[16:]
	copy(buf, mqName)
	return bm.RemoveDelay(key)
}

//func (bm *badgerMeta) ChangeMQLife(mqName string, life int64, eventId int64) error {
//	info, err := bm.GetMQInfo(mqName)
//	if err != nil {
//		return err
//	}
//	if info == nil || info.State == store.MqStateDeleted {
//		return errors.New("mq not exist")
//	}
//
//	if life == 0 && !info.IsTemp() {
//		return nil
//	}
//
//	normMqName := normalMqName(mqName)
//
//	err = bm.db.Update(func(txn *badger.Txn) error {
//		item, e := txn.Get(normMqName)
//		if e != nil {
//			return e
//		}
//		var mValue []byte
//		if mValue, e = item.ValueCopy(nil); e != nil {
//			return e
//		}
//		binary.LittleEndian.PutUint64(mValue[8:], uint64(life))
//		binary.LittleEndian.PutUint64(mValue[33:], uint64(eventId))
//		if e = txn.Set(normMqName, mValue); e != nil {
//			return e
//		}
//		if info.IsTemp() {
//			if e = txn.Delete(mqLifetimeName(mqName, info.ExpireAt)); e != nil {
//				return e
//			}
//		}
//		info.ExpireAt = life
//		if info.IsTemp() {
//			if e = txn.Set(mqLifetimeName(mqName, life), lifeValueHolder); e != nil {
//				return e
//			}
//		}
//		return nil
//	})
//	return nil
//}

func (bm *badgerMeta) GetMQInfo(mqName string) (*store.MqInfo, error) {
	var valMeta mqMetaValue

	err := bm.db.View(func(txn *badger.Txn) error {
		var e error
		var rawValue []byte
		if rawValue, e = getRawValue(normalMqName(mqName), txn); e != nil {
			return e
		}
		valMeta.fromBytes(rawValue)
		return nil
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return valMeta.toMqInfo(mqName), nil
}

func (bm *badgerMeta) GetMQSimpleInfoList() ([]*store.MqInfo, error) {
	var infoList []*store.MqInfo
	var valMeta mqMetaValue
	err := bm.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = normalPrefix

		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(opts.Prefix); it.ValidForPrefix(opts.Prefix); it.Next() {
			item := it.Item()
			mqName := string(item.Key()[len(normalPrefix):])

			if e := item.Value(func(val []byte) error {
				valMeta.fromBytes(val)
				return nil
			}); e != nil {
				return e
			}
			infoList = append(infoList, valMeta.toMqInfo(mqName))
		}
		return nil
	})

	return infoList, err
}

func (bm *badgerMeta) SaveDelay(mqName string, payload []byte) error {
	preLen := len(delayPrefix)
	key := make([]byte, preLen+16+len(mqName))
	copy(key, delayPrefix)

	buf := key[preLen:]
	// copy 时间戳+eventId
	copy(buf[:16], payload)
	copy(buf[16:], mqName)

	return bm.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, payload)
	})
}
func (bm *badgerMeta) ExistDelay(key []byte) (bool, error) {
	exist := false
	err := bm.db.View(func(txn *badger.Txn) error {
		var e error
		if exist, e = existKey(key, txn); e != nil {
			return e
		}
		return nil
	})

	return exist, err
}

//func (bm *badgerMeta) GetDelayId() (uint64, error) {
//	return bm.globalIdGen.Next()
//}

//func (bm *badgerMeta) SaveCheckPoint(key string, fileId, pos int64) error {
//	buf := make([]byte, 16)
//	binary.LittleEndian.PutUint64(buf, uint64(fileId))
//	binary.LittleEndian.PutUint64(buf[8:], uint64(pos))
//
//	return bm.db.Update(func(txn *badger.Txn) error {
//		if err := txn.Set([]byte(key), buf); err != nil {
//			return err
//		}
//		return nil
//	})
//}

//func (bm *badgerMeta) GetCheckPoint(key string) (int64, int64, error) {
//	var value []byte
//	err := bm.db.View(func(txn *badger.Txn) error {
//		var e error
//		if value, e = getRawValue([]byte(key), txn); e != nil {
//			return e
//		}
//		return nil
//	})
//
//	if err == badger.ErrKeyNotFound {
//		return -1, -1, nil
//	}
//	if err != nil {
//		return 0, 0, err
//	}
//
//	fileId := int64(binary.LittleEndian.Uint64(value))
//	pos := int64(binary.LittleEndian.Uint64(value[8:]))
//
//	return fileId, pos, nil
//}

func (bm *badgerMeta) SetInstanceRole(role store.InstanceRoleEnum) error {
	return bm.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte(roleKey), role.AsBytes()); err != nil {
			return err
		}
		return nil
	})

}
func (bm *badgerMeta) GetInstanceRole() (store.InstanceRoleEnum, error) {
	var value []byte
	err := bm.db.View(func(txn *badger.Txn) error {
		item, e := txn.Get([]byte(roleKey))
		if e != nil {
			return e
		}
		if value, e = item.ValueCopy(nil); e != nil {
			return e
		}
		return nil
	})

	if errors.Is(err, badger.ErrKeyNotFound) {
		return store.Unset, nil
	}
	if err != nil {
		return store.Master, err
	}
	return store.InstanceRoleEnum(value[0]), nil
}

func (bm *badgerMeta) Close() error {
	if !bm.db.IsClosed() {
		err := bm.db.Close()
		return err
	}
	return nil
}

func (bm *badgerMeta) existMq(normalMqName []byte) (bool, error) {
	exist := false
	err := bm.db.View(func(txn *badger.Txn) error {
		var e error
		if exist, e = existKey(normalMqName, txn); e != nil {
			return e
		}
		return nil
	})

	return exist, err
}

func existKey(key []byte, txn *badger.Txn) (bool, error) {
	_, err := txn.Get(key)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}

	return false, err
}

func getRawValue(key []byte, txn *badger.Txn) ([]byte, error) {
	var ret []byte
	item, err := txn.Get(key)
	if err != nil {
		return nil, err
	}
	if err = item.Value(func(val []byte) error {
		ret = make([]byte, len(val))
		copy(ret, val)
		return nil
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

func normalMqName(mqName string) []byte {
	pl := len(normalPrefix)
	buf := make([]byte, pl+len(mqName))
	copy(buf, normalPrefix)
	copy(buf[pl:], mqName)
	return buf
}
