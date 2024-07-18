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

func (bm *badgerMeta) CreateTopic(topicName string, expireAt int64, eventId int64) (*store.TopicInfo, error) {
	norTopicName := normalTopicName(topicName)
	exist, err := bm.existTopic(norTopicName)
	if err != nil {
		return nil, err
	}
	if exist {
		return nil, errors.New("topic exists")
	}

	info := &store.TopicInfo{
		Name:            topicName,
		CreateTimeStamp: time.Now().UnixMilli(),
		ExpireAt:        expireAt,
	}
	valMeta := topicMetaValue{
		createTime:         info.CreateTimeStamp,
		expireAtTime:       expireAt,
		createEventId:      eventId,
		stateChangeTime:    info.CreateTimeStamp,
		stateChangeEventId: eventId,
		state:              store.TopicStateNormal,
	}
	err = bm.db.Update(func(txn *badger.Txn) error {
		if e := txn.Set(norTopicName, valMeta.toBytes()); e != nil {
			return e
		}
		if info.IsTemp() {
			if e := txn.Set(topicLifetimeName(topicName, expireAt), lifeValueHolder); e != nil {
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

func (bm *badgerMeta) CopyCreateTopic(info *store.TopicInfo) error {
	norTopicName := normalTopicName(info.Name)
	exist, err := bm.existTopic(norTopicName)
	if err != nil {
		return err
	}
	if exist {
		return nil
	}

	var valMeta topicMetaValue

	valMeta.fromTopicInfo(info)

	err = bm.db.Update(func(txn *badger.Txn) error {
		if e := txn.Set(norTopicName, valMeta.toBytes()); e != nil {
			return e
		}
		if info.IsTemp() {
			if e := txn.Set(topicLifetimeName(info.Name, info.ExpireAt), lifeValueHolder); e != nil {
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

func (bm *badgerMeta) DeleteTopic(topicName string, force bool) (bool, error) {
	exist := true
	var valMeta topicMetaValue
	err := bm.db.Update(func(txn *badger.Txn) error {
		k := normalTopicName(topicName)
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
			if err = txn.Delete(topicLifetimeName(topicName, valMeta.expireAtTime)); err != nil {
				if !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
			}
		}
		if !force {
			valMeta.state = store.TopicStateDeleted
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

func (bm *badgerMeta) ScanExpireTopics() ([]string, int64, error) {
	now := time.Now().UnixMilli()
	var next int64
	var topicList []string

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
			topicList = append(topicList, string(key[8:]))
		}

		return nil
	})
	logger.Get().Infof("ScanExpireTopics,next scan time from db:%d", next)
	return topicList, next, err
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
				Key:       key,
				Payload:   valueBuf,
				TopicName: string(buf[16:]),
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
func (bm *badgerMeta) RemoveDelayByName(data []byte, topicName string) error {
	preLen := len(delayPrefix)
	key := make([]byte, preLen+16+len(topicName))
	copy(key, delayPrefix)

	buf := key[preLen:]
	// copy 时间戳
	copy(buf, data)
	buf = buf[16:]
	copy(buf, topicName)
	return bm.RemoveDelay(key)
}

func (bm *badgerMeta) GetTopicInfo(topicName string) (*store.TopicInfo, error) {
	var valMeta topicMetaValue

	err := bm.db.View(func(txn *badger.Txn) error {
		var e error
		var rawValue []byte
		if rawValue, e = getRawValue(normalTopicName(topicName), txn); e != nil {
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
	return valMeta.toTopicInfo(topicName), nil
}

func (bm *badgerMeta) GetTopicSimpleInfoList() ([]*store.TopicInfo, error) {
	var infoList []*store.TopicInfo
	var valMeta topicMetaValue
	err := bm.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = normalPrefix

		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(opts.Prefix); it.ValidForPrefix(opts.Prefix); it.Next() {
			item := it.Item()
			topicName := string(item.Key()[len(normalPrefix):])

			if e := item.Value(func(val []byte) error {
				valMeta.fromBytes(val)
				return nil
			}); e != nil {
				return e
			}
			infoList = append(infoList, valMeta.toTopicInfo(topicName))
		}
		return nil
	})

	return infoList, err
}

func (bm *badgerMeta) SaveDelay(topicName string, payload []byte) error {
	preLen := len(delayPrefix)
	key := make([]byte, preLen+16+len(topicName))
	copy(key, delayPrefix)

	buf := key[preLen:]
	// copy 时间戳+eventId
	copy(buf[:16], payload)
	copy(buf[16:], topicName)

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

func (bm *badgerMeta) existTopic(normalTopicName []byte) (bool, error) {
	exist := false
	err := bm.db.View(func(txn *badger.Txn) error {
		var e error
		if exist, e = existKey(normalTopicName, txn); e != nil {
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

func normalTopicName(topicName string) []byte {
	pl := len(normalPrefix)
	buf := make([]byte, pl+len(topicName))
	copy(buf, normalPrefix)
	copy(buf[pl:], topicName)
	return buf
}
