package fss

import (
	"errors"
	"github.com/rolandhe/smss/pkg/dir"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"github.com/rolandhe/smss/store/badger_meta"
	"path"
	"sync"
	"time"
)

func NewMeta(root string) (store.Meta, error) {
	metaRoot := path.Join(root, store.MetaDir)
	return badger_meta.NewMeta(metaRoot)
}

func NewFileStore(root string, meta store.Meta) (store.Store, error) {
	fsStoreRoot := path.Join(root, store.TopicDir)
	if err := ensureStoreDirectory(fsStoreRoot); err != nil {
		meta.Close()
		return nil, err
	}
	topicList, err := meta.GetTopicSimpleInfoList()
	if err != nil {
		meta.Close()
		return nil, err
	}
	if err = ensureTopicDirectory(fsStoreRoot, topicList); err != nil {
		meta.Close()
		return nil, err
	}

	fstore := &fileStore{
		root: fsStoreRoot,
		meta: meta,
		writerMap: &safeMap{
			wmap: map[string]*topicWriter{},
		},
	}

	return fstore, nil
}

type wrappedMsges struct {
	messages []*store.TopicMessage
	saveTime int64
}

type safeMap struct {
	sync.Mutex
	wmap map[string]*topicWriter
}

func (sm *safeMap) getWriterOrCreate(topicName string, createFnc func() (*topicWriter, error)) (*topicWriter, error) {
	sm.Lock()
	defer sm.Unlock()
	var err error
	w := sm.wmap[topicName]
	if w == nil {
		if createFnc != nil {
			if w, err = createFnc(); err != nil {
				return nil, err
			}
			sm.wmap[topicName] = w
		}
	}
	return w, nil
}

func (sm *safeMap) delete(topicName string) {
	sm.Lock()
	defer sm.Unlock()
	delete(sm.wmap, topicName)
}

func (sm *safeMap) invalidWriter(topicName string) *sync.WaitGroup {
	sm.Lock()
	defer sm.Unlock()

	writer := sm.wmap[topicName]
	if writer == nil {
		return nil
	}
	writer.Terminate()
	return &writer.WaitGroup
}

func (sm *safeMap) removeWriter(topicName string) {
	sm.Lock()
	defer sm.Unlock()

	writer := sm.wmap[topicName]
	if writer == nil {
		return
	}
	writer.Close()
	delete(sm.wmap, topicName)
}

type fileStore struct {
	root      string
	meta      store.Meta
	writerMap *safeMap
}

func (fs *fileStore) ensureWriter(topicName string) (*topicWriter, error) {
	return fs.writerMap.getWriterOrCreate(topicName, func() (*topicWriter, error) {
		info, err := fs.GetTopicInfoReader().GetTopicInfo(topicName)
		if err != nil {
			return nil, err
		}
		if info == nil || info.IsInvalid() {
			return nil, errors.New("topic not exist")
		}
		writer := newWriter(topicName, TopicPath(fs.root, topicName))
		return writer, nil
	})
}

func (fs *fileStore) CreateTopic(topicName string, life int64, eventId int64) error {
	info, err := fs.meta.CreateTopic(topicName, life, eventId)
	if err != nil {
		return err
	}
	logger.Get().Infof("%+v", info)
	p := TopicPath(fs.root, topicName)
	err = dir.EnsurePathExist(p)
	if err != nil {
		fs.meta.DeleteTopic(topicName, true)
	}
	return err
}

func (fs *fileStore) ForceDeleteTopic(topicName string, cb func() error) error {
	exist, err := fs.meta.DeleteTopic(topicName, false)
	if err != nil {
		return err
	}
	if !exist {
		return nil
	}
	wg := fs.writerMap.invalidWriter(topicName)
	if wg != nil {
		wg.Wait()
		fs.writerMap.removeWriter(topicName)
	}

	if err = cb(); err != nil {
		return err
	}
	_, err = fs.meta.DeleteTopic(topicName, true)
	return err
}

func (fs *fileStore) GetManagerMeta() store.ManagerMeta {
	return fs.meta
}

func (fs *fileStore) GetTopicInfoReader() store.TopicInfoReader {
	return fs.meta
}

func (fs *fileStore) GetScanner() store.Scanner {
	return fs.meta
}

func (fs *fileStore) GetTopicPath(topicName string) string {
	return TopicPath(fs.root, topicName)
}

func (fs *fileStore) Save(topicName string, messages []*store.TopicMessage) (int, error) {
	wrapMsg := &wrappedMsges{
		messages: messages,
		saveTime: time.Now().UnixMilli(),
	}
	writer, err := fs.ensureWriter(topicName)
	if err != nil {
		return standard.SyncFdIgnore, err
	}
	if writer.IsInvalid() {
		return standard.SyncFdIgnore, errors.New("topic not exist")
	}

	writer.WaitGroup.Add(1)
	defer writer.WaitGroup.Done()
	syncFd, _, err := writer.Write(wrapMsg, nil)
	return syncFd, err
}

func (fs *fileStore) SaveDelayMsg(topicName string, payload []byte) error {
	return fs.meta.SaveDelay(topicName, payload)
}

func (fs *fileStore) Close() error {
	return fs.meta.Close()
}

func (fs *fileStore) GetReader(topicName, whoami string, fileId, pos int64, batchSize int) (store.TopicBlockReader, error) {
	info, err := fs.meta.GetTopicInfo(topicName)
	if err != nil {
		return nil, err
	}
	if info == nil || info.IsInvalid() {
		return nil, errors.New(topicName + " not exist")
	}
	dataRoot := TopicPath(fs.root, topicName)
	reader := newBlockReader(dataRoot, whoami, topicName, batchSize, &TopicNotifyRegister{
		fs:        fs,
		topicName: topicName,
		whoami:    whoami,
	})
	if err = reader.Init(fileId, pos); err != nil {
		return nil, err
	}
	return reader, nil
}

// registerReaderNotify 消息读取端注册新消息写入回调
func (fs *fileStore) registerReaderNotify(topicName, whoami string, notify *standard.NotifyDevice) (standard.LogFileInfoGet, error) {
	writer, err := fs.ensureWriter(topicName)
	if err != nil {
		return nil, err
	}
	infoGet, err := writer.RegNotify(whoami, notify)
	if err != nil {
		return nil, err
	}
	writer.WaitGroup.Add(1)
	return infoGet, nil
}

func (fs *fileStore) unRegisterReaderNotify(topicName, whoami string) {
	writer, _ := fs.writerMap.getWriterOrCreate(topicName, nil)
	if writer == nil {
		return
	}

	writer.UnRegNotify(whoami)
	writer.WaitGroup.Done()
}
