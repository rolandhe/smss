package fss

import (
	"errors"
	"github.com/rolandhe/smss/pkg"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"github.com/rolandhe/smss/store/badger_meta"
	"log"
	"path"
	"sync"
	"time"
)

func NewMeta(root string) (store.Meta, error) {
	metaRoot := path.Join(root, store.MetaDir)
	return badger_meta.NewMeta(metaRoot)
}

func NewFileStore(root string, writerBuffSize int, meta store.Meta) (store.Store, error) {

	fsStoreRoot := path.Join(root, store.DataDir)
	if err := ensureStoreDirectory(fsStoreRoot); err != nil {
		meta.Close()
		return nil, err
	}
	mqList, err := meta.GetMQSimpleInfoList()
	if err != nil {
		meta.Close()
		return nil, err
	}
	if err = ensureMqDirectory(fsStoreRoot, mqList); err != nil {
		meta.Close()
		return nil, err
	}

	fstore := &fileStore{
		root: fsStoreRoot,
		meta: meta,
		c:    make(chan *asyncMsg, writerBuffSize),
		writerMap: &safeMap{
			wmap: map[string]*mqWriter{},
		},
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		doWriter(fstore)
	}()
	wg.Wait()

	return fstore, nil
}

func doWriter(fstore *fileStore) {
	c := fstore.c
	for {
		var msg *asyncMsg
		select {
		case msg = <-c:
		case <-time.After(time.Millisecond * 1000):
			continue
		}
		writer, err := fstore.ensureWriter(msg.mqName)
		if err != nil {
			msg.err = err
		} else if writer.IsInvalid() {
			msg.err = errors.New("mq not exist")
		} else {
			writer.WaitGroup.Add(1)
			msg.err = writer.Write(msg, nil)
			writer.WaitGroup.Done()
		}
		close(msg.notify)
	}
}

type asyncMsg struct {
	messages []*store.MQMessage
	saveTime int64
	mqName   string
	notify   chan struct{}
	err      error
}

type safeMap struct {
	sync.Mutex
	wmap map[string]*mqWriter
}

func (sm *safeMap) getWriterOrCreate(mqName string, createFnc func() (*mqWriter, error)) (*mqWriter, error) {
	sm.Lock()
	defer sm.Unlock()
	var err error
	w := sm.wmap[mqName]
	if w == nil {
		if createFnc != nil {
			if w, err = createFnc(); err != nil {
				return nil, err
			}
			sm.wmap[mqName] = w
		}
	}
	return w, nil
}

func (sm *safeMap) delete(mqName string) {
	sm.Lock()
	defer sm.Unlock()
	delete(sm.wmap, mqName)
}

func (sm *safeMap) invalidWriter(mqName string) *sync.WaitGroup {
	sm.Lock()
	defer sm.Unlock()

	writer := sm.wmap[mqName]
	if writer == nil {
		return nil
	}
	writer.Terminate()
	return &writer.WaitGroup
}

func (sm *safeMap) removeWriter(mqName string) {
	sm.Lock()
	defer sm.Unlock()

	writer := sm.wmap[mqName]
	if writer == nil {
		return
	}
	writer.Close()
	delete(sm.wmap, mqName)
}

type fileStore struct {
	root      string
	meta      store.Meta
	c         chan *asyncMsg
	writerMap *safeMap
}

func (fs *fileStore) ensureWriter(mqName string) (*mqWriter, error) {
	return fs.writerMap.getWriterOrCreate(mqName, func() (*mqWriter, error) {
		info, err := fs.GetMqInfoReader().GetMQInfo(mqName)
		if err != nil {
			return nil, err
		}
		if info == nil || info.IsInvalid() {
			return nil, errors.New("mq not exist")
		}
		writer := newWriter(mqName, MqPath(fs.root, mqName))
		return writer, nil
	})
}

func (fs *fileStore) CreateMq(mqName string, life int64, eventId int64) error {
	info, err := fs.meta.CreateMQ(mqName, life, eventId)
	if err != nil {
		return err
	}
	log.Printf("%v\n", info)
	p := MqPath(fs.root, mqName)
	err = pkg.EnsurePathExist(p)
	if err != nil {
		fs.meta.DeleteMQ(mqName, true)
	}
	return err
}

func (fs *fileStore) ForceDeleteMQ(mqName string, cb func() error) error {
	exist, err := fs.meta.DeleteMQ(mqName, false)
	if err != nil {
		return err
	}
	if !exist {
		return nil
	}
	wg := fs.writerMap.invalidWriter(mqName)
	if wg != nil {
		wg.Wait()
		fs.writerMap.removeWriter(mqName)
	}

	if err = cb(); err != nil {
		return err
	}
	_, err = fs.meta.DeleteMQ(mqName, true)
	return err
}

func (fs *fileStore) ChangeMqLife(mqName string, life int64, eventId int64) error {
	return fs.meta.ChangeMQLife(mqName, life, eventId)
}

func (fs *fileStore) GetManagerMeta() store.ManagerMeta {
	return fs.meta
}

func (fs *fileStore) GetMqInfoReader() store.MqInfoReader {
	return fs.meta
}

func (fs *fileStore) GetScanner() store.Scanner {
	return fs.meta
}

func (fs *fileStore) GetMqPath(mqName string) string {
	return MqPath(fs.root, mqName)
}

func (fs *fileStore) Save(mqName string, messages []*store.MQMessage) error {
	wrapMsg := &asyncMsg{
		messages: messages,
		saveTime: time.Now().UnixMilli(),
		notify:   make(chan struct{}),
		mqName:   mqName,
	}
	fs.c <- wrapMsg
	<-wrapMsg.notify
	return wrapMsg.err
}

func (fs *fileStore) SaveDelayMsg(mqName string, payload []byte) error {
	return fs.meta.SaveDelay(mqName, payload)
}

func (fs *fileStore) Close() error {
	return fs.meta.Close()
}

func (fs *fileStore) GetReader(mqName, whoami string, fileId, pos int64, batchSize int) (store.MqBlockReader, error) {
	info, err := fs.meta.GetMQInfo(mqName)
	if err != nil {
		return nil, err
	}
	if info == nil || info.IsInvalid() {
		return nil, errors.New(mqName + " not exist")
	}
	dataRoot := MqPath(fs.root, mqName)
	reader := newBlockReader(dataRoot, whoami, mqName, batchSize, &MqNotifyRegister{
		fs:     fs,
		mqName: mqName,
		whoami: whoami,
	})
	if err = reader.Init(fileId, pos); err != nil {
		return nil, err
	}
	return reader, nil
}

// registerReaderNotify 消息读取端注册新消息写入回调
func (fs *fileStore) registerReaderNotify(mqName, whoami string, notify *standard.NotifyDevice) (standard.LogFileInfoGet, error) {
	writer, err := fs.ensureWriter(mqName)
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

func (fs *fileStore) unRegisterReaderNotify(mqName, whoami string) {
	writer, _ := fs.writerMap.getWriterOrCreate(mqName, nil)
	if writer == nil {
		return
	}

	writer.UnRegNotify(whoami)
	writer.WaitGroup.Done()
}
