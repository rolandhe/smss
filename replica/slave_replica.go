package replica

import (
	"github.com/rolandhe/smss/pkg/dir"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"time"
)

func SlaveReplica(masterHost string, masterPort int, seqId int64, needSync bool, worker standard.MessageWorking, fstore store.Store) error {
	sc, err := newSlaveReplicaClient(masterHost, masterPort, &dependWorker{
		MessageWorking: worker,
		ManagerMeta:    fstore.GetManagerMeta(),
	})
	if err != nil {
		return err
	}
	if needSync {
		err = syncMqInfo(sc, seqId, fstore)
		if err != nil {
			return err
		}
	}

	go func() {
		client := sc
		newSeqId := seqId
		for {
			newSeqId = run(client, newSeqId)
			time.Sleep(time.Millisecond * 2000)
			for {
				client, err = newSlaveReplicaClient(masterHost, masterPort, &dependWorker{
					MessageWorking: worker,
					ManagerMeta:    fstore.GetManagerMeta(),
				})
				if err == nil {
					break
				}
				logger.Get().Infof("new sc err:%v", err)
				time.Sleep(time.Millisecond * 5000)
			}

		}
	}()
	return nil
}

func syncMqInfo(sc *slaveClient, seqId int64, fstore store.Store) error {
	infos, err := sc.getValidMq(seqId)
	if err != nil {
		return err
	}
	for _, info := range infos {
		if err = fstore.GetManagerMeta().CopyCreateTopic(info); err != nil {
			return err
		}
		p := fstore.GetTopicPath(info.Name)
		err = dir.EnsurePathExist(p)
		if err != nil {
			fstore.GetManagerMeta().DeleteTopic(info.Name, true)
		}
	}
	return nil
}

func run(sc *slaveClient, seqId int64) int64 {
	defer sc.Close()
	err := sc.replica(seqId)
	logger.Get().Infof("slave,last eventId=%d, run err:%v", sc.lastEventId, err)
	return sc.lastEventId
}

type dependWorker struct {
	standard.MessageWorking
	store.ManagerMeta
}
