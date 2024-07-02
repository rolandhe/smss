package replica

import (
	"github.com/rolandhe/smss/pkg/dir"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"log"
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
			client, err = newSlaveReplicaClient(masterHost, masterPort, &dependWorker{
				MessageWorking: worker,
				ManagerMeta:    fstore.GetManagerMeta(),
			})
			if err != nil {
				log.Printf("new sc err:%v\n", err)
				time.Sleep(time.Millisecond * 5000)
				continue
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
		if err = fstore.GetManagerMeta().CopyCreateMq(info); err != nil {
			return err
		}
		p := fstore.GetMqPath(info.Name)
		err = dir.EnsurePathExist(p)
		if err != nil {
			fstore.GetManagerMeta().DeleteMQ(info.Name, true)
		}
	}
	return nil
}

func run(sc *slaveClient, seqId int64) int64 {
	defer sc.Close()
	err := sc.replica(seqId)
	log.Printf("slave,last eventId=%d, run err:%v\n", sc.lastEventId, err)
	return sc.lastEventId
}

type dependWorker struct {
	standard.MessageWorking
	store.ManagerMeta
}
