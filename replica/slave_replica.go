package replica

import (
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"log"
)

func SlaveReplica(masterHost string, masterPort int, seqId int64, needSync bool, worker standard.MessageWorking, manager store.ManagerMeta) error {
	sc, err := newSlaveReplicaClient(masterHost, masterPort, &dependWorker{
		MessageWorking: worker,
		ManagerMeta:    manager,
	})
	if err != nil {
		return err
	}
	if needSync {
		err = syncMqInfo(sc, seqId, manager)
		if err != nil {
			return err
		}
	}

	go run(sc, seqId)
	return nil
}

func syncMqInfo(sc *slaveClient, seqId int64, manager store.ManagerMeta) error {
	infos, err := sc.getValidMq(seqId)
	if err != nil {
		return err
	}
	for _, info := range infos {
		if err = manager.CopyCreateMq(info); err != nil {
			return err
		}
	}
	return nil
}

func run(sc *slaveClient, seqId int64) {
	defer sc.Close()
	err := sc.replica(seqId)
	log.Printf("slave run err:%v\n", err)
}

type dependWorker struct {
	standard.MessageWorking
	store.ManagerMeta
}
