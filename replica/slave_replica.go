package replica

import (
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"log"
)

func SlaveReplica(masterHost string, masterPort int, seqId int64, worker standard.MessageWorking, scanner store.Scanner) error {
	sc, err := newSlaveReplicaClient(masterHost, masterPort, &dependWorker{
		MessageWorking: worker,
		Scanner:        scanner,
	})
	if err != nil {
		return err
	}
	go run(sc, seqId)
	return nil
}

func run(sc *slaveClient, seqId int64) {
	defer sc.Close()
	err := sc.replica(seqId)
	log.Printf("slave run err:%v\n", err)
}

type dependWorker struct {
	standard.MessageWorking
	store.Scanner
}
