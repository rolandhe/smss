package main

import (
	"flag"
	"github.com/rolandhe/smss/cmd"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/store"
)

var (
	role  = flag.String("role", "master", "实例角色, master or slave")
	host  = flag.String("host", "", "master host")
	port  = flag.Int("port", 8080, "master port")
	seqId = flag.Int64("event-id", 0, "replica event id")
)

func main() {
	if err := conf.Init(); err != nil {
		return
	}

	flag.Parse()

	roleValue := store.Master
	if *role == "slave" {
		roleValue = store.Slave
	}

	cmd.StartServer(conf.MainStorePath, &cmd.InstanceRole{
		Role:     roleValue,
		FromHost: *host,
		FromPort: *port,
		SeqId:    *seqId,
	})
}
