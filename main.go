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
	seqId = flag.Int64("event", 0, "replica event id")
)

// main
// master mode: ./smss -role master
// slave mode: ./smss -role slave -host 127.0.0.1 -port 8080 -event 0
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
