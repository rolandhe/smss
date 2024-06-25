package main

import (
	"github.com/rolandhe/smss/cmd"
	"github.com/rolandhe/smss/conf"
)

func main() {
	if err := conf.Init(); err != nil {
		return
	}
	cmd.StartServer(conf.MainStorePath)
}
