package main

import (
	"log"
	"flag"
	"runtime"
	"./mc"
	"./controller"
	"./mc_constants"
	)

func fail(cmd controller.Command) {
	log.Printf("Unhandled command:  %d", cmd.Cmd)
	runtime.Goexit()
}

func doStuff(src <-chan controller.Command,
	res chan<- controller.Result,
	death chan<- bool,
	client mc.MemcachedClient) {

	defer func () { death <- true }()

	r := func(response mc_constants.MCResponse,
		c controller.Command) (rv controller.Result) {

		rv.Cmd = c
		rv.Res = response
		return
	}

	for {
		var cmd controller.Command
		cmd = <- src
		switch cmd.Cmd {
		default: fail(cmd)
		case controller.GET: res <- r(mc.Get(client, cmd.Key), cmd)
		case controller.ADD: res <- r(mc.Add(client, cmd.Key, "hi"), cmd)
		case controller.DEL: res <- r(mc.Del(client, cmd.Key), cmd)
		}
	}
}

var prot = flag.String("prot", "tcp", "Layer 3 protocol (tcp, tcp4, tcp6)")
var dest = flag.String("dest", "localhost:11211", "Host:port to connect to")
var concurrency = flag.Int("concurrency", 32, "Number of concurrent clients")

func main() {
	flag.Parse()
        log.Printf("Connecting %d clients to %s/%s", *concurrency, *prot, *dest)
	src, results := controller.New()
	death := make(chan bool)
        for i := 0; i < *concurrency; i++ {
		go doStuff(src, results, death, mc.Connect(*prot, *dest))
	}
        for i := 0; i < *concurrency; i++ {
		<- death
	}
}
