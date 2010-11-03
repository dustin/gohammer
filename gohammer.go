package main

import (
	"log"
	"flag"
	"runtime"
	"./mc"
	"./controller"
	)

func doGet(client mc.MemcachedClient, cmd controller.Command) {
	rv := mc.Get(client, cmd.Key)
	log.Printf("Get response:  %d (%d bytes)", rv.Status, len(rv.Body))
}

func doAdd(client mc.MemcachedClient, cmd controller.Command) {
	rv := mc.Add(client, cmd.Key, "hi")
	log.Printf("Add response:  %d", rv.Status)
}

func doDel(client mc.MemcachedClient, cmd controller.Command) {
	rv := mc.Del(client, cmd.Key)
	log.Printf("Del response:  %d", rv.Status)
}

func fail(cmd controller.Command) {
	log.Printf("Unhandled command:  %d", cmd.Cmd)
	runtime.Goexit()
}

func die(death chan<- bool) {
	death <- true
}

func doStuff(src <-chan controller.Command,
	death chan<- bool,
	client mc.MemcachedClient) {

	defer die(death)

	log.Printf("Doing stuff.")
	for {
		var cmd controller.Command
		cmd = <- src
		switch cmd.Cmd {
		default: fail(cmd)
		case 0: doGet(client, cmd)
		case 1: doAdd(client, cmd)
		case 2: doDel(client, cmd)
		}
	}
}

var prot = flag.String("prot", "tcp", "Layer 3 protocol (tcp, tcp4, tcp6)")
var dest = flag.String("dest", "localhost:11211", "Host:port to connect to")
var concurrency = flag.Int("concurrency", 32, "Number of concurrent clients")

func main() {
	flag.Parse()
        log.Printf("Connecting %d clients to %s/%s", *concurrency, *prot, *dest)
	c := controller.New()
	death := make(chan bool)
        for i := 0; i < *concurrency; i++ {
		go doStuff(c.Ready, death, mc.Connect(*prot, *dest))
	}
        for i := 0; i < *concurrency; i++ {
		<- death
	}
}
