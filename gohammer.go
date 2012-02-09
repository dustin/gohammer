package main

import (
	"flag"
	"github.com/dustin/gomemcached"
	"log"
	"runtime"
)

func fail(cmd Command) {
	log.Printf("Unhandled command:  %d", cmd.Cmd)
	runtime.Goexit()
}

func doStuff(src <-chan Command,
	res chan<- Result,
	death chan<- bool,
	body []byte,
	client *MemcachedClient) {

	defer func() { death <- true }()

	r := func(response gomemcached.MCResponse, c Command) (rv Result) {

		rv.Cmd = c
		rv.Res = response
		return
	}

	flags := 19

	for {
		var cmd Command
		cmd = <-src
		switch cmd.Cmd {
		default:
			fail(cmd)
		case GET:
			res <- r(Get(client, cmd.Key), cmd)
		case ADD:
			res <- r(Add(client, cmd.Key, flags, 0, body), cmd)
		case DEL:
			res <- r(Del(client, cmd.Key), cmd)
		}
	}
}

var prot = flag.String("prot", "tcp", "Layer 3 protocol (tcp, tcp4, tcp6)")
var dest = flag.String("dest", "localhost:11211", "Host:port to connect to")
var concurrency = flag.Int("concurrency", 32, "Number of concurrent clients")
var nkeys = flag.Int("keys", 1000000, "Number of keys")
var bodylen = flag.Int("bodylen", 20, "Number of bytes of value")

func main() {
	flag.Parse()
	log.Printf("Connecting %d clients to %s/%s", *concurrency, *prot, *dest)
	src, results := NewController(*nkeys)
	death := make(chan bool)

	// Initialize the value
	body := make([]byte, *bodylen)
	for i := 0; i < *bodylen; i++ {
		body[i] = 'x'
	}

	// Start them all
	for i := 0; i < *concurrency; i++ {
		go doStuff(src, results, death, body, Connect(*prot, *dest))
	}

	// Wait for them all to die
	for i := 0; i < *concurrency; i++ {
		<-death
	}
}
