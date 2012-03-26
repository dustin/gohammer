package main

import (
	"flag"
	"log"
	"runtime"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
)

func fail(cmd Command) {
	log.Printf("Unhandled command:  %d", cmd.Cmd)
	runtime.Goexit()
}

func doStuff(src <-chan Command,
	res chan<- Result,
	death chan<- bool,
	body []byte,
	prot, addr string) {

	defer func() { death <- true }()

	client, err := memcached.Connect(prot, addr)
	if err != nil {
		log.Printf("Error connecting to %v/%v: %v", prot, addr, err)
		runtime.Goexit()
	}

	r := func(response gomemcached.MCResponse, e error, c Command) (rv Result) {
		rv.Cmd = c
		rv.Error = err
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
			resp, err := client.Get(0, cmd.Key)
			res <- r(resp, err, cmd)
		case ADD:
			resp, err := client.Add(0, cmd.Key, flags, 0, body)
			res <- r(resp, err, cmd)
		case DEL:
			resp, err := client.Del(0, cmd.Key)
			res <- r(resp, err, cmd)
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
		go doStuff(src, results, death, body, *prot, *dest)
	}

	// Wait for them all to die
	for i := 0; i < *concurrency; i++ {
		<-death
	}
}
