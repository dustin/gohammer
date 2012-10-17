package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
)

// Modified atomically.
var isDone int32

func handleResponses(client *memcached.Client, ch chan<- Result) {
	for {
		res, err := client.Receive()
		ch <- Result{
			Error: err,
			Res:   res,
		}
	}
}

func sendCommand(client *memcached.Client,
	opcode gomemcached.CommandCode,
	key []byte,
	body []byte) {

	flags := 19
	exp := 0

	var err error
	switch opcode {
	default:
		log.Fatalf("Unhandled opcode: %v", opcode)
	case gomemcached.GETQ, gomemcached.DELETEQ:
		err = client.Transmit(&gomemcached.MCRequest{
			Opcode:  opcode,
			VBucket: 0,
			Key:     key,
			Cas:     0,
			Opaque:  0,
			Extras:  []byte{},
			Body:    []byte{}})
	case gomemcached.ADDQ:
		req := &gomemcached.MCRequest{
			Opcode:  opcode,
			VBucket: 0,
			Key:     key,
			Cas:     0,
			Opaque:  0,
			Extras:  []byte{0, 0, 0, 0, 0, 0, 0, 0},
			Body:    body}
		binary.BigEndian.PutUint64(req.Extras, uint64(flags)<<32|uint64(exp))
		err = client.Transmit(req)
	}
	if err != nil {
		log.Fatalf("Error transmitting request: %v", err)
	}
}

func doStuff(id int,
	res chan<- Result,
	wg *sync.WaitGroup,
	body []byte,
	prot, addr string) {

	defer wg.Done()

	localstats := [256]int64{}

	keys := make([][]byte, *nkeys / *concurrency)
	for i := 0; i < len(keys); i++ {
		keys[i] = []byte(fmt.Sprintf("c%d.k%d", id, i))
	}

	client, err := memcached.Connect(prot, addr)
	if err != nil {
		log.Printf("Error connecting to %v/%v: %v", prot, addr, err)
		return
	}

	go handleResponses(client, res)

	applyLocalStats := func() {
		for j, v := range localstats {
			atomic.AddInt64(&stats[j], v)
			localstats[j] = 0
		}
	}

	cmds := []gomemcached.CommandCode{
		gomemcached.ADDQ,
		gomemcached.GETQ,
		gomemcached.DELETEQ,
	}
	cmdi := 0
	ids := rand.Perm(len(keys))
	for {
		for i, thisId := range ids {
			key := keys[thisId]
			opcode := cmds[cmdi]

			sendCommand(client, opcode, key, body)
			localstats[opcode]++

			if i%1000 == 0 {
				applyLocalStats()
				if atomic.LoadInt32(&isDone) == 1 {
					return
				}
			}
		}
		applyLocalStats()
		cmdi++
		if cmdi >= len(cmds) {
			cmdi = 0
			ids = rand.Perm(len(keys))
		}
	}
}

var prot = flag.String("prot", "tcp", "Layer 3 protocol (tcp, tcp4, tcp6)")
var dest = flag.String("dest", "localhost:11211", "Host:port to connect to")
var concurrency = flag.Int("concurrency", 32, "Number of concurrent clients")
var nkeys = flag.Int("keys", 1000000, "Number of keys")
var bodylen = flag.Int("bodylen", 20, "Number of bytes of value")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var testDuration = flag.Duration("duration", 0,
	"Total duration of test (0 == forever)")

func main() {
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	log.Printf("Connecting %d clients to %s/%s", *concurrency, *prot, *dest)
	results := NewController(*nkeys)
	wg := sync.WaitGroup{}

	// Initialize the value
	body := make([]byte, *bodylen)
	for i := 0; i < *bodylen; i++ {
		body[i] = 'x'
	}

	// Start them all
	wg.Add(*concurrency)
	for i := 0; i < *concurrency; i++ {
		go doStuff(i, results, &wg, body, *prot, *dest)
	}

	if *testDuration != 0 {
		time.AfterFunc(*testDuration, func() {
			log.Printf("Marking done.")
			atomic.StoreInt32(&isDone, 1)
		})
	}

	// Wait for them all to die
	wg.Wait()
}
