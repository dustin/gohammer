package main

import (
	"fmt"
	"github.com/dustin/gomemcached"
	"log"
	"math/rand"
	"time"
)

const numCommands = 3
const readySize = 1024

const (
	GET = iota
	ADD
	DEL
)

type Command struct {
	Cmd     uint8
	Key     string
	VBucket uint16
}

type Result struct {
	Cmd Command
	Res gomemcached.MCResponse
}

func toString(id uint8) string {
	switch id {
	case GET:
		return "GET"
	case ADD:
		return "ADD"
	case DEL:
		return "DEL"
	}
	panic("unhandled")
}

func NewController(numKeys int) (<-chan Command, chan<- Result) {
	ready := make(chan Command, readySize)
	responses := make(chan Result)
	keys := make([]string, numKeys)

	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("k%d", i)
	}

	go handleResponses(responses)
	go createCommands(ready, keys)
	return ready, responses
}

func resetCounters(m map[uint8]int) {
	m[GET] = 0
	m[ADD] = 0
	m[DEL] = 0
}

func reportSignaler(ch chan bool) {
	for {
		time.Sleep(5 * 1000 * 1000 * 1000)
		ch <- true
	}
}

func report(m map[uint8]int, tdiff int64) {
	var total float32 = 0
	for _, v := range m {
		total += float32(v)
	}
	log.Printf("%.2f ops/s (add=%d, get=%d, del=%d)",
		total/float32(tdiff), m[ADD], m[GET], m[DEL])
	resetCounters(m)
}

func handleResponses(ch <-chan Result) {
	cmds := make(map[uint8]int)
	statNotifier := make(chan bool)
	go reportSignaler(statNotifier)
	resetCounters(cmds)
	prev := time.Now()
	for {
		select {
		// Do we need to report?
		case <-statNotifier:
			now := time.Now()
			report(cmds, (now.Unix() - prev.Unix()))
			prev = now

			// Do we have a result?
		case result := <-ch:
			cmds[result.Cmd.Cmd]++
			if result.Res.Status != 0 {
				log.Printf("Response from %s (%s): %d",
					toString(result.Cmd.Cmd),
					result.Cmd.Key,
					result.Res.Status)
			}
		}
	}
}

func createCommands(ch chan<- Command, keys []string) {
	cmds := []uint8{ADD, GET, DEL}
	cmdi := 0
	for {
		ids := rand.Perm(len(keys))
		for i := 0; i < len(keys); i++ {
			thisId := ids[i]
			var cmd Command
			cmd.Key = keys[thisId]
			cmd.Cmd = cmds[cmdi]
			cmd.VBucket = uint16(i % 1024)
			ch <- cmd
		}
		cmdi++
		if cmdi >= len(cmds) {
			cmdi = 0
		}
	}
}
