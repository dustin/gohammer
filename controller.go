package main

import (
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dustin/gomemcached"
)

const numCommands = 3
const readySize = 1024

var stats [256]int64

type Command struct {
	Cmd     gomemcached.CommandCode
	Key     string
	VBucket uint16
}

type Result struct {
	Error error
	Res   *gomemcached.MCResponse
}

func NewController(numKeys int) chan<- Result {
	responses := make(chan Result)

	go handleResults(responses)
	return responses
}

func report(tdiff int64) {
	var total float32 = 0
	trailer := []string{}
	for i, v := range stats {
		total += float32(v)
		if v > 0 {
			trailer = append(trailer,
				fmt.Sprintf("%s: %v",
					gomemcached.CommandCode(i), v))
			atomic.AddInt64(&stats[i], 0-v)
		}
	}
	log.Printf("%.2f ops/s (%s)",
		total/float32(tdiff),
		strings.Join(trailer, ", "))

}

func handleResults(ch <-chan Result) {
	reportSignaler := time.NewTicker(time.Second * 5)
	prev := time.Now()
	for {
		select {
		// Do we need to report?
		case now := <-reportSignaler.C:
			report((now.Unix() - prev.Unix()))
			prev = now

			// Do we have a result?
		case result := <-ch:
			if result.Error != nil {
				log.Printf("Got an error:  %v", result.Error)
			} else {
				if result.Res.Status != 0 {
					log.Printf("Response from %s: %s",
						result.Res.Opcode,
						result.Res.Status)
				}
			}
		}
	}
}
