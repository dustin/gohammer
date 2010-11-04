package controller

import "log"
import "fmt"
import "rand"
import "time"
import "./mc_constants"

const numCommands = 3
const readySize = 100

const (
	GET = iota
	ADD
	DEL
	)

type Command struct {
	Cmd uint8
	Key string
}

type Result struct {
	Cmd Command
	Res mc_constants.MCResponse
}

func toString(id uint8) (string) {
	switch id {
	case GET: return "GET"
	case ADD: return "ADD"
	case DEL: return "DEL"
	}
	panic("unhandled")
}

func New(numKeys int) (<-chan Command, chan<- Result) {
	ready := make(chan Command, readySize)
	responses := make(chan Result)
	keys := make([]string, numKeys)
	states := make([]bool, numKeys)

	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("k%d", i)
		states[i] = false
	}

	go handleResponses(responses)
	go createCommands(ready, keys, states)
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
	for _,v := range m {
		total += float32(v)
	}
	log.Printf("%.2f ops/s", total / float32(tdiff))
	resetCounters(m)
}

func handleResponses(ch <-chan Result) {
	cmds := make(map[uint8]int)
	statNotifier := make(chan bool)
	go reportSignaler(statNotifier)
	resetCounters(cmds)
	prev := time.Seconds()
	for {
		select {
			// Do we need to report?
		case <- statNotifier:
			now := time.Seconds()
			report(cmds, (now - prev))
			prev = now

			// Do we have a result?
		case result := <- ch:
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

func createCommands(ch chan<- Command, keys []string, states []bool) {
	for {
		ids := rand.Perm(len(keys))
		for i := 0; i < len(keys); i++ {
			thisId := ids[i]
			var cmd Command
			if states[thisId] {
				cmd.Cmd = uint8(DEL)
				states[thisId] = false
			} else {
				cmd.Cmd = uint8(ADD)
				states[thisId] = true
			}
			cmd.Key = keys[thisId]
			ch <- cmd
		}
	}
}