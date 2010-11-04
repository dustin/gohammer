package controller

import "log"
import "fmt"
import "rand"
import "./mc_constants"

const numCommands = 3
const readySize = 100
const numKeys = 1000000

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

func New() (<-chan Command, chan<- Result) {
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

func handleResponses(ch <-chan Result) {
	i := 0
	for {
		result := <- ch
		i++
		if result.Res.Status != 0 {
			log.Printf("Response from %s (%s): %d",
				toString(result.Cmd.Cmd),
				result.Cmd.Key,
				result.Res.Status)
		}
		if i % 10000 == 0 {
			log.Printf("Sent %d commands", i)
		}
	}
}

func createCommands(ch chan<- Command, keys []string, states []bool) {
	for {
		ids := rand.Perm(numKeys)
		for i := 0; i < numKeys; i++ {
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