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
	go createCommands(ready, keys)
	return ready, responses
}

func handleResponses(ch <-chan Result) {
	for {
		result := <- ch
		log.Printf("Response from %s (%s): %d",
			toString(result.Cmd.Cmd),
			result.Cmd.Key,
			result.Res.Status)
	}
}

func createCommands(ch chan<- Command, keys []string) {
	for {
		var cmd Command;
		cmd.Cmd = uint8(rand.Int() % numCommands)
		cmd.Key = keys[rand.Int() % numKeys]
		ch <- cmd
	}
}