package controller

import "fmt"

const readySize = 100
const numKeys = 10000

type Command struct {
	Cmd uint8
	Key string
}

type Controller struct {
	// Commands ready for execution
	Ready chan Command
	Keys []string
}

func New() (rv Controller) {
	rv.Ready = make(chan Command, readySize)
	rv.Keys = make([]string, numKeys)

	for i := 0; i < numKeys; i++ {
		rv.Keys[i] = fmt.Sprintf("k%d", i)
	}

	go createCommands(rv)
	return
}

func createCommands(c Controller) {
	i := 0
	ki := 0
	for {
		var cmd Command;
		cmd.Cmd = uint8(i)
		cmd.Key = c.Keys[ki]
		c.Ready <- cmd

		i++
		if i > 2 {
			i = 0
		}
		ki++
		if ki >= numKeys {
			ki = 0
		}
	}
}