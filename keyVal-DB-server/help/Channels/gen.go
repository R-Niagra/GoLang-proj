package main

import (
	"fmt"
)

func combineIntChans(a chan int, b chan int) chan int {

	// create channel of the same type
	combined := make(chan int)

	// important to launch a goroutine, otherwise main would be blocked
	// in the for loop forever and return won't happen
	go func() {

		for {
			select {

			// to take data channel a and send it on to the combined channel
			case val := <-a:
				combined <- val

			// to take data channel a and send it on to the combined channel
			case val := <-b:
				combined <- val
			}
		}
	}()

	// return the combined channel
	return combined
}

func main() {

	// make two input channels
	a := make(chan int)
	b := make(chan int)

	// launch a goroutine so that main doesn't get blocked
	go func() {

		// fill the input channels with data
		for i := 0; i < 4; i++ {
			a <- 1
			b <- 2
		}
	}()

	// obtain the combined channel
	combinedChannel := combineIntChans(a, b)

	// print from the combined channel
	for i := 0; i < 8; i++ {
		fmt.Println(<-combinedChannel)
	}
}
