package main

import (
	"fmt"
)

func main() {
	fmt.Println("Hello, world!")

	var s string
	s = "I'm a string"
	fmt.Printf("Type: %8T, value: %15v\n", s, s)

	f := 10.00110011
	fmt.Printf("Type: %8T, value: %15.5g\n", f, f)

	var i = 22102110
	formattedString := fmt.Sprintf("Type: %8T, value: %15d\n", i, i)
	fmt.Println(formattedString)
}
