package main

import (
	"fmt"
	"net"
	"os"
	"bufio"
	"encoding/json"
	"strconv"
	"bitcoin"
	"log"
)

func main() {
	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport> <message> <maxNonce>", os.Args[0])
		return
	}
const (
	name = "log.txt"
    flag = os.O_RDWR | os.O_CREATE
    perm = os.FileMode(0666)
)

file, err := os.OpenFile(name, flag, perm)
defer file.Close();
if err != nil {
return
}

LOGF := log.New(file, "", log.Lshortfile|log.Lmicroseconds)

	LOGF.Println("check 1")	
	hostport := os.Args[1]

	message := os.Args[2]


	maxNonce, err := strconv.ParseUint(os.Args[3], 10, 64)
	
	if err != nil {
		fmt.Printf("%s is not a number.\n", os.Args[3])
		return
	}
	LOGF.Println("check 2")	

	client, err := net.Dial("tcp", hostport)
	if err != nil {
		fmt.Println("Failed :", err)
		return
	}

	defer client.Close()
	LOGF.Println("check 3")	

	rw,err := rwBuffer(client)
	
	// TODO: implement this!

	m := bitcoin.NewRequest(message,0, maxNonce)

	LOGF.Println("Sending join req")	
	b, err := json.Marshal(m)
	_, er := rw.Write(b)
		if er != nil {
			fmt.Printf("There was an error writing to the server: %s\n", er)
			printDisconnected()
			return
		}
		er = rw.Flush()
		if err != nil {
			fmt.Printf("There was an error writing to the server: %s\n", er)
			printDisconnected()
			return
		} 

		///////////////////////////////////////////////
		// fmt.Println("Waiting for the answer")
		bite := make([]byte,500)

		LOGF.Println("Waiting for the answer")
		num, err := rw.Read(bite)
		if err != nil {
			// fmt.Printf("There was an error reading from the server: %s\n", err)
			printDisconnected()
			return
		}
		LOGF.Println("Got ans")

	var res bitcoin.Message
	err = json.Unmarshal(bite[:num], &res)
	if err !=nil{
		fmt.Println("Error unmarshalling!")
		printDisconnected()
		return
	}


	printResult(res.Hash, res.Nonce)
	LOGF.Println("Result", res.Hash, res.Nonce)

}

// printResult prints the final result to stdout.
func printResult(hash, nonce uint64) {
	fmt.Println("Result", hash, nonce)

}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}

func rwBuffer(conn net.Conn) (*bufio.ReadWriter, error) {
	
	r:= bufio.NewReader(conn)
	w:= bufio.NewWriter(conn)

	bw:=bufio.NewWriterSize(w, 100)
	br:= bufio.NewReaderSize(r, 100)

	return (bufio.NewReadWriter(br, bw)),nil	
}
