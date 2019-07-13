package main

import (
	"fmt"
	"net"
	"os"
	"encoding/json"
	"bitcoin"
	"bufio"
)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (net.Conn, error) {

	conn, err := net.Dial("tcp", hostport)
	if err != nil{
	return nil, err
	}



	return conn, nil
}

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
		return
	}

	hostport := os.Args[1]
	miner, err := joinWithServer(hostport)
	if err != nil {
		fmt.Println("Failed to join with server:", err)
		return
	}

	defer miner.Close()

	joinMsg := bitcoin.NewJoin(); 


	rw,err := rwBuffer(miner)

	fmt.Println("Sending join request!!!")

	sendToServer(joinMsg,rw)

	bite := make([]byte,200)


		for{

		fmt.Println("ready to receive a job!!")
		data, err := rw.Read(bite)
		if err != nil {
			fmt.Println("There was an error reading from the server: %s\n", err)
			return
					}


		var job bitcoin.Message
		err = json.Unmarshal(bite[:data], &job)
		fmt.Println("Received job: ",job)

		minHash,minNonce :=  doJob(job)

		fmt.Println("ans is: ", minHash, minNonce)
		result := bitcoin.NewResult(minHash,minNonce)
		
		sendToServer(result,rw)
		
	}

}

func sendToServer(msg *bitcoin.Message, rw *bufio.ReadWriter){
	
	bite := make([]byte,200)
	bite, err := json.Marshal(msg)
	
	_,er := rw.Write(bite)

		if er != nil {
			fmt.Printf("There was an error writing to the server: %s\n", er)
			return
		}
		er = rw.Flush()
		if err != nil {
			fmt.Printf("There was an error writing to the server: %s\n", er)
			return
		} 
}


func doJob(job bitcoin.Message)(uint64,uint64){

var ans uint64
var minHash uint64
var minNonce uint64
minHash=18446744073709551614;
for i:= job.Lower; i<= job.Upper; i++{

		ans = bitcoin.Hash(job.Data,i)
		if(ans<minHash){
		minHash = ans
		minNonce=i
		}
	}
return minHash,minNonce
}


func rwBuffer(conn net.Conn) (*bufio.ReadWriter, error) {
	
	r:= bufio.NewReader(conn)
	w:= bufio.NewWriter(conn)

	bw:=bufio.NewWriterSize(w, 100)
	br:= bufio.NewReaderSize(r, 100)

	return (bufio.NewReadWriter(br, bw)),nil
}