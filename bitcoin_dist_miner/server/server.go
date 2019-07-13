package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"bufio"
	"strconv"
	"encoding/json"
	"bitcoin"
	"container/list"
	// "reflect"
)

type server struct {
	listener net.Listener
}

func startServer(port int) (*server, error) {
	// TODO: implement this!

	port_str:= ":"+strconv.Itoa(port)

	var myServer server;
	var err error


	myServer.listener, err = net.Listen("tcp", port_str)
	if err != nil {
		return nil, err 
	      }

	

	return &myServer, nil
}



var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "0log.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	// Usage: LOGF.Println() or LOGF.Printf()

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <port>", os.Args[0])
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be a number:", err)
		return
	}

	srv, err := startServer(port)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Server listening on port", port)

	defer srv.listener.Close()

	// TODO: implement this!

	//Creating jobs queue
	jobs := list.New()
	clients := list.New()
	// fmt.Println("type is",reflect.TypeOf(jobs))
	jobSig := make(chan int,100)
	minerNum := 0
	clientNum := 0

	msgConv := make(chan bitcoin.Message)     //Will convey message
	chanConv := make(chan (chan bitcoin.Message))	//Will convey channel for a client for miner communication
	getJobNum := make(chan int)
	go jobScheduler(jobs,clients,msgConv,chanConv,getJobNum,jobSig,&minerNum,&clientNum)

	
	func(){

		for {
		conn, err := srv.listener.Accept()
		 
		if err != nil {
			// fmt.Printf("Couldn't accept a client connection: %s\n", err)
		} else {
		 fmt.Println("Somebody just got connected")
		 LOGF.Println("Somebody just got connected")
		 
		go handleConnection(conn,jobs,clients,jobSig,&minerNum,msgConv,chanConv,getJobNum,&clientNum)

		}
	  }
	
	}()	



}


func handleConnection(conn net.Conn,jobs *list.List,clients *list.List, jobSig chan int, minerNum *int, msgConv chan bitcoin.Message, chanConv chan chan bitcoin.Message, getJobNum chan int, clientNum *int){


	rw := ConnectionToRW(conn)

		bite := make([]byte,500)
		num, err := rw.Read(bite)   // Waiting for the 1st message
		// fmt.Println("bite is: ",bite[:num])

		if err != nil {
			fmt.Println("Error reading!! Close in handle connection")
			LOGF.Print("Reeor reading.. Close in handle connection")
			return
		}

		m := unmarshall(bite[:num])
		fmt.Println("msg is",m)
		LOGF.Print("msg is, ",m)

		if(m.Type == 0){    //Against miner
			*minerNum +=1;
			fmt.Println("miners are:  ", *minerNum)
			LOGF.Print("miners are:  ", *minerNum)

			handleMiner(rw,jobs,clients,jobSig,minerNum)
			LOGF.Println("Miner returned")

		}else if(m.Type == 1){  //against client
		connecto := make(chan bitcoin.Message, 10)
		fmt.Println("miners are:  ", *minerNum)
		LOGF.Println("miners are:  ", *minerNum)
		
		msgConv<- m
		chanConv<- connecto

		numJobs:= <-getJobNum

		*clientNum+= 1;
		handleClient(rw,connecto,numJobs,clientNum)
		}

}

func handleMiner(rw *bufio.ReadWriter,jobs *list.List, clients *list.List, jobSig chan int, minerNum *int){

		fmt.Println("in Miner handler")
		// present:= 0	

		for{

		if(jobs.Len()<=0){

		 <-jobSig 
		LOGF.Println("M waiting for job")

		}

		if(jobs.Len()>0){
			// present=0
			job := jobs.Front()
			jobs.Remove(job)

			client := clients.Front()
			clients.Remove(client)
			
			fmt.Print("job assigned is: ",job.Value)
			LOGF.Print("job assigned is: ",job.Value)
			
			msg := (job.Value).(bitcoin.Message)

			sendMsg(msg,rw)     //Job to the miner is sent
			
			fmt.Println("Waiting for the ans")
			LOGF.Println("Accepting miners response.")
			ans :=make([]byte,500)
			num, err := rw.Read(ans)   // Waiting for the reply message
			if err != nil {
			
			LOGF.Println("Error reading!! Close conn by miner. Reschedule job")
			LOGF.Print("miners are:  ", *minerNum)
			fmt.Println("Error reading!! Close conn by miner. Rescheduling job")
			
			jobs.PushBack(msg)

			clients.PushBack((client.Value).(chan bitcoin.Message))
			*minerNum -= 1			
			LOGF.Print("After subt miners are:  ", *minerNum)

			return
				}
			// fmt.Println("Received: ", ans)	
			clChan  := make(chan bitcoin.Message)
			
			clChan = (client.Value).(chan bitcoin.Message)
			// clients.Remove(client)
			LOGF.Println("clients left are: ",clients.Len())

			answer := unmarshall(ans[:num])
			clChan <- answer   //Send answer to the respective client
		}
	}
}


func handleClient(rw *bufio.ReadWriter, connecto chan bitcoin.Message, jobParts int, clientNum *int){
	//Only need to wait for the result
	counter :=0;
	var minHash uint64;
	minHash =18446744073709551614;
	var minNonce uint64;
	LOGF.Println("In client handler")

	for{
		data := <-connecto       //Receives answer form the miner on specific channel being sent
		fmt.Println("got: ", data)
		LOGF.Print("Got: ",data)
		// msg := unmarshall(data)
		if(data.Hash < minHash){
			minHash = data.Hash
			minNonce = data.Nonce
		}
		counter++;
		if(counter >= jobParts){
			break
		}

	}

	result := bitcoin.NewResult(minHash,minNonce)
	sendMsg(*result,rw)
	LOGF.Println("Result sent to client.", result)
	// *clientNum-= 1

}

func unmarshall(msg []byte)(bitcoin.Message){

	// bite := []byte(msg)
	// fmt.Println("Received: ",msg,)

	var m bitcoin.Message
	err := json.Unmarshal(msg, &m)
	if err !=nil{
		fmt.Println("Error unmarshalling!")
		LOGF.Print("Error unmarshalling")
	}
	// fmt.Println("Receiveed 2nd: ",m.Type)
	return m

}


func sendMsg(msg bitcoin.Message,rw *bufio.ReadWriter){

	b, err := json.Marshal(msg)

	_, er := rw.Write(b)

	if er != nil {
		fmt.Printf("There was an error writing to the server: %s\n", er)
		LOGF.Printf("There was an error writing to the server: %s\n", er)
		
		return
	}

	er = rw.Flush()
	
	if err != nil {
		fmt.Printf("There was an error writing to the server: %s\n", er)
		LOGF.Printf("There was an error writing to the server: %s\n", er)
		
		return
	} 


}


func jobScheduler(jobs *list.List, clients *list.List, msgConv chan bitcoin.Message, chanConv chan chan bitcoin.Message, getJobNum chan int, jobSig chan int, minerNum *int, clientNum *int){

	var jobPiece uint64

	for{

		msg:= <-msgConv
		connecto := <-chanConv


		numOfMiners:= *minerNum
		if(numOfMiners <= 0){
			numOfMiners =1
		}
		
		factor := jobs.Len()/(numOfMiners)
		 if(factor<=0){
		 	factor=1
		 }else if(factor > numOfMiners){
		 	factor= numOfMiners   
		 }
		jobLen := msg.Upper - msg.Lower
		
		if(jobLen <= 10000){
		jobPiece = jobLen	
		}else {
		jobPiece = (jobLen/uint64(numOfMiners))*uint64(factor)
		// jobPiece = (jobLen/uint64(numOfMiners+15))
			}
		// jobPiece = (jobLen/uint64(4))
			// }


		jobNum := jobLen/jobPiece
		count := 0
		LOGF.Println("Miners: ", numOfMiners, " Jobs: ", jobs.Len()," clients: ",clients.Len() ," Job pieces Len: ",jobPiece, "Job Numbers: ",jobNum)
		fmt.Println("Miners: ", numOfMiners, " Jobs: ", jobs.Len()," clients: ",clients.Len() ," Job pieces Len: ",jobPiece, "Job Numbers: ",jobNum)
		
		newJob := msg

		for i:=0; i<int(jobLen); i+=int(jobPiece){
		count +=1;

		newJob.Lower = uint64(i)
			if count == int(jobNum){
			newJob.Upper = msg.Upper
			i=int(msg.Upper)
			}else if count< int(jobNum){
			newJob.Upper = (uint64(i)+ jobPiece)
			}else{
				break
			}

		jobs.PushFront(newJob)
		clients.PushFront(connecto)
		jobSig <- 1
		LOGF.Println("New job Signaled and is: ",newJob)
		fmt.Println("New job Signaled and is: ",newJob)

		
	}
		getJobNum<- count
	}
}


func ConnectionToRW(conn net.Conn) *bufio.ReadWriter {

	r:= bufio.NewReader(conn)
	w:= bufio.NewWriter(conn)

	bw:=bufio.NewWriterSize(w, 500)
	br:= bufio.NewReaderSize(r, 500)

	return bufio.NewReadWriter(br, bw)
}