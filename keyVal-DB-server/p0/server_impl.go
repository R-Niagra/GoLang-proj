// Implementation of a KeyValueServer. Students should write their code in this file.

package p0
import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
)

type keyValueServer struct {
	// TODO: implement this!
	connections int;
	maxClients int;
	status []int;
	inc chan int;
	ret chan int;
}
// New creates and returns (but does not start) a new KeyValueServer.
func New() *keyValueServer {
	// TODO: implement this!
	init_db();

	myStruct:= keyValueServer{connections:0,maxClients:0}
	myStruct.inc = make(chan int,500)
	myStruct.ret = make(chan int,500)
	myStruct.status=make([]int,1500)

	return &myStruct

}

func (kvs *keyValueServer) Start(port int) error {
	// TODO: implement this!
	port_str:= ":"+strconv.Itoa(port)

	ln, err := net.Listen("tcp", port_str)
	if err != nil {
		return err
	      }


	var replyAll [1500]chan string
	for i := range replyAll {
   		replyAll[i] = make(chan string,500)
		}

	putGetC:= make(chan string,200)
	var key string;
	var value []byte;

	var conns []net.Conn;

	go kvs.countHandler();   // Takes care of increment rece cond
	

	go func(){     	//Go routine for get and put in data base

		for{
			select{
					case data:= <- putGetC:
						
						msgCom := strings.Split(data[:len(data)-1],",")
						if(msgCom[0]=="put"){
						key=msgCom[1]
						value=[]byte(msgCom[2])
						put(key,value)
						// fmt.Println("value is placed")
							}	

						if(msgCom[0]=="get"){
						key=msgCom[1]
						answer:= string(get(key))
						answer=key+","+answer
						fmt.Println(answer)

						mCount:= len(conns)

						for i:=0;i<mCount;i++{	

						if(conns[i] != nil){		//Only sending to those who are alive
						(replyAll[i]) <-  answer
						}
							}
						}
			}
		}
	}()


	go func(){

		for {
		 conn, err := ln.Accept()

		 conns = append(conns,conn)

		if err != nil {
			// fmt.Printf("Couldn't accept a client connection: %s\n", err)
		} else {

			kvs.ret <- 1;
			numb:= <-kvs.ret
			go kvs.handleConnection(conns[len(conns)-1],replyAll[numb],putGetC,numb)
			}
		}	
	}()
	// fmt.Println("check3")

	return nil
}

func (kvs *keyValueServer) handleConnection(conn net.Conn,replyAll chan string,putGetC chan string,index int){

	// defer closeConn(conn)

	kvs.inc<- 1
	rw := ConnectionToRW(conn)

	go func(){

		for {

				data := <-replyAll    //Every connection will relay reply to its own client
			
					_, err := rw.WriteString(data+"\n")
					if err != nil {
						// fmt.Printf("There was an error writing to the server: %s\n", err)
						return
					}
					err = rw.Flush()
					// fmt.Println("Your string is flushed")
					if err != nil {
						// fmt.Printf("There was an error writing to the server: %s\n", err)
						return
						} 
		}
	}()

	for {
		// get client message
		msg, err := rw.ReadString('\n')

		if err != nil {
			kvs.inc<- (-1)

			closeConn(conn)
			return
					}

		putGetC <- msg
	}
}

func closeConn(conn net.Conn) {
	// clean up connection related data structures and goroutines here
	conn.Close()
	conn=nil;
	
}

// ConnectionToRW takes a connection and returns a buffered reader / writer on it
func ConnectionToRW(conn net.Conn) *bufio.ReadWriter {

	r:= bufio.NewReader(conn)
	w:= bufio.NewWriter(conn)

	bw:=bufio.NewWriterSize(w, 500)
	br:= bufio.NewReaderSize(r, 500)

	return bufio.NewReadWriter(br, bw)
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!
}

func (kvs *keyValueServer)countHandler(){

	for{
		select{
			case data:= <- kvs.inc:
				kvs.connections=kvs.connections + data
				// fmt.Println("conn are: ", kvs.connections)
			
			case <- kvs.ret:
				kvs.ret <- kvs.connections
		}
	}
}

func (kvs *keyValueServer) Count() int {

	kvs.ret <- 0
	value:= <-kvs.ret
	return value
}
