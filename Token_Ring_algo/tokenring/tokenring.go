package tokenring


// MsgType represents the type of messages that can be passed between nodes
type MsgType int

// custom message type to be used for communication between nodes
const (
	TOKEN MsgType = iota // DO NOT CHANGE OR REMOVE THIS TYPE
	// TODO: add message types as needed
	Confirm
)

// Message is used to communicate with the other nodes
type Message struct {
	Type MsgType // DO NOT CHANGE OR REMOVE THIS FIELD
	p_id int
	Round int
}

// TokenRing runs the token-ring mutual exclusion algorithm on a node
func TokenRing(pid int, comm map[int]chan Message, startRound <-chan bool, quit <-chan bool, data <-chan int, resource *[]int) {

	// TODO: initialization code

	round := 0
	myTurn := false 
	repass :=false
	waitConfirmation:=false;
	nextFails:= 0;
	afterPass := 0   //Denotes rounds after token has benn passed
	nextTokenRound:= 100   //Max next round a node will receive a token

	for {
		// quit / crash the program
		if <-quit {
			return
		}
		// start the round
		<-startRound

		if(round==0 && pid==0){
			// fmt.Println(pid, " have the resource");
			myTurn = true
		}

		round++;
		var prev int;
		
		msgs := getMessages(comm[pid],round-1)
		// fmt.Println("Got the msg", len(msgs))

		for _, msg := range msgs {
		// fmt.Println("Msg type  is: ",msg.Type)

		if(msg.Type==TOKEN){ 
			// fmt.Println(pid," have received a token");
			myTurn=true;
			//Reply back with confirmation
			prev=msg.p_id
			confirmMessage:= Message{Confirm,pid,round}    //Sends back confirmation message to logically previous node
			comm[prev] <- confirmMessage
			// fmt.Println("My id is: ", pid, "prev is: ", prev) 
		
		}else if(msg.Type==Confirm && waitConfirmation==true){
			// fmt.Println("Token passed. Confirmation received. My pid: ", pid);   // Confirms that token has been passed in next 2 rounds
			waitConfirmation=false;
			repass=false;
		}
	}


		if(waitConfirmation==true){  //No confirmation message was received above in the immediate next round
			afterPass ++;
			if(afterPass>=2){// Token was failed to pass. Will resend token
			// fmt.Println(pid ,"  failed to pass token. Resend!!")
			myTurn = true;
			repass=true;
			nextFails ++;
			}
		}

		next := (pid+1 + nextFails)%len(comm)   //Finds the logically next node
		// fmt.Println("My id is: ", pid, "next is: ", next, "Repass ", repass) 

		if(myTurn==true || round>=nextTokenRound){   //In case of not getting token after max rounds it will pass token to next node
			if(repass==false){	//Will only access resource if it is passed by other pid
				for len(data)>0 {  //If it has a repass flag don't make changes
					value:= <-data
					// fmt.Println("Value to append is: ",value)
					put(resource,value)
				}
			}
			//Send token to next logical neighbour
			tokenMsg := Message{TOKEN,pid,round}    //pass token message
			comm[next] <- tokenMsg;
			myTurn = false
			waitConfirmation=true;
			afterPass=0;
			nextTokenRound = round+(2*(len(comm)-2))+2;  

		}
	}
}	

// assumes messages from previous rounds have been read already
func getMessages(msgs chan Message, round int) []Message {
	var result []Message

	// check if there are messages to be read
	if len(msgs) > 0 {
		var m Message

		// read all messages belonging to the corresponding round
		for m = <-msgs; m.Round == round; m = <-msgs {
			result = append(result, m)
			if len(msgs) == 0 {
				break
			}
		}

		// insert back message from any other round
		if m.Round != round {
			msgs <- m
		}
	}
	return result
}

// append a value to the shared resource
func put(resource *[]int, value int) {
	*resource = append(*resource, value)
}







