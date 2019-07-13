package paxos

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"paxosapp/rpc/paxosrpc"
	"time"
	// "bytes"
	// "strconv"
	// "paxosapp/rpc/paxosrpc"
	// "log"
)

var PROPOSE_TIMEOUT = 15 * time.Second

type paxosNode struct {
	// TODO: implement this!
	// conns map[int]net.Conn
	cliMap           map[int]*rpc.Client
	id               int
	pNum             int
	ringSize         int
	store            map[string]interface{}
	minProposal      map[string]int
	acceptedProposal map[string]int
}

// Desc:
// NewPaxosNode creates a new PaxosNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if this node
// could not be started in spite of dialing any other nodes numRetries times.
//
// Params:
// myHostPort: the hostport string of this new node. We use tcp in this project.
//			   	Note: Please listen to this port rather than hostMap[srvId]
// hostMap: a map from all node IDs to their hostports.
//				Note: Please connect to hostMap[srvId] rather than myHostPort
//				when this node try to make rpc call to itself.
// numNodes: the number of nodes in the ring
// numRetries: if we can't connect with some nodes in hostMap after numRetries attempts, an error should be returned
// replace: a flag which indicates whether this node is a replacement for a node which failed.
func NewPaxosNode(myHostPort string, hostMap map[int]string, numNodes, srvId, numRetries int, replace bool) (PaxosNode, error) {

	// port_str:= ":"+strconv.Itoa(port)
	//fmt.Println(myHostPort,"NewPaxosNodeStart")
	// fmt.Println("NEW  Replaced node **************************")
	node := paxosNode{cliMap: make(map[int]*rpc.Client), id: srvId, pNum: 10, ringSize: len(hostMap), store: make(map[string]interface{}), minProposal: make(map[string]int), acceptedProposal: make(map[string]int)}

	rpc.RegisterName("PaxosNode", paxosrpc.Wrap(&node)) //Register the node to make it accessible to other
	rpc.HandleHTTP()
	ln, err := net.Listen("tcp", myHostPort) //Listener on the port

	if err != nil {
		fmt.Println("Failed to listen:", err)
	}

	go http.Serve(ln, nil)

	counter := 0
	for i, _ := range hostMap {
		count := 0
		for count <= numRetries { //Retry to connect to nodes

			// fmt.Println("i is: ",i)
			cli, err := rpc.DialHTTP("tcp", hostMap[i])

			if err == nil {
				//fmt.Println(myHostPort,"connected",hostMap[i])
				node.cliMap[counter] = cli //Once connected it stores the client in map
				counter++
				break

			} else if err != nil {
				// fmt.Println("Retrying. Failed because :", err)
				time.Sleep(1 * time.Second)
			}
			count++
		}
	}

	if replace {

		for i, _ := range hostMap {

			repArgs := paxosrpc.ReplaceServerArgs{SrvID: srvId, Hostport: myHostPort}
			repReply := paxosrpc.ReplaceServerReply{}
			err := (node.cliMap[i]).Call("PaxosNode.RecvReplaceServer", &repArgs, &repReply)
			if err != nil {
				fmt.Println("Error in Propose RecvReplaceServer:", err)
			}

		}

		for i, _ := range hostMap {

			rcArg := paxosrpc.ReplaceCatchupArgs{}
			rcRep := paxosrpc.ReplaceCatchupReply{Data: make([]byte, 512)}
			err := (node.cliMap[i]).Call("PaxosNode.RecvReplaceCatchup", &rcArg, &rcRep)
			if err != nil {
				fmt.Println("Error in Propose RecvReplaceCatchup:", err)
			} else {

				var dat map[string]interface{}
				if err := json.Unmarshal(rcRep.Data, &dat); err != nil {
					fmt.Println(err)
				}

				for keyData, valData := range dat {
					commitArgs := paxosrpc.CommitArgs{Key: keyData, V: uint32(valData.(float64)), RequesterId: srvId}
					cr := paxosrpc.CommitReply{}
					err := node.RecvCommit(&commitArgs, &cr) //Calls to commit the key and valu
					if err != nil {
						fmt.Println("err", err)
					}
				}

			}
			break
		}

	}

	// fmt.Println("returning: ", len(node.cliMap)," is is: ",node.id)
	// node.ringSize= len(node.cliMap)
	// fmt.Println(myHostPort,"NewPaxosNodeEnd")

	return &node, nil
}

// Desc:
// GetNextProposalNumber generates a proposal number which will be passed to
// Propose. Proposal numbers should not repeat for a key, and for a particular
// <node, key> pair, they should be strictly increasing.
//
// Params:
// args: the key to propose
// reply: the next proposal number for the given key
func (pn *paxosNode) GetNextProposalNumber(args *paxosrpc.ProposalNumberArgs, reply *paxosrpc.ProposalNumberReply) error {

	// fmt.Println("key is: ",*args)
	// pn.minProposal[args.Key]=0      //Initializing the minproposal
	proposalNum := pn.pNum + pn.id //Gives unique proposal number for each node

	reply.N = proposalNum
	// pn.pNum+=10

	// fmt.Println("Proposal Num is: ",proposalNum, " key is: ", *args, "id: ", pn.id)

	return nil
}

// Desc:
// Propose initializes proposing a value for a key, and replies with the
// value that was committed for that key. Propose should not return until
// a value has been committed, or PROPOSE_TIMEOUT seconds have passed.
//
// Params:
// args: the key, value pair to propose together with the proposal number returned by GetNextProposalNumber
// reply: value that was actually committed for the given key
func (pn *paxosNode) Propose(args *paxosrpc.ProposeArgs, reply *paxosrpc.ProposeReply) error {
	// fmt.Println("")
	// fmt.Println("in propose: ",pn.id, "key/value to commit: ", args.Key,args.V)
	// fmt.Println("key/value to commit: ", args.Key,args.V)

	okCount := 0
	majority := (pn.ringSize / 2) + 1 // Majority votes required for reject or accept
	pr := paxosrpc.PrepareReply{}
	minReject := -10

	for i := 0; i < pn.ringSize; i++ {

		prepareArgs := paxosrpc.PrepareArgs{Key: args.Key, N: args.N, RequesterId: pn.id} //For passingto the RecvPrepare
		err := (pn.cliMap[i]).Call("PaxosNode.RecvPrepare", &prepareArgs, &pr)
		if err != nil {
			fmt.Println("Error in Propose RecvPrepare:", err)
		}

		if pr.Status == paxosrpc.OK { // Detects number of OK messages from the ring connection
			// fmt.Println("Received an OK in ",pn.id, "cnt " , i)
			okCount++
		} else {
			// fmt.Println("Received a reject", pn.id, "cnt " , i)
			// Update propsed value
			if pr.N_a > minReject {
				minReject = pr.N_a
				// fmt.Println(pr.V_a, "pr.V_a")
				reply.V = pr.V_a
			}
		}

		if okCount >= majority {
			//break
		}

	}

	if okCount >= majority { //If OKresponse > mmajority go to accept message
		// fmt.Println("Gain majority vote. GO next")

		okCount = 0
		//sending the accept message which is essentially phase 2

		for i := 0; i < pn.ringSize; i++ {

			acceptArg := paxosrpc.AcceptArgs{Key: args.Key, N: args.N, V: args.V, RequesterId: pn.id}
			ar := paxosrpc.AcceptReply{}
			client := pn.cliMap[i]
			err := client.Call("PaxosNode.RecvAccept", &acceptArg, &ar)
			if err != nil {

				fmt.Println("Error in Propose RecvAccept:", err)

			} else {

				if ar.Status == paxosrpc.OK {
					okCount++
				} else {
					// fmt.Println("Received Reject in Accept", pn.id)
				}

				if okCount >= majority {
					// fmt.Println("majority commited has been reached. Shall reak")
					reply.V = args.V
					//break
				}

			}
		}

	} else {

		// fmt.Println("Failed to achieve majority vote in prepare", "reply.V=", reply.V, "arg.V", args.V)
		time.Sleep(5 * time.Second)
		if pn.store[args.Key] != nil {
			reply.V = pn.store[args.Key]
			return nil
		} else {
			return errors.New("No Majority")
		}

	}

	if okCount >= majority {
		//time.Sleep(10*time.Second)
		reply.V = args.V
		return nil

	} else {

		// fmt.Println("Failed to achieve majority vote in accept")
		time.Sleep(5 * time.Second)
		if pn.store[args.Key] != nil {
			reply.V = pn.store[args.Key]
			return nil
		} else {
			return errors.New("No Majority")
		}
		// time.Sleep(10*time.Second)
		// return errors.New("No Majority")
	}

}

// func (pn *paxosNode) abc(x int, s string){
// 	fmt.Println("got into this")
// }

// Desc:
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (pn *paxosNode) GetValue(args *paxosrpc.GetValueArgs, reply *paxosrpc.GetValueReply) error {

	// fmt.Println()
	//fmt.Println("Checking by getValue key is: ",args.Key," printing store val: ",pn.store[args.Key], " id is: ",pn.id)

	if val, ok := pn.store[args.Key]; ok {
		//do something here
		reply.V = val
		reply.Status = paxosrpc.KeyFound //Key found if key value id present in db
	} else {
		reply.Status = paxosrpc.KeyNotFound //Corrosponding to not found
	}

	return nil
}

// Desc:
// Receive a Prepare message from another Paxos Node. The message contains
// the key whose value is being proposed by the node sending the prepare
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
//
// Params:
// args: the Prepare Message, you must include RequesterId when you call this API
// reply: the Prepare Reply Message

func (pn *paxosNode) RecvPrepare(args *paxosrpc.PrepareArgs, reply *paxosrpc.PrepareReply) error {

	pn.pNum += 10 // Update the proposal number. For the sake of monotonically increasing pn

	// fmt.Println("RecvPrepare: key: ", args.Key, " proposal: ", args.N, "recv id is: ",pn.id , "from", args.RequesterId)

	if proNum, ok := pn.minProposal[args.Key]; ok { //Checks of key value pair exists in the store

		//key exists in minPeoposal. Compare
		//fmt.Println("RecvPrepare val is",val,"ok is", ok)

		if args.N > proNum { //Checks for the new proposal number if it is greater than the previous min_proposal

			//send ok
			// fmt.Println("sender: ", args.RequesterId, "receiver:" , pn.id ,"OK sent RecvPrepare")
			reply.Status = paxosrpc.OK
			reply.N_a = args.N
			reply.V_a = nil
			pn.minProposal[args.Key] = args.N //Added the min proposal number

		} else {
			// reject in case old proposal number is received
			// fmt.Println("sender: ", args.RequesterId, "receiver:" , pn.id ,"Old proposal number received")
			reply.Status = paxosrpc.Reject
			reply.N_a = proNum
			// fmt.Println("pn.store[args.Key]=",pn.store[args.Key])
			reply.V_a = pn.store[args.Key]
		}

	} else {

		// key doesn't exist. simply add to it
		// fmt.Println("sender: ", args.RequesterId, "receiver:" , pn.id ,"Key doesn't exist in db")
		reply.Status = paxosrpc.OK
		reply.N_a = -1
		reply.V_a = nil
		pn.minProposal[args.Key] = args.N // Added the min proposal number

	}

	return nil
}

// Desc:
// Receive an Accept message from another Paxos Node. The message contains
// the key whose value is being proposed by the node sending the accept
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
//
// Params:
// args: the Please Accept Message, you must include RequesterId when you call this API
// reply: the Accept Reply Message
func (pn *paxosNode) RecvAccept(args *paxosrpc.AcceptArgs, reply *paxosrpc.AcceptReply) error {

	// fmt.Println("RecvAccept")

	if args.N >= pn.minProposal[args.Key] {
		// accpet this proposal. Sending commit proposal
		commitArgs := paxosrpc.CommitArgs{Key: args.Key, V: args.V, RequesterId: pn.id}
		cr := paxosrpc.CommitReply{}
		// err:= (pn.cliMap[pn.id]).Call("PaxosNode.RecvCommit",&commitArgs,&cr)
		err := pn.RecvCommit(&commitArgs, &cr) //Calls to commit the key and value

		if err != nil {

			fmt.Println("Error in RecvAccept against commit: ", err)

		} else {

			// fmt.Println("Value has been commited by: ",pn.id)
			pn.acceptedProposal[args.Key] = args.N //Updates accepted proposal
			reply.Status = paxosrpc.OK

		}

	} else {

		// fmt.Println("not an updated proposal. Rejecting")
		reply.Status = paxosrpc.Reject

	}

	return nil
}

// Desc:
// Receive a Commit message from another Paxos Node. The message contains
// the key whose value was proposed by the node sending the commit
// message.
//
// Params:
// args: the Commit Message, you must include RequesterId when you call this API
// reply: the Commit Reply Message
func (pn *paxosNode) RecvCommit(args *paxosrpc.CommitArgs, reply *paxosrpc.CommitReply) error {

	// fmt.Println("Received commit req by: ", args.RequesterId,"key value", args.Key,args.V)

	pn.store[args.Key] = args.V //Updates the store with the key and value

	// fmt.Println("in store: ",pn.store[args.Key])
	// fmt.Println("Calling getValue to check")
	// getValueArg := paxosrpc.GetValueArgs{Key:args.Key}
	// rp := paxosrpc.GetValueReply{}
	// err:= pn.cliMap[pn.id].Call("PaxosNode.GetValue", &getValueArg,&rp)
	// if(err!=nil){
	// 	fmt.Println("Error is: ",err)
	// }
	// fmt.Println("reply in commit: ",rp.V)

	return nil
}

// Desc:
// Notify another node of a replacement server which has started up. The
// message contains the Server ID of the node being replaced, and the
// hostport of the replacement node
//
// Params:
// args: the id and the hostport of the server being replaced
// reply: no use
func (pn *paxosNode) RecvReplaceServer(args *paxosrpc.ReplaceServerArgs, reply *paxosrpc.ReplaceServerReply) error {

	cli, err := rpc.DialHTTP("tcp", args.Hostport)
	// fmt.Println("RecvReplaceServer in ", pn.id , args.Hostport ,args.SrvID)
	if err != nil {
		fmt.Println("RecvReplaceServer", err)
	} else {
		// fmt.Println("args.SrvID",args.SrvID, cli)
		pn.cliMap[pn.ringSize] = cli
	}

	return nil
}

// Desc:
// Request the value that was agreed upon for a particular round. A node
// receiving this message should reply with the data (as an array of bytes)
// needed to make the replacement server aware of the keys and values
// committed so far.
//
// Params:
// args: no use
// reply: a byte array containing necessary data used by replacement server to recover
func (pn *paxosNode) RecvReplaceCatchup(args *paxosrpc.ReplaceCatchupArgs, reply *paxosrpc.ReplaceCatchupReply) error {

	var buf []byte
	buf, _ = json.Marshal(pn.store)
	reply.Data = buf
	// fmt.Println( string(reply.Data) )

	// Reply
	var dat map[string]interface{}
	if err := json.Unmarshal(reply.Data, &dat); err != nil {
		fmt.Println(err)
	}

	// fmt.Println("type assertion")
	// fmt.Println("RecvReplaceCatchup", pn.id)

	return nil
}
