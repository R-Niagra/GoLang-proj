package main

import (
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"time"
	"tokenring"
)

const waitBetweenRounds = 2 // number of seconds to wait between rounds
var noNode []int
var randomGen *rand.Rand

func main() {

	noNode = []int{}
	randomGen = rand.New(rand.NewSource(time.Now().UnixNano()))

	testsToRun := []func(){basicTest1, basicTest2, basicTest3, basicTest4, basicTest5, basicTest6, advancedTest1, advancedTest2}
	flags := []string{"basic1", "basic2", "basic3", "basic4", "basic5", "basic6", "basadvanced1", "basadvanced2"}

	testSet := "no"

	if len(os.Args) == 1 {
		testSet = "bas"
	} else {
		if os.Args[1] == "basic" {
			testSet = "basic"
		} else if os.Args[1] == "advanced" {
			testSet = "advanced"
		} else {
			testSet = os.Args[1]
		}
	}

	for i := 0; i < len(testsToRun); i++ {
		if strings.Contains(flags[i], testSet) {
			testsToRun[i]()
		}
	}
}

func genRandomSlice(size int) []int {
	var randSlice []int
	for i := 0; i < size; i++ {
		randSlice = append(randSlice, randomGen.Intn(1000000))
	}
	return randSlice
}

func setUp(numNodes int) (chan bool, []chan bool, []chan int, map[int]chan tokenring.Message) {
	start := make(chan bool, numNodes)
	var quit []chan bool
	var data []chan int
	commChannels := make(map[int]chan tokenring.Message)
	return start, quit, data, commChannels
}

func startUp(numNodes int, quit *[]chan bool, data *[]chan int, commChannels map[int]chan tokenring.Message, start chan bool, resource *[]int) {
	for i := 0; i < numNodes; i++ {
		*quit = append(*quit, make(chan bool, 1))
		*data = append(*data, make(chan int, 100))
		commChannels[i] = make(chan tokenring.Message, 2)
	}
	for i := 0; i < numNodes; i++ {
		go tokenring.TokenRing(i, commChannels, start, (*quit)[i], (*data)[i], resource)
	}
}

func inSlice(slice []int, item int) bool {
	for _, val := range slice {
		if item == val {
			return true
		}
	}
	return false
}

func crashAndStart(nodes []int, quit []chan bool, start chan bool, crashNode []int) {
	for _, i := range nodes {
		// send crashNode quit signal and start the rest of the nodes
		if inSlice(crashNode, i) {
			quit[i] <- true
		} else {
			quit[i] <- false
			start <- true
		}
	}
}

func sliceToMap(values []int) map[int]int {
	result := make(map[int]int)
	for _, v := range values {
		_, isKey := result[v]
		if isKey {
			result[v]++
		} else {
			result[v] = 1
		}
	}
	return result
}

func checkResult(resource []int, values []int, moreThanOneToken bool) {
	if moreThanOneToken {
		fmt.Println("\nMore than one token was found circulating in the ring in some round.")
		fmt.Println("Test failed.")
		return
	}
	if reflect.DeepEqual(sliceToMap(resource), sliceToMap(values)) {
		fmt.Println("\nTest passed.")
	} else {
		fmt.Println("\nThe shared resource did not receive the correct number and values of updates.")
		fmt.Println("Test failed.")
	}
}

func countTokens(commChannels map[int]chan tokenring.Message) []int {
	var result []int
	for k, v := range commChannels {
		var msgsList []tokenring.Message
		for len(v) > 0 {
			msg := <-v
			if msg.Type == tokenring.TOKEN {
				result = append(result, k)
			}
			msgsList = append(msgsList, msg)
		}
		for len(msgsList) > 0 {
			v <- msgsList[0]
			msgsList = msgsList[1:]
		}
	}
	return result
}

func shutDown(nodes []int, quit []chan bool) {
	for _, i := range nodes {
		quit[i] <- true
	}
}

func sendDataRoundRobin(data []chan int, values []int) {
	valInd := 0
	dataInd := 0
	for valInd < len(values) {
		data[dataInd] <- values[valInd]
		valInd++
		dataInd = (dataInd + 1) % len(data)
	}
}

func removeVal(values []int, remVal int) []int {
	var index int
	for i, v := range values {
		if v == remVal {
			index = i
			break
		}
	}
	return append(values[:index], values[index+1:]...)
}

func emptyChannel(comm map[int]chan tokenring.Message, pid int) {
	for len(comm[pid]) > 0 {
		<-comm[pid]
	}
}

func emptyDeadChannels(comm map[int]chan tokenring.Message, deadNodes []int) {
	for _, node := range deadNodes {
		emptyChannel(comm, node)
	}
}

func getData(dataChan chan int) []int {
	var data []int
	for len(dataChan) > 0 {
		data = append(data, <-dataChan)
	}
	for _, v := range data {
		dataChan <- v
	}
	return data
}

func basicTest1() {
	fmt.Println("\n========== Basic Test 1: 3 nodes, 3 rounds ==========")
	fmt.Println("PID of 3 nodes: 0, 1 and 2\n")
	fmt.Println("Description:")
	fmt.Println("1. Nodes are given 3 values each to put into the shared resource.")
	fmt.Println("2. All values are available to each node at the start of round 1.\n")

	// set up the parameters and the resource
	const numNodes = 3
	var resource []int
	start, quit, data, commChannels := setUp(numNodes)

	// start up the nodes
	fmt.Println("Starting up the nodes...")
	startUp(numNodes, &quit, &data, commChannels, start, &resource)

	// add values into the channels
	values := []int{0, 1, 2, 3, 4, 5, 6, 7, 8}
	sendDataRoundRobin(data, values)

	// run the rounds
	fmt.Println("Running rounds 1-3...")
	aliveNodes := []int{0, 1, 2}
	moreThanOneToken := false
	for i := 1; i <= 3; i++ {
		crashAndStart(aliveNodes, quit, start, noNode)
		time.Sleep(time.Second * waitBetweenRounds)
		if len(countTokens(commChannels)) > 1 {
			moreThanOneToken = true
		}
	}

	// check result
	checkResult(resource, values, moreThanOneToken)

	// shutdown all nodes
	shutDown(aliveNodes, quit)
	time.Sleep(time.Second * waitBetweenRounds)
}

func basicTest2() {
	fmt.Println("\n\n========== Basic Test 2: 4 nodes, 6 rounds ==========")
	fmt.Println("PID of 4 nodes: 0, 1, 2 and 3\n")
	fmt.Println("Description:")
	fmt.Println("1. Nodes are given 4 values each to put into the resource.")
	fmt.Println("2. 2 of the values are available to each node at the start of round 1.")
	fmt.Println("3. Rest of the values (i.e. 2) are available to each node at the start of round 3.\n")

	// set up the parameters and the resource
	const numNodes = 4
	var resource []int
	start, quit, data, commChannels := setUp(numNodes)

	// start up the nodes
	fmt.Println("Starting up the nodes...")
	startUp(numNodes, &quit, &data, commChannels, start, &resource)

	// add values into the channels - available at start of round 1
	values := genRandomSlice(8)
	sendDataRoundRobin(data, values)

	// run rounds 1-2
	fmt.Println("Running rounds 1-2...")
	aliveNodes := []int{0, 1, 2, 3}
	moreThanOneToken := false
	for i := 1; i <= 2; i++ {
		crashAndStart(aliveNodes, quit, start, noNode)
		time.Sleep(time.Second * waitBetweenRounds)
		if len(countTokens(commChannels)) > 1 {
			moreThanOneToken = true
		}
	}

	// add values into the channels - available at start of round 3
	values2 := genRandomSlice(8)
	sendDataRoundRobin(data, values2)
	values = append(values, values2...)

	// run rounds 3-6
	fmt.Println("Running rounds 3-6...")
	for i := 3; i <= 6; i++ {
		crashAndStart(aliveNodes, quit, start, noNode)
		time.Sleep(time.Second * waitBetweenRounds)
		if len(countTokens(commChannels)) > 1 {
			moreThanOneToken = true
		}
	}

	// check result
	checkResult(resource, values, moreThanOneToken)

	// shutdown all nodes
	shutDown(aliveNodes, quit)
	time.Sleep(time.Second * waitBetweenRounds)
}

func basicTest3() {
	fmt.Println("\n\n========== Basic Test 3: 6 nodes, 15 rounds ==========")
	fmt.Println("PID of 6 nodes: 0, 1, 2, 3, 4 and 5\n")
	fmt.Println("Description:")
	fmt.Println("1. Nodes are given random number of values each to put into the resource.")
	fmt.Println("2. Values are available to each node at the start of rounds 2, 5, 8, 9, 10.\n")

	// set up the parameters and the resource
	const numNodes = 6
	var resource []int
	start, quit, data, commChannels := setUp(numNodes)

	// start up the nodes
	fmt.Println("Starting up the nodes...")
	startUp(numNodes, &quit, &data, commChannels, start, &resource)

	// run rounds 1-15
	aliveNodes := []int{0, 1, 2, 3, 4, 5}
	dataRound := []int{2, 5, 8, 9, 10}
	moreThanOneToken := false
	var values []int
	for i := 1; i <= 15; i++ {
		fmt.Printf("Running round %d...\n", i)
		// add values into the channels
		if inSlice(dataRound, i) {
			valuesNew := genRandomSlice(randomGen.Intn(50))
			values = append(values, valuesNew...)
			sendDataRoundRobin(data, valuesNew)
		}
		// start the round
		crashAndStart(aliveNodes, quit, start, noNode)
		time.Sleep(time.Second * waitBetweenRounds)
		// check number of tokens floating around
		if len(countTokens(commChannels)) > 1 {
			moreThanOneToken = true
		}
	}

	// check result
	checkResult(resource, values, moreThanOneToken)

	// shutdown all nodes
	shutDown(aliveNodes, quit)
	time.Sleep(time.Second * waitBetweenRounds)
}

func basicTest4() {
	fmt.Println("\n\n========== Basic Test 4: 4 nodes, 8 rounds ==========")
	fmt.Println("PID of 4 nodes: 0, 1, 2 and 3\n")
	fmt.Println("Description:")
	fmt.Println("1. Nodes are given 4 values each to put into the resource.")
	fmt.Println("2. 2 of the values are available to each node at the start of round 1.")
	fmt.Println("3. A node without the token fails at the start of round 3.")
	fmt.Println("4. Rest of the values (i.e. 2) are available to each node at the start of round 3.\n")

	// set up the parameters and the resource
	const numNodes = 4
	var resource []int
	start, quit, data, commChannels := setUp(numNodes)

	// start up the nodes
	fmt.Println("Starting up the nodes...")
	startUp(numNodes, &quit, &data, commChannels, start, &resource)

	// add values into the channels - available at start of round 1
	values := genRandomSlice(8)
	sendDataRoundRobin(data, values)

	// run rounds 1-2
	fmt.Println("Running rounds 1-2...")
	aliveNodes := []int{0, 1, 2, 3}
	moreThanOneToken := false
	var tokenHistory []int
	for i := 1; i <= 2; i++ {
		crashAndStart(aliveNodes, quit, start, noNode)
		time.Sleep(time.Second * waitBetweenRounds)
		nodesWithToken := countTokens(commChannels)
		if len(nodesWithToken) == 0 {
			fmt.Printf("No token found in circulation at the end of round %d. No crash has happened yet.\n", i)
			fmt.Println("\nTest failed.")
			return
		}
		tokenHistory = append(tokenHistory, nodesWithToken[0])
		if len(nodesWithToken) > 1 {
			moreThanOneToken = true
		}
	}
	if tokenHistory[0] == tokenHistory[1] {
		fmt.Println("Token was not passed to another node in the last round.")
		fmt.Println("\nTest failed.")
		return
	}

	// run round 3
	fmt.Println("Running round 3...")
	// add values into the channels - available at start of round 3
	values2 := genRandomSlice(8)
	sendDataRoundRobin(data, values2)
	values = append(values, values2...)
	// crash a node without the token, i.e.
	//	 was not going to get the token in round 3 and
	//	 didn't have the token in round 2
	deadNodes := []int{}
	for _, val := range aliveNodes {
		if !inSlice(tokenHistory, val) {
			deadNodes = append(deadNodes, val)
			break
		}
	}
	if len(deadNodes) != 1 {
		fmt.Println("\nMore than one token was found circulating in the ring in some round.")
		fmt.Println("\nTest failed.")
		return
	}
	// empty Message channel of crashed node
	emptyDeadChannels(commChannels, deadNodes)
	// remove data values of crashed node from expected values in resource
	crashedNodeData := getData(data[deadNodes[0]])
	for _, v := range crashedNodeData {
		values = removeVal(values, v)
	}
	// start the round
	crashAndStart(aliveNodes, quit, start, deadNodes)
	aliveNodes = removeVal(aliveNodes, deadNodes[0])
	time.Sleep(time.Second * waitBetweenRounds)
	if len(countTokens(commChannels)) > 1 {
		moreThanOneToken = true
	}

	// run rounds 4-8
	fmt.Println("Running rounds 4-8...")
	for i := 4; i <= 8; i++ {
		emptyDeadChannels(commChannels, deadNodes)
		crashAndStart(aliveNodes, quit, start, noNode)
		time.Sleep(time.Second * waitBetweenRounds)
		if len(countTokens(commChannels)) > 1 {
			moreThanOneToken = true
		}
	}

	// check result
	checkResult(resource, values, moreThanOneToken)

	// shutdown all nodes
	shutDown(aliveNodes, quit)
	time.Sleep(time.Second * waitBetweenRounds)
}

func basicTest5() {
	fmt.Println("\n\n========== Basic Test 5: 4 nodes, 8 rounds ==========")
	fmt.Println("PID of 4 nodes: 0, 1, 2 and 3\n")
	fmt.Println("Description:")
	fmt.Println("1. Nodes are given 4 values each to put into the resource.")
	fmt.Println("2. 2 of the values are available to each node at the start of round 1.")
	fmt.Println("3. Node receiving the token fails at the start of round 3.")
	fmt.Println("4. Rest of the values (i.e. 2) are available to each node at the start of round 3.\n")

	// set up the parameters and the resource
	const numNodes = 4
	var resource []int
	start, quit, data, commChannels := setUp(numNodes)

	// start up the nodes
	fmt.Println("Starting up the nodes...")
	startUp(numNodes, &quit, &data, commChannels, start, &resource)

	// add values into the channels - available at start of round 1
	values := genRandomSlice(8)
	sendDataRoundRobin(data, values)

	// run rounds 1-2
	fmt.Println("Running rounds 1-2...")
	aliveNodes := []int{0, 1, 2, 3}
	moreThanOneToken := false
	var tokenHistory []int
	for i := 1; i <= 2; i++ {
		crashAndStart(aliveNodes, quit, start, noNode)
		time.Sleep(time.Second * waitBetweenRounds)
		nodesWithToken := countTokens(commChannels)
		if len(nodesWithToken) == 0 {
			fmt.Printf("No token found in circulation at the end of round %d. No crash has happened yet.\n", i)
			fmt.Println("\nTest failed.")
			return
		}
		tokenHistory = append(tokenHistory, nodesWithToken[0])
		if len(nodesWithToken) > 1 {
			moreThanOneToken = true
		}
	}
	if tokenHistory[0] == tokenHistory[1] {
		fmt.Println("Token was not passed to another node in the last round.")
		fmt.Println("\nTest failed.")
		return
	}

	// run round 3
	fmt.Println("Running round 3...")
	// add values into the channels - available at start of round 3
	values2 := genRandomSlice(8)
	sendDataRoundRobin(data, values2)
	values = append(values, values2...)
	// crash the node receiving the token in round 3
	deadNodes := []int{tokenHistory[1]}
	// empty Message channel of crashed node
	emptyDeadChannels(commChannels, deadNodes)
	// remove data values of crashed node from expected values in resource
	crashedNodeData := getData(data[deadNodes[0]])
	for _, v := range crashedNodeData {
		values = removeVal(values, v)
	}
	// start the round
	crashAndStart(aliveNodes, quit, start, deadNodes)
	aliveNodes = removeVal(aliveNodes, deadNodes[0])
	time.Sleep(time.Second * waitBetweenRounds)
	if len(countTokens(commChannels)) > 1 {
		moreThanOneToken = true
	}

	// run rounds 4-8
	fmt.Println("Running rounds 4-8...")
	for i := 4; i <= 8; i++ {
		emptyDeadChannels(commChannels, deadNodes)
		crashAndStart(aliveNodes, quit, start, noNode)
		time.Sleep(time.Second * waitBetweenRounds)
		if len(countTokens(commChannels)) > 1 {
			moreThanOneToken = true
		}
	}

	// check result
	checkResult(resource, values, moreThanOneToken)

	// shutdown all nodes
	shutDown(aliveNodes, quit)
	time.Sleep(time.Second * waitBetweenRounds)
}

func basicTest6() {
	fmt.Println("\n\n========== Basic Test 6: 4 nodes, 8 rounds ==========")
	fmt.Println("PID of 4 nodes: 0, 1, 2 and 3\n")
	fmt.Println("Description:")
	fmt.Println("1. Nodes are given 4 values each to put into the resource.")
	fmt.Println("2. 2 of the values are available to each node at the start of round 1.")
	fmt.Println("3. Most recent node to send / pass the token fails at the start of round 3.")
	fmt.Println("4. Rest of the values (i.e. 2) are available to each node at the start of round 3.\n")

	// set up the parameters and the resource
	const numNodes = 4
	var resource []int
	start, quit, data, commChannels := setUp(numNodes)

	// start up the nodes
	fmt.Println("Starting up the nodes...")
	startUp(numNodes, &quit, &data, commChannels, start, &resource)

	// add values into the channels - available at start of round 1
	values := genRandomSlice(8)
	sendDataRoundRobin(data, values)

	// run rounds 1-2
	fmt.Println("Running rounds 1-2...")
	aliveNodes := []int{0, 1, 2, 3}
	moreThanOneToken := false
	var tokenHistory []int
	for i := 1; i <= 2; i++ {
		crashAndStart(aliveNodes, quit, start, noNode)
		time.Sleep(time.Second * waitBetweenRounds)
		nodesWithToken := countTokens(commChannels)
		if len(nodesWithToken) == 0 {
			fmt.Printf("No token found in circulation at the end of round %d. No crash has happened yet.\n", i)
			fmt.Println("\nTest failed.")
			return
		}
		tokenHistory = append(tokenHistory, nodesWithToken[0])
		if len(nodesWithToken) > 1 {
			moreThanOneToken = true
		}
	}
	if tokenHistory[0] == tokenHistory[1] {
		fmt.Println("Token was not passed to another node in the last round.")
		fmt.Println("\nTest failed.")
		return
	}

	// run round 3
	fmt.Println("Running round 3...")
	// add values into the channels - available at start of round 3
	values2 := genRandomSlice(8)
	sendDataRoundRobin(data, values2)
	values = append(values, values2...)
	// crash the node receiving the token in round 3
	deadNodes := []int{tokenHistory[0]}
	// empty Message channel of crashed node
	emptyDeadChannels(commChannels, deadNodes)
	// remove data values of crashed node from expected values in resource
	crashedNodeData := getData(data[deadNodes[0]])
	for _, v := range crashedNodeData {
		values = removeVal(values, v)
	}
	// start the round
	crashAndStart(aliveNodes, quit, start, deadNodes)
	aliveNodes = removeVal(aliveNodes, deadNodes[0])
	time.Sleep(time.Second * waitBetweenRounds)
	if len(countTokens(commChannels)) > 1 {
		moreThanOneToken = true
	}

	// run rounds 4-8
	fmt.Println("Running rounds 4-8...")
	for i := 4; i <= 8; i++ {
		emptyDeadChannels(commChannels, deadNodes)
		crashAndStart(aliveNodes, quit, start, noNode)
		time.Sleep(time.Second * waitBetweenRounds)
		if len(countTokens(commChannels)) > 1 {
			moreThanOneToken = true
		}
	}

	// check result
	checkResult(resource, values, moreThanOneToken)

	// shutdown all nodes
	shutDown(aliveNodes, quit)
	time.Sleep(time.Second * waitBetweenRounds)
}

func advancedTest1() {
	fmt.Println("\n\n========== Advanced Test 1: 5 nodes, upto 30 rounds ==========")
	fmt.Println("PID of 5 nodes: 0, 1, 2, 3 and 4\n")
	fmt.Println("Description:")
	fmt.Println("1. Nodes are given 4 values each to put into the resource.")
	fmt.Println("2. All of the values are available to each node at the start of round 1.")
	fmt.Println("3. All nodes crash except those passing the token to each other at the start of round 3.")
	fmt.Println("4. Node passing the token at the end of round 2 received 2 more values at the start of round 3.\n")

	// set up the parameters and the resource
	const numNodes = 5
	var resource []int
	start, quit, data, commChannels := setUp(numNodes)

	// start up the nodes
	fmt.Println("Starting up the nodes...")
	startUp(numNodes, &quit, &data, commChannels, start, &resource)

	// add values into the channels - available at start of round 1
	values := genRandomSlice(20)
	sendDataRoundRobin(data, values)

	// run rounds 1-2
	fmt.Println("Running rounds 1-2...")
	aliveNodes := []int{0, 1, 2, 3, 4}
	moreThanOneToken := false
	var tokenHistory []int
	for i := 1; i <= 2; i++ {
		crashAndStart(aliveNodes, quit, start, noNode)
		time.Sleep(time.Second * waitBetweenRounds)
		nodesWithToken := countTokens(commChannels)
		if len(nodesWithToken) == 0 {
			fmt.Printf("No token found in circulation at the end of round %d. No crash has happened yet.\n", i)
			fmt.Println("\nTest failed.")
			return
		}
		tokenHistory = append(tokenHistory, nodesWithToken[0])
		if len(nodesWithToken) > 1 {
			moreThanOneToken = true
		}
	}
	if tokenHistory[0] == tokenHistory[1] {
		fmt.Println("Token was not passed to another node in the last round.")
		fmt.Println("\nTest failed.")
		return
	}

	// run round 3
	fmt.Println("Running round 3...")
	// add 2 values into the channel of token passer
	values2 := genRandomSlice(2)
	values = append(values, values2...)
	data[tokenHistory[0]] <- values2[0]
	data[tokenHistory[0]] <- values2[1]
	// crash all nodes except those sending or receiving the token, i.e.
	//   3 nodes are crashed, 2 continue to be responsive
	deadNodes := []int{}
	for i := 0; i < numNodes; i++ {
		if !inSlice(tokenHistory, i) {
			deadNodes = append(deadNodes, i)
		}
	}
	// empty Message channel of crashed nodes
	emptyDeadChannels(commChannels, deadNodes)
	// remove data values of crashed nodes from expected values in resource
	var crashedNodeData []int
	for _, v := range deadNodes {
		crashedNodeData = append(crashedNodeData, getData(data[v])...)
	}
	for _, v := range crashedNodeData {
		values = removeVal(values, v)
	}
	// start the round
	crashAndStart(aliveNodes, quit, start, deadNodes)
	for _, v := range deadNodes {
		aliveNodes = removeVal(aliveNodes, v)
	}
	time.Sleep(time.Second * waitBetweenRounds)
	if len(countTokens(commChannels)) > 1 {
		moreThanOneToken = true
	}

	// run rounds 4-11
	fmt.Println("Running rounds 4-11...")
	for i := 4; i <= 11; i++ {
		// empty Message channel of crashed nodes
		emptyDeadChannels(commChannels, deadNodes)
		// start the nodes
		crashAndStart(aliveNodes, quit, start, noNode)
		time.Sleep(time.Second * waitBetweenRounds)
		if len(countTokens(commChannels)) > 1 {
			moreThanOneToken = true
		}
	}

	// check result
	fmt.Println("\n----Results upto round 11:")
	checkResult(resource, values, moreThanOneToken)

	// run rounds 12-14
	fmt.Println("\n\nRunning rounds 12-14...")
	for i := 12; i <= 14; i++ {
		// empty Message channel of crashed nodes
		emptyDeadChannels(commChannels, deadNodes)
		// start the nodes
		crashAndStart(aliveNodes, quit, start, noNode)
		time.Sleep(time.Second * waitBetweenRounds)
		if len(countTokens(commChannels)) > 1 {
			moreThanOneToken = true
		}
	}

	// check result
	fmt.Println("\n----Results upto round 14:")
	checkResult(resource, values, moreThanOneToken)

	// run rounds 15-30
	fmt.Println("\n\nRunning rounds 15-30...")
	for i := 15; i <= 30; i++ {
		// empty Message channel of crashed nodes
		emptyDeadChannels(commChannels, deadNodes)
		// start the nodes
		crashAndStart(aliveNodes, quit, start, noNode)
		time.Sleep(time.Second * waitBetweenRounds)
		if len(countTokens(commChannels)) > 1 {
			moreThanOneToken = true
		}
	}

	// check result
	fmt.Println("\n----Results upto round 30:")
	checkResult(resource, values, moreThanOneToken)

	// shutdown all nodes
	shutDown(aliveNodes, quit)
	time.Sleep(time.Second * waitBetweenRounds)
}

func advancedTest2() {
	fmt.Println("\n\n========== Advanced Test 2: 5 nodes, upto 40 rounds ==========")
	fmt.Println("PID of 5 nodes: 0, 1, 2, 3 and 4\n")
	fmt.Println("Description:")
	fmt.Println("1. Nodes are given 4 values each to put into the resource.")
	fmt.Println("2. 2 of the values are available to each node at the start of round 6.")
	fmt.Println("3. Most recent node to send / pass the token fails at the start of round 8.")
	fmt.Println("4. Node receiving the token also fails at the start of round 8.")
	fmt.Println("5. Rest of the values (i.e. 2) are available to each node at the start of round 8.")
	fmt.Println("6. Predecessor of the node crashed in 3 is given 2 more values at the start of round 16.\n")

	// set up the parameters and the resource
	const numNodes = 5
	var resource []int
	start, quit, data, commChannels := setUp(numNodes)

	// start up the nodes
	fmt.Println("Starting up the nodes...")
	startUp(numNodes, &quit, &data, commChannels, start, &resource)

	// burn round 1-5
	fmt.Println("Running rounds 1-5...")
	aliveNodes := []int{0, 1, 2, 3, 4}
	moreThanOneToken := false
	for i := 1; i <= 5; i++ {
		crashAndStart(aliveNodes, quit, start, noNode)
		time.Sleep(time.Second * waitBetweenRounds)
		if len(countTokens(commChannels)) > 1 {
			moreThanOneToken = true
		}
	}
	if len(countTokens(commChannels)) < 1 {
		fmt.Println("No token being passed at the end of round 5.")
		fmt.Println("\nTest failed.")
		return
	}
	predecessor := countTokens(commChannels)[0]

	// add values into the channels - available at start of round 6
	values := genRandomSlice(10)
	sendDataRoundRobin(data, values)

	// run rounds 6-7
	fmt.Println("Running rounds 6-7...")
	var tokenHistory []int
	for i := 6; i <= 7; i++ {
		crashAndStart(aliveNodes, quit, start, noNode)
		time.Sleep(time.Second * waitBetweenRounds)
		nodesWithToken := countTokens(commChannels)
		if len(nodesWithToken) == 0 {
			fmt.Printf("No token found in circulation at the end of round %d. No crash has happened yet.\n", i)
			fmt.Println("\nTest failed.")
			return
		}
		tokenHistory = append(tokenHistory, nodesWithToken[0])
		if len(nodesWithToken) > 1 {
			moreThanOneToken = true
		}
	}
	if tokenHistory[0] == tokenHistory[1] {
		fmt.Println("Token was not passed to another node in the last round.")
		fmt.Println("\nTest failed.")
		return
	}

	// run round 8
	fmt.Println("Running round 8...")
	// add values into the channels - available at start of round 3
	values2 := genRandomSlice(10)
	sendDataRoundRobin(data, values2)
	values = append(values, values2...)
	// crash the node receiving the token in round 8
	// crash the node which sent the token at the end of round 7
	deadNodes := []int{tokenHistory[0], tokenHistory[1]}
	// empty Message channel of crashed node
	emptyDeadChannels(commChannels, deadNodes)
	// data values of crashed node will never be seen in resource
	crashedNodeData := getData(data[deadNodes[0]])
	crashedNodeData = append(crashedNodeData, getData(data[deadNodes[1]])...)
	for _, v := range crashedNodeData {
		values = removeVal(values, v)
	}
	// start the round
	crashAndStart(aliveNodes, quit, start, deadNodes)
	aliveNodes = removeVal(aliveNodes, deadNodes[0])
	aliveNodes = removeVal(aliveNodes, deadNodes[1])
	time.Sleep(time.Second * waitBetweenRounds)
	if len(countTokens(commChannels)) > 1 {
		moreThanOneToken = true
	}

	// run rounds 9-15
	fmt.Println("Running rounds 9-15...")
	for i := 9; i <= 15; i++ {
		emptyDeadChannels(commChannels, deadNodes)
		crashAndStart(aliveNodes, quit, start, noNode)
		time.Sleep(time.Second * waitBetweenRounds)
		if len(countTokens(commChannels)) > 1 {
			moreThanOneToken = true
		}
	}

	// add 2 values into channel of node 0 at the start of round 16
	values3 := genRandomSlice(2)
	values = append(values, values3...)
	data[predecessor] <- values3[0]
	data[predecessor] <- values3[1]

	// run rounds 16-17
	fmt.Println("Running rounds 16-17...")
	for i := 16; i <= 17; i++ {
		emptyDeadChannels(commChannels, deadNodes)
		crashAndStart(aliveNodes, quit, start, noNode)
		time.Sleep(time.Second * waitBetweenRounds)
		if len(countTokens(commChannels)) > 1 {
			moreThanOneToken = true
		}
	}

	// check result
	fmt.Println("\n----Results upto round 17:")
	checkResult(resource, values, moreThanOneToken)

	// run rounds 18-19
	fmt.Println("\n\nRunning rounds 18-19...")
	for i := 18; i <= 19; i++ {
		emptyDeadChannels(commChannels, deadNodes)
		crashAndStart(aliveNodes, quit, start, noNode)
		time.Sleep(time.Second * waitBetweenRounds)
		if len(countTokens(commChannels)) > 1 {
			moreThanOneToken = true
		}
	}

	// check result
	fmt.Println("\n----Results upto round 19:")
	checkResult(resource, values, moreThanOneToken)

	// run rounds 20-40
	fmt.Println("\n\nRunning rounds 20-40...")
	for i := 20; i <= 40; i++ {
		emptyDeadChannels(commChannels, deadNodes)
		crashAndStart(aliveNodes, quit, start, noNode)
		time.Sleep(time.Second * waitBetweenRounds)
		if len(countTokens(commChannels)) > 1 {
			moreThanOneToken = true
		}
	}

	// check result
	fmt.Println("\n----Results upto round 40:")
	checkResult(resource, values, moreThanOneToken)

	// shutdown all nodes
	shutDown(aliveNodes, quit)
	time.Sleep(time.Second * waitBetweenRounds)
}
