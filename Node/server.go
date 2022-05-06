package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type RaftInfo struct {
	CurrentTerm int       `json: "CurrentTerm"`
	VotedFor    string    `json: "VotedFor"`
	Log         []LogInfo `json: "Log"`
	LogTerm     []int     `json: "LogTerm"`
	Timeout     int       `json: "Timeout"`
	HeartBeat   int       `json: "HeartBeat"`
}

type LogInfo struct {
	Key   string `json: "Key"`
	Value string `json: "Value"`
	Term  int    `json: "Term"`
}

type jsonMessage struct {
	Sender_name  string `json: "sender_name"`
	Request      string `json: "request"` //Type of Request: APPEND_RPC/VOTE_REQUEST/VOTE_ACK
	Term         int    `json: "term"`
	Key          string `json: "key"`
	Value        string `json: "value"`
	Entries      string `json: entries`
	PrevLogIndex int    `json: prevLogIndex`
	LeaderCommit string `json: leaderCommit`
	PrevLogTerm  int    `json: prevLogTerm`
}

type Rooms struct {
	Rooms []RoomType `json: "Rooms"`
}

type RoomType struct {
	Type  string   `json: "type"`
	Dates []string `json: "Dates"`
}

type Booking struct {
	RoomType string   `json: "RoomType"`
	Dates    []string `json: "Dates"`
}

var nodeName string = os.Getenv("node_name")
var handleReq bool = true
var curTimeOut time.Time
var previousTime time.Time
var port string = ":5555"

type serverType int

const (
	Leader    serverType = 1
	Follower  serverType = 2
	Candidate serverType = 3
)

var currentServerType serverType
var raftInfo RaftInfo

//var nodeListID []string = []string{"127.0.0.1:5555", "127.0.0.1:5556"}

var nodeListID []string = []string{"1", "2", "3", "4", "5"}

var currentAcceptedVote = 0
var currentRejectedVote = 0

var commitIndex = 0
var lastApplied = 0
var commitIndexMap map[int]int

var nextIndex []int = []int{1, 1, 1, 1, 1}
var matchIndex []int = []int{0, 0, 0, 0, 0}

var leaderInfo string

func main() {

	//time.Sleep(100000 * time.Millisecond)
	raftInfo = loadRaftJson()

	//For localHost testing & no environment variable
	if nodeName == "" {
		nodeName = "127.0.0.1"
	}
	fmt.Println("---Node Name---" + nodeName)
	fmt.Println(raftInfo)
	//To generate random timeout, due to a bug we are generating random timeout sperately for every node--
	if nodeName == "Node1" {
		raftInfo.Timeout += rand.Intn(20)
	}
	if nodeName == "Node2" {
		raftInfo.Timeout += rand.Intn(30)
	}
	if nodeName == "Node3" {
		raftInfo.Timeout += rand.Intn(40)
	}
	if nodeName == "Node4" {
		raftInfo.Timeout += rand.Intn(50)
	}
	if nodeName == "Node5" {
		raftInfo.Timeout += rand.Intn(60)
	}

	fmt.Println("Random timeout value", raftInfo.Timeout)

	p := make([]byte, 2048)
	addr := net.UDPAddr{
		Port: 5555,
		IP:   net.ParseIP(nodeName),
	}
	curTimeOut = time.Now()
	ser, err := net.ListenUDP("udp", &addr)
	if err != nil {
		fmt.Printf("Some error %v\n", err)
		return
	}
	defer ser.Close()
	go handleRequest(ser, p)

	http.HandleFunc("/", rootHandler)
	//http.HandleFunc("/index.html", serveFile2)
	http.HandleFunc("/getReservation", getReservation)
	http.HandleFunc("/makeReservation", checkandUpdateReservation)
	http.HandleFunc("/updateNodes", updateNodes)
	fmt.Println("Server started at port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))

	fmt.Println("\n\n\n---Server Shutdown---\n\n\n")

}

func loadRaftJson() RaftInfo {
	jsonFile, err := os.Open("raft.json")
	if err != nil {
		fmt.Println(err)
	}

	defer jsonFile.Close()
	byteValue, _ := ioutil.ReadAll(jsonFile)
	var raftInfo RaftInfo
	json.Unmarshal(byteValue, &raftInfo)
	return raftInfo
}

func healthCheckTimer() {
	curTime := time.Now()
	previousTime = time.Now()
	for {
		ticker := time.NewTicker(time.Duration(1 * time.Millisecond))
		defer ticker.Stop()
		done := make(chan bool)
		sleep := 50 * time.Millisecond
		go func() {
			time.Sleep(sleep)
			done <- true
		}()
		ticks := 0
		exit := false

		for !exit {
			curTime = time.Now()
			select {
			case <-done:

				if handleReq && currentServerType == Leader && curTime.Sub(previousTime) > time.Duration(raftInfo.HeartBeat)*time.Millisecond {
					fmt.Println("-----Sending HeartBeat-----")
					previousTime = time.Now()
					sendHeartBeat()

				}

				if handleReq && currentServerType != Leader && curTime.Sub(previousTime) > time.Duration(raftInfo.Timeout)*time.Millisecond {
					// //Start Election Process:
					fmt.Println("Starting election process")

					raftInfo.CurrentTerm += 1
					//Send VoteRequest to all Nodess
					previousTime = time.Now()
					if currentServerType != Candidate {
						currentServerType = Candidate
						startVotingProcess()
					}

				}

				exit = true

			case <-ticker.C:
				ticks++
			}
		}
	}

}
func handleRequest(ser *net.UDPConn, p []byte) {
	go healthCheckTimer()
	for {
		n, remoteaddr, err := ser.ReadFromUDP(p)
		if err != nil {
			fmt.Printf("error  %v", err)
			continue
		}
		var reqData jsonMessage
		jsonErr := json.Unmarshal(p[:n], &reqData)

		if jsonErr != nil {
			fmt.Printf("Error in JSON format  %v", err)
			continue
		}
		//fmt.Println(reqData)
		if handleReq { //To enable or disable server request handling

			if reqData.Request == "APPEND_RPC" {
				previousTime = time.Now()
				//Empty response
				if currentServerType != Follower && reqData.Term > raftInfo.CurrentTerm {
					currentServerType = Follower
				}

				leaderInfo = reqData.Sender_name //Update LeaderInfo
				if reqData.Entries != "" && currentServerType != Leader {

					var leaderMsg jsonMessage
					leaderMsg.Sender_name = nodeName
					leaderMsg.Key = "SUCCESS"
					leaderMsg.Request = "APPEND_REPLY"

					remoteaddr.Port = 5555
					if err != nil {
						fmt.Printf("Couldn't send response %v", err)
					}

					if raftInfo.CurrentTerm > reqData.Term || reqData.PrevLogIndex > len(raftInfo.Log) {
						fmt.Println("Retry APPEND RPC FOR NODE", nodeName, "Index", reqData.PrevLogIndex, len(raftInfo.Log))
						leaderMsg.Value = "false"
					} else if reqData.PrevLogIndex < len(raftInfo.Log) {
						leaderMsg.Value = "false"
						if reqData.Term > raftInfo.Log[reqData.PrevLogIndex].Term {
							raftInfo.Log = raftInfo.Log[:reqData.PrevLogIndex]
						}
					} else {
						lastApplied++
						var logObj LogInfo
						logObj.Key = reqData.Key
						logObj.Value = reqData.Entries
						logObj.Term = reqData.Term
						raftInfo.Log = append(raftInfo.Log, logObj)
						leaderMsg.Value = "true"
						commitIndex++

					}
					jsonResp, _ := json.Marshal(leaderMsg)
					_, err = ser.WriteToUDP([]byte(jsonResp), remoteaddr)
				}
				raftInfo.CurrentTerm = reqData.Term // UPDATE THE TERM
			} else if reqData.Request == "APPEND_REPLY" {
				nodeID, _ := strconv.Atoi(strings.Split(reqData.Sender_name, "Node")[1])
				if currentServerType == Leader {
					if reqData.Value == "true" {
						//---IF LEN IS LOWER THAN THE CURRENT LOG
						if len(raftInfo.Log)-1 > nextIndex[nodeID-1] {
							nextIndex[nodeID-1]++
							log.Println("UPDATING APPEND RPC FOR NODE", nodeID, "Index", nextIndex[nodeID-1])
							retryAppendRPC(strings.Split(reqData.Sender_name, "Node")[1], nextIndex[nodeID-1])
						}
					} else {
						//DECREASE THE NEXT NODE INDEX AND RETRY
						nextIndex[nodeID-1] -= 2
						//Retry APPEND RPC
						go retryAppendRPC(strings.Split(reqData.Sender_name, "Node")[1], nextIndex[nodeID-1])
					}

				}
			} else if reqData.Request == "VOTE_REQUEST" {
				previousTime = time.Now()
				go sendVoteResponse(reqData, ser, remoteaddr)
			} else if reqData.Request == "VOTE_ACK" {
				if currentServerType != Candidate {
					continue
				}
				if reqData.Value == "true" {
					currentAcceptedVote += 1
					if currentAcceptedVote >= len(nodeListID)/2 {
						currentServerType = Leader
						currentAcceptedVote = 0
						currentRejectedVote = 0
						for k := 0; k < len(nodeListID); k++ {
							nextIndex[k] = len(raftInfo.Log) + 1
						}
						fmt.Println("-----Converted to LEADER-----")
						go sendHeartBeat() //TO reset all candidate nodes
					}
				} else {
					currentRejectedVote += 1
					if currentRejectedVote >= len(nodeListID)/2 {
						currentServerType = Follower
						currentAcceptedVote = 0
						currentRejectedVote = 0
					}
				}
				//fmt.Println(currentServerType)
			} else if reqData.Request == "STORE" {
				if currentServerType == Follower {
					var leaderMsg jsonMessage
					leaderMsg.Sender_name = nodeName
					leaderMsg.Key = "LEADER"
					leaderMsg.Value = leaderInfo
					leaderMsg.Request = "LEADER_INFO"
					jsonResp, _ := json.Marshal(leaderMsg)
					remoteaddr.Port = 5555
					//matchIndex[nodeID-1]++
					_, err = ser.WriteToUDP([]byte(jsonResp), remoteaddr)
					if err != nil {
						fmt.Printf("Couldn't send response %v", err)
					}
				} else if currentServerType == Leader {
					//raftInfo.CurrentTerm += 1
					var logObj LogInfo
					logObj.Term = raftInfo.CurrentTerm
					logObj.Key = reqData.Key
					logObj.Value = reqData.Value
					raftInfo.Log = append(raftInfo.Log, logObj)
					//----TODO SEND RPC CALL TO ALL NODES
					sendappendRPCRequest()
				}
			} else if reqData.Request == "RETRIEVE" {
				if false && currentServerType != Leader {
					//
					var leaderMsg jsonMessage
					leaderMsg.Sender_name = nodeName
					leaderMsg.Key = "LEADER"
					leaderMsg.Value = leaderInfo
					leaderMsg.Request = "LEADER_INFO"
					jsonResp, _ := json.Marshal(leaderMsg)
					remoteaddr.Port = 5555
					_, err = ser.WriteToUDP([]byte(jsonResp), remoteaddr)
					if err != nil {
						fmt.Printf("Couldn't send response %v", err)
					}
				} else {

					var leaderMsg jsonMessage
					leaderMsg.Sender_name = nodeName
					leaderMsg.Key = "COMMITED_LOGS"
					var resultArray []string
					for _, v := range raftInfo.Log {
						resultArray = append(resultArray, "{\"Key\":\""+v.Key+"\" ,\"Value\":\""+v.Value+"\" ,\"Term\""+strconv.Itoa(v.Term)+"}")
					}
					leaderMsg.Value = "[" + strings.Join(resultArray, ",") + "]"
					leaderMsg.Request = "RETRIEVE"
					jsonResp, _ := json.Marshal(leaderMsg)
					remoteaddr.Port = 5555
					_, err = ser.WriteToUDP([]byte(jsonResp), remoteaddr)
					if err != nil {
						fmt.Printf("Couldn't send response %v", err)
					}
				}
			}
		}
		//----ADD ALL CONTROLLER API HERE----
		if reqData.Request == "CONVERT_FOLLOWER" {
			fmt.Println("-----------Converting to FOLLOWER--------")
			previousTime = time.Now() // To reset timeout
			currentServerType = Follower
			handleReq = true
		} else if reqData.Request == "LEADER_INFO" {
			fmt.Println("-----------LEADER_INFO----------")
			var leaderMsg jsonMessage
			leaderMsg.Sender_name = nodeName
			leaderMsg.Key = "LEADER"
			leaderMsg.Value = leaderInfo
			leaderMsg.Request = "LEADER_INFO"
			jsonResp, _ := json.Marshal(leaderMsg)
			remoteaddr.Port = 5555
			_, err = ser.WriteToUDP([]byte(jsonResp), remoteaddr)
			if err != nil {
				fmt.Printf("Couldn't send response %v", err)
			}
		} else if reqData.Request == "SHUTDOWN" {
			handleReq = false
		} else if reqData.Request == "TIMEOUT" {
			previousTime = time.Time{}

		}

	}
}

func sendappendRPCRequest() {
	for _, v := range nodeListID {
		tmp, _ := strconv.Atoi(v) // tmp contains the ID
		v = "Node" + v
		if v == nodeName { //Skip if current node
			continue
		}
		nextIndex[tmp-1]++
		var msg jsonMessage
		msg.Sender_name = nodeName
		msg.Request = "APPEND_RPC"
		msg.Key = raftInfo.Log[len(raftInfo.Log)-1].Key
		msg.Entries = raftInfo.Log[len(raftInfo.Log)-1].Value
		msg.Term = raftInfo.CurrentTerm

		msg.PrevLogIndex = nextIndex[tmp-1] - 1 //Check nad remove one
		msg.PrevLogIndex = len(raftInfo.Log) - 1
		jsonReq, err2 := json.Marshal(msg)
		if err2 != nil {
			fmt.Println("ERRRORRRR", err2.Error())
		}
		go func(v string) {
			conn, err := net.Dial("udp", v+port)
			if err != nil {
				fmt.Printf("Some error %v", err)
				return
			}
			fmt.Fprintf(conn, string(jsonReq))
			if err == nil {
				//fmt.Printf("%s\n", p)
			} else {
				fmt.Printf("Some error %v %s\n", err)
			}
			conn.Close()
		}(v)

	}
}

func retryAppendRPC(nodeListID string, index int) {

	v := nodeListID
	tmp, _ := strconv.Atoi(v) // tmp contains the ID
	v = "Node" + v

	if index < 0 {
		index = 0
	}
	var msg jsonMessage
	msg.Sender_name = nodeName
	msg.Request = "APPEND_RPC"
	msg.Term = raftInfo.CurrentTerm
	msg.Key = raftInfo.Log[index].Key
	msg.Entries = raftInfo.Log[index].Value
	msg.PrevLogTerm = raftInfo.Log[index].Term // TODO CHECK

	msg.PrevLogIndex = nextIndex[tmp-1]
	msg.PrevLogIndex = index
	jsonReq, _ := json.Marshal(msg)
	conn, err := net.Dial("udp", v+port)
	if err != nil {
		fmt.Printf("Some error %v", err)
		return
	}
	fmt.Fprintf(conn, string(jsonReq))
	if err == nil {
		//fmt.Printf("%s\n", p)
	} else {
		fmt.Printf("Some error %v %s\n", err)
	}
	conn.Close()
}

func startVotingProcess() {
	//Send Vote Request
	fmt.Println("startVotingProcess")
	p := make([]byte, 2048)
	//	port := ""

	var msg jsonMessage
	msg.Sender_name = nodeName
	msg.Request = "VOTE_REQUEST"
	msg.Term = raftInfo.CurrentTerm
	msg.PrevLogIndex = len(raftInfo.Log) - 1
	jsonReq, _ := json.Marshal(msg)

	//loop to all node
	for _, v := range nodeListID {
		v = "Node" + v
		if v == nodeName { //Skip if current node
			continue
		}
		go func(v string) {
			conn, err := net.Dial("udp", v+port)
			if err != nil {
				fmt.Printf("Some error %v", err)
				return
			}
			fmt.Fprintf(conn, string(jsonReq))

			_, err = bufio.NewReader(conn).Read(p)
			if err == nil {
				//	fmt.Printf("%s\n", p)
			} else {
				fmt.Printf("Some error %v\n", err)
			}
			conn.Close()
		}(v)

	}
}

func sendHeartBeat() {
	p := make([]byte, 2048)
	//port := ""

	var msg jsonMessage
	msg.Sender_name = nodeName
	msg.Request = "APPEND_RPC"
	msg.Term = raftInfo.CurrentTerm
	jsonResp, _ := json.Marshal(msg)

	//loop to all node
	for _, v := range nodeListID {
		v = "Node" + v
		if v == nodeName { //Skip if current node
			continue
		}
		go func(v string) {

			conn, err := net.Dial("udp", v+port)
			if err != nil {
				fmt.Printf("Some error %v", err)
				return
			}
			fmt.Fprintf(conn, string(jsonResp))

			//_, err = bufio.NewReader(conn).Read(p)
			if err == nil {
				//fmt.Printf("%s\n", p)
			} else {
				fmt.Printf("Some error %v %s\n", err, p)
			}
			conn.Close()
		}(v)

	}

}

func sendVoteResponse(reqData jsonMessage, conn *net.UDPConn, addr *net.UDPAddr) {
	previousTime = time.Now()
	var voteRes jsonMessage
	voteRes.Sender_name = nodeName
	voteRes.Key = "Vote"
	voteRes.Request = "VOTE_ACK"
	//Check if term is lower or log index is lower
	if reqData.Term < raftInfo.CurrentTerm || (reqData.Term == raftInfo.CurrentTerm && len(raftInfo.Log)-1 > reqData.PrevLogIndex) {
		voteRes.Value = "false"
	} else {
		voteRes.Value = "true"
		raftInfo.VotedFor = addr.String()
		raftInfo.CurrentTerm = reqData.Term
	}

	voteRes.Term = raftInfo.CurrentTerm
	jsonResp, err := json.Marshal(voteRes)
	if err != nil {
		fmt.Printf("Couldn't create json  %v", err)
	}
	addr.Port = 5555
	_, err = conn.WriteToUDP([]byte(jsonResp), addr)
	if err != nil {
		fmt.Printf("Couldn't send response %v", err)
	}
}

//Hotel Reservation source code

func getReservation(w http.ResponseWriter, r *http.Request) {
	if currentServerType != Leader { //Redirect to leader node
		port := getLeaderPort()
		log.Println(("http://localhost:" + port))
		http.Redirect(w, r, "http://localhost:"+port, 303)
		return
	}
	jsonFile, _ := os.Open("./data.json")
	defer jsonFile.Close()
	byteValue, _ := ioutil.ReadAll(jsonFile)

	var result map[string]interface{}
	json.Unmarshal([]byte(byteValue), &result)
	w.Header().Set("Content-Type", "application/json")
	jsonResp, _ := json.Marshal(result)

	//send log all log
	var j jsonMessage
	j.Key = "NodeGETCommand"
	keys, _ := r.URL.Query()["RoomType"]

	j.Value = string("RoomType:" + keys[0])
	var logObj LogInfo
	logObj.Key = j.Key
	logObj.Value = j.Value
	logObj.Term = raftInfo.CurrentTerm
	raftInfo.Log = append(raftInfo.Log, logObj)
	sendappendRPCRequest()

	w.Write(jsonResp)

}

func checkandUpdateReservation(w http.ResponseWriter, r *http.Request) {
	if currentServerType != Leader {
		port := getLeaderPort()
		http.Redirect(w, r, "http://localhost:"+port, http.StatusSeeOther)
		return
	}
	fmt.Println("INSIDE CHECK N UPDATE")
	//NodeListID := [3]string{"http://node3:8080", "http://node2:8080", "http://node1:8080"}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}

	for i := 0; i < len(nodeListID); i++ {
		go func(i int) {
			req, err := http.NewRequest("POST", "http://Node"+nodeListID[i]+":8080"+"/updateNodes", bytes.NewBuffer([]byte(body)))
			req.Header.Set("Content-Type", "application/json")
			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				panic(err)
			}

			fmt.Println("response Status:", resp.Status)
		}(i)
	}

	//Update current Term and send to update all log
	var j jsonMessage
	j.Key = "NodeWriteCommand"
	j.Value = string(body)
	var logObj LogInfo
	logObj.Key = j.Key
	logObj.Value = j.Value
	logObj.Term = raftInfo.CurrentTerm
	raftInfo.Log = append(raftInfo.Log, logObj)
	sendappendRPCRequest()
}

func updateNodes(w http.ResponseWriter, r *http.Request) {
	if !handleReq {
		return
	}
	jsonFile, _ := os.Open("data.json")
	defer jsonFile.Close()
	byteValue, _ := ioutil.ReadAll(jsonFile)
	var rooms Rooms
	json.Unmarshal(byteValue, &rooms)

	rmType := rooms.Rooms

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}
	log.Println(string(body))
	var b Booking
	err = json.Unmarshal([]byte(body), &b)

	for i := 0; i < len(rmType); i++ {
		fmt.Println("King", rmType[i].Type)
		if rmType[i].Type != b.RoomType {
			continue
		}
		dates := rmType[i].Dates
		for j := 0; j < len(dates); j++ {
			if contains(b.Dates, dates[j]) {
				dates = append(dates[:j], dates[j+1:]...)
				break
			}
		}
		rmType[i].Dates = dates
	}
	rooms.Rooms = rmType
	w.Header().Set("Content-Type", "application/json")
	jsonResp, _ := json.Marshal(rooms)
	fmt.Println(rooms)
	w.Write(jsonResp)
	os.WriteFile("data.json", []byte(string(jsonResp)), 0644)

}

func rootHandler(w http.ResponseWriter, r *http.Request) {
	if currentServerType != Leader {
		port := getLeaderPort()
		log.Println(("http://localhost:" + port))
		http.Redirect(w, r, "http://localhost:"+port, 303)
		return
	}
	http.ServeFile(w, r, "./index.html")
}

//Add it to seprate util file
func contains(s []string, t string) bool {
	for _, a := range s {
		if a == t {
			return true
		}
	}
	return false
}

func getLeaderPort() string {
	switch leaderInfo {

	case "Node1":
		return "8080"
	case "Node2":
		return "8081"
	case "Node3":
		return "8082"
	case "Node4":
		return "8083"
	}
	return "8084"
}
