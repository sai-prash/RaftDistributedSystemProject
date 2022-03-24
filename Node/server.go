package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"time"
)

type RaftInfo struct {
	CurrentTerm int      `json: "CurrentTerm"`
	VotedFor    string   `json: "VotedFor"`
	Log         []string `json: "Log"`
	Timeout     int      `json: "Timeout"`
	HeartBeat   int      `json: "HeartBeat"`
}

type jsonMessage struct {
	Sender_name string `json: "sender_name"`
	Request     string `json: "request"` //Type of Request: APPEND_RPC/VOTE_REQUEST/VOTE_ACK
	Term        int    `json: "term"`
	Key         string `json: "key"`
	Value       string `json: "value"`
}

// type voteReq struct {
// 	Vote bool `json: "vote"`
// }

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

//var nodeList []string = []string{"127.0.0.1:5555", "127.0.0.1:5556"}

var nodeList []string = []string{"Node1", "Node2", "Node3", "Node4", "Node5"}

var currentAcceptedVote = 0
var currentRejectedVote = 0

func main() {
	// nodeList=["", "", "", "", ""]
	raftInfo = loadRaftJson()

	if nodeName == "" {
		nodeName = "127.0.0.1"
	}
	fmt.Println("---Node Name---" + nodeName)
	fmt.Println(raftInfo)

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
	handleRequest(ser, p)

	fmt.Println("\n\n\n---Server Shutdown---\n\n\n")

}

func loadRaftJson() RaftInfo {
	jsonFile, _ := os.Open("raft.json")
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
		sleep := 1000 * time.Millisecond
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

				if currentServerType == Leader && curTime.Sub(previousTime) > time.Duration(raftInfo.HeartBeat)*time.Millisecond {
					fmt.Println("Sending HeartBeat process")
					raftInfo.CurrentTerm += 1
					previousTime = time.Now()
					//Send HeartBeat
					sendHeartBeat()

				}

				if currentServerType != Leader && curTime.Sub(previousTime) > time.Duration(raftInfo.Timeout)*time.Millisecond {
					//Start Election Process:
					fmt.Println(curTime)
					fmt.Println(previousTime)
					fmt.Println(curTime.Sub(previousTime))
					fmt.Println(time.Duration(raftInfo.Timeout) * time.Millisecond)
					fmt.Println(curTime.Sub(previousTime) > time.Duration(raftInfo.Timeout)*time.Millisecond)
					fmt.Println("Starting election process")

					raftInfo.CurrentTerm += 1
					//Send VoteRequest to all Nodess
					previousTime = time.Now()
					if currentServerType != Candidate {
						currentServerType = Candidate
						go startVotingProcess()
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
		if handleReq { //To enable or disable server request handling
			n, remoteaddr, err := ser.ReadFromUDP(p)
			if err != nil {
				fmt.Printf("error  %v", err)
				continue
			}
			fmt.Printf("Read a message from %v %s \n", remoteaddr, p)
			var reqData jsonMessage
			fmt.Println(n)
			jsonErr := json.Unmarshal(p[:n], &reqData)
			if jsonErr != nil {
				fmt.Printf("Error in JSON format  %v", err)
				continue
			}

			if reqData.Request == "APPEND_RPC" {
				previousTime = time.Now()
				//Empty response
				if currentServerType != Follower && reqData.Term > raftInfo.CurrentTerm {
					currentServerType = Follower
				}
				//go sendHeartBeatResponse(reqData, ser, remoteaddr)
			} else if reqData.Request == "VOTE_REQUEST" {
				go sendVoteResponse(reqData, ser, remoteaddr)
			} else if reqData.Request == "VOTE_ACK" {
				fmt.Println("-----------------" + reqData.Value)
				fmt.Println(currentServerType)
				if currentServerType != Candidate {
					continue
				}
				if reqData.Value == "true" {
					currentAcceptedVote += 1
					fmt.Println("-----------------", currentAcceptedVote >= len(nodeList)/2)
					if currentAcceptedVote >= len(nodeList)/2 {
						currentServerType = Leader
						currentAcceptedVote = 0
						currentRejectedVote = 0
					}
				} else {
					currentRejectedVote += 1
					if currentRejectedVote >= len(nodeList)/2 {
						currentServerType = Follower
						currentAcceptedVote = 0
						currentRejectedVote = 0
					}
				}
				fmt.Println(currentServerType)
			} else if reqData.Request == "CONVERT_FOLLOWER" { //----ADD ALL CONTROLLER API HERE----
				currentServerType = Follower
			}

		}

	}
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
	jsonReq, _ := json.Marshal(msg)

	//loop to all node
	for _, v := range nodeList {
		if v == nodeName { //Skip if current node
			//continue
		}
		conn, err := net.Dial("udp", v+port)
		if err != nil {
			fmt.Printf("Some error %v", err)
			return
		}
		fmt.Fprintf(conn, string(jsonReq))

		_, err = bufio.NewReader(conn).Read(p)
		if err == nil {
			fmt.Printf("%s\n", p)
		} else {
			fmt.Printf("Some error %v\n", err)
		}
		conn.Close()
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
	for _, v := range nodeList {
		fmt.Println("Sending HearBeat to: " + v)
		if v == nodeName { //Skip if current node
			continue
		}
		conn, err := net.Dial("udp", v+port)
		if err != nil {
			fmt.Printf("Some error %v", err)
			return
		}
		fmt.Fprintf(conn, string(jsonResp))

		//_, err = bufio.NewReader(conn).Read(p)
		if err == nil {
			fmt.Printf("%s\n", p)
		} else {
			fmt.Printf("Some error %v\n", err)
		}
		conn.Close()
	}

}

func sendHeartBeatResponse(reqData jsonMessage, conn *net.UDPConn, addr *net.UDPAddr) {
	//Add heartBeat response if needed
}

func sendVoteResponse(reqData jsonMessage, conn *net.UDPConn, addr *net.UDPAddr) {
	var voteRes jsonMessage
	voteRes.Sender_name = nodeName
	voteRes.Key = "Vote"
	voteRes.Request = "VOTE_ACK"
	if reqData.Term >= raftInfo.CurrentTerm {
		voteRes.Value = "true"
	} else {
		voteRes.Value = "false"
	}
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