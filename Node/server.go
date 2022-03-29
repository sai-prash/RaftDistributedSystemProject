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

var nodeList []string = []string{"node1", "node2", "node3", "node4", "node5"}

var currentAcceptedVote = 0
var currentRejectedVote = 0

var leaderInfo string

func main() {
	// nodeList=["", "", "", "", ""]

	//time.Sleep(100000 * time.Millisecond)
	raftInfo = loadRaftJson()

	if nodeName == "" {
		nodeName = "127.0.0.1"
	}
	fmt.Println("---Node Name---" + nodeName)
	fmt.Println(raftInfo)

	if nodeName == "Node1" {
		//time.Sleep(2000 * time.Millisecond)
		//raftInfo.Timeout += 1000
		//currentServerType = Leader
	}
	//Add random timeout
	raftInfo.Timeout += rand.Intn(100)
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
	http.HandleFunc("/index.html", serveFile2)
	http.HandleFunc("/getReservation", getReservation)
	http.HandleFunc("/makeReservation", checkandUpdateReservation)
	http.HandleFunc("/updateNodes", updateNodes)
	fmt.Println("Server started at port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))

	fmt.Println("\n\n\n---Server Shutdown---\n\n\n")

}

func loadRaftJson() RaftInfo {
	jsonFile, err := os.Open("raft.json")
	fmt.Println(err)
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
					fmt.Println("-----Starting HeartBeat process-----")
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
				raftInfo.CurrentTerm = reqData.Term
				leaderInfo = reqData.Sender_name //Update LeaderInfo
				//go sendHeartBeatResponse(reqData, ser, remoteaddr)
			} else if reqData.Request == "VOTE_REQUEST" {
				previousTime = time.Now()
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
						go sendHeartBeat() //TO reset all candidate nodes
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
				fmt.Println("-----------Converting to FOLLOWER--------")
				currentServerType = Follower
			} else if reqData.Request == "LEADER_INFO" {
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
				fmt.Printf("%s\n", p)
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
	for _, v := range nodeList {
		if v == nodeName { //Skip if current node
			continue
		}
		go func(v string) {
			fmt.Println("Sending HearBeat to: " + v)

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
		}(v)

	}

}

func sendHeartBeatResponse(reqData jsonMessage, conn *net.UDPConn, addr *net.UDPAddr) {
	//Add heartBeat response if needed
}

func sendVoteResponse(reqData jsonMessage, conn *net.UDPConn, addr *net.UDPAddr) {
	previousTime = time.Now()
	var voteRes jsonMessage
	voteRes.Sender_name = nodeName
	voteRes.Key = "Vote"
	voteRes.Request = "VOTE_ACK"
	voteRes.Term = raftInfo.CurrentTerm
	if reqData.Term > raftInfo.CurrentTerm {
		voteRes.Value = "true"
		raftInfo.VotedFor = addr.String()
		raftInfo.CurrentTerm = reqData.Term
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

//Hotel Reservation source code

func getReservation(w http.ResponseWriter, r *http.Request) {
	log.Println("dsdsdsdssdds")
	//connection()
	jsonFile, _ := os.Open("./data.json")
	defer jsonFile.Close()
	byteValue, _ := ioutil.ReadAll(jsonFile)

	var result map[string]interface{}
	json.Unmarshal([]byte(byteValue), &result)
	w.Header().Set("Content-Type", "application/json")
	jsonResp, _ := json.Marshal(result)
	w.Write(jsonResp)

}

func checkandUpdateReservation(w http.ResponseWriter, r *http.Request) {

	fmt.Println("INSIDE CHECK N UPDATE")
	NodeList := [3]string{"http://node3:8080", "http://node2:8080", "http://node1:8080"}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 3; i++ {
		fmt.Println(NodeList[i] + "/updateNodes")
		req, err := http.NewRequest("POST", NodeList[i]+"/updateNodes", bytes.NewBuffer([]byte(body)))
		req.Header.Set("Content-Type", "application/json")
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}

		fmt.Println("response Status:", resp.Status)
		fmt.Println("response Headers:", resp.Header)
	}

}

func updateNodes(w http.ResponseWriter, r *http.Request) {
	fmt.Println("------------Updating NODES----------")

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
	fmt.Println("---", b)

	for i := 0; i < len(rmType); i++ {
		fmt.Println("King", rmType[i].Type)
		if rmType[i].Type != b.RoomType {
			continue
		}
		dates := rmType[i].Dates
		fmt.Println("")
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

func serveFile(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "./index.html")
}

func serveFile2(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "./index.html")
}

func rootHandler(w http.ResponseWriter, r *http.Request) {

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
