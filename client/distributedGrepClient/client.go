package distributedgrepclient

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

var SERVER_TCP_PORT string = ":5432"
var CLIENT_TCP_PORT string = ":2345"
var MAX_TCP_DATA_LEN int = 80000000

var nodes map[string]int
var mu sync.Mutex

func InitializeClient() {
	if len(os.Args) != 2 {
		log.Println("Usage: go run client.go <grep pattern>")
		return
	}
	machineFile := "../logs/machine.txt"

	content, readErr := os.ReadFile(machineFile)
	if readErr != nil {
		log.Println("SERVER:99 = Error:", readErr)
		return
	}

	// Convert the file content to an integer
	machineNumber := string(content[0])
	grepPattern := os.Args[1]

	// HARDCODED IP ADDRESSES OF ALL MACHINES. TODO not sure if the ip addresses change if vm is powered off?
	nodes = make(map[string]int)
	nodes["172.22.156.162"] = 1
	nodes["172.22.158.162"] = 2
	nodes["172.22.94.162"] = 3
	nodes["172.22.156.163"] = 4
	nodes["172.22.158.163"] = 5
	nodes["172.22.94.163"] = 6
	nodes["172.22.156.164"] = 7
	nodes["172.22.158.164"] = 8
	nodes["172.22.94.164"] = 9
	nodes["172.22.156.165"] = 10

	// TODO seperate thread to execute grep on local file
	intValue, convertErr := strconv.Atoi(machineNumber)
	if convertErr != nil {
		fmt.Println("Error converting string to int:", convertErr)
		return
	}

	executeGrep(grepPattern, intValue)
	client(grepPattern, machineNumber)
}

func client(grepCommand string, machineNum string) {
	var wg sync.WaitGroup

	mu.Lock()
	for key, value := range nodes {
		if fmt.Sprint(value) != machineNum {

			wg.Add(1)

			go func(innerKey string) {
				sendGrepRequest(innerKey, grepCommand)
				wg.Done()
			}(key)
		}
	}
	mu.Unlock()

	wg.Wait()
	// fmt.Println("Reached end of client")
}

func handleTCPData(conn net.Conn) {
	r := bufio.NewReader(conn)
	chunkSize := 10000 * 1024 // 1 mB
	buffer := make([]byte, chunkSize)
	receivedData := ""
	totalBytesRead := 0
	defer conn.Close()

	for {
		bytesRead, readErr := r.Read(buffer)
		totalBytesRead += bytesRead
		if bytesRead == 0 || readErr == io.EOF {
			break
		}

		if readErr != nil {
			fmt.Println("Error in handling tcp data:", readErr)
			return
		}

		receivedData += string(buffer[:bytesRead])
	}

	remoteAddr := conn.RemoteAddr().String()
	host, _, _ := net.SplitHostPort(remoteAddr)
	ipaddr := host

	prettyPrint(receivedData, nodes[ipaddr])
}

func sendGrepRequest(targetServerAddr string, grepCommand string) {

	conn, dialErr := net.DialTimeout("tcp", targetServerAddr+SERVER_TCP_PORT, 1000*time.Millisecond)
	if dialErr != nil {

		mu.Lock()
		delete(nodes, targetServerAddr)
		mu.Unlock()
		fmt.Printf("%s\n", dialErr.Error())
		return
	}
	defer conn.Close()

	message := []byte(grepCommand)
	_, writeErr := conn.Write(message)
	if writeErr != nil {
		fmt.Println("CLIENT:78 = Error sending:", writeErr)
		return
	}

	handleTCPData(conn)
}

func executeGrep(grepPattern string, machineNum int) {
	execQuery := fmt.Sprintf("grep -rH %s ../logs/machine.%d.log", grepPattern, machineNum)
	cmd := exec.Command("bash", "-c", execQuery)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()

	var data string

	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			data = ""
		} else {
			fmt.Println("Command failed:", err)
		}
	} else {
		data = stdout.String()
	}

	prettyPrint(data, machineNum)
}

func prettyPrint(receivedData string, machineNum int) {
	numLines := strings.Count(receivedData, "\n")

	mu.Lock()
	fmt.Printf("Machine %d sent %d lines:\n%s\n", machineNum, numLines, receivedData)
	mu.Unlock()
}
