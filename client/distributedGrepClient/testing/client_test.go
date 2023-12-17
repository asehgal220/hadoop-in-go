package testing

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"regexp"
	"testing"
	"time"

	client "gitlab.engr.illinois.edu/asehgal4/cs425mps/client/distributedGrepClient"
)

type MachineData struct {
	ip            string
	MachineNumber int
}

func TestSendingData(t *testing.T) {

	print(os.Getwd())

	machineFile := "../../../logs/machine.txt"

	content, read_err := os.ReadFile(machineFile)
	if read_err != nil {
		fmt.Println("SERVER:99 = Error:", read_err)
		return
	}

	// Convert the file content to an integer
	machineNumber := string(content[0])

	var nodes = map[string]MachineData{
		"asehgal4@fa23-cs425-4901.cs.illinois.edu": {"172.22.156.162", 1},
		"asehgal4@fa23-cs425-4902.cs.illinois.edu": {"172.22.158.162", 2},
		"asehgal4@fa23-cs425-4903.cs.illinois.edu": {"172.22.94.162", 3},
		"asehgal4@fa23-cs425-4904.cs.illinois.edu": {"172.22.156.163", 4},
		"asehgal4@fa23-cs425-4905.cs.illinois.edu": {"172.22.158.163", 5},
		"asehgal4@fa23-cs425-4906.cs.illinois.edu": {"172.22.94.163", 6},
		"asehgal4@fa23-cs425-4907.cs.illinois.edu": {"172.22.156.164", 7},
		"asehgal4@fa23-cs425-4908.cs.illinois.edu": {"172.22.158.164", 8},
		"asehgal4@fa23-cs425-4909.cs.illinois.edu": {"172.22.94.164", 9},
		"asehgal4@fa23-cs425-4910.cs.illinois.edu": {"172.22.156.165", 10},
	}

	// Store current node
	currNode := "172.22.94.163"

	for node, MachineInfo := range nodes {
		if MachineInfo.ip == currNode {
			continue
		}
		startRemoteServer(node)
	}

	time.Sleep(time.Millisecond * 100)

	// Generate random log file
	generateLogFiles(machineNumber)

	// Set output of distributed grep command to be a local file
	origStdout := os.Stdout

	distributedLogFile, err := os.Create("results.log")
	if err != nil {
		for node, MachineInfo := range nodes {
			if MachineInfo.ip == currNode {
				continue
			}
			stopRemoteServer(node)
		}
		t.Fatalf("Unable to create log file: %v", err)
	}
	defer distributedLogFile.Close()

	os.Stdout = distributedLogFile

	// Send random sample log file to all servers
	localMachineSampleLog := fmt.Sprintf("../../../logs/machine.%s.log", machineNumber)
	for node, nodeInformation := range nodes {
		if nodeInformation.ip == currNode {
			continue
		}
		LogPath := fmt.Sprintf("/home/asehgal4/MPs/mp1/logs/machine.%d.log", nodeInformation.MachineNumber)

		cmd := exec.Command("scp", localMachineSampleLog, node+":"+LogPath)

		output, err := cmd.CombinedOutput()
		if err != nil {
			for node, machineInfo := range nodes {
				if machineInfo.ip == currNode {
					continue
				}
				stopRemoteServer(node)
			}
			t.Errorf("scp command failed with error: %v, output: %s", err, output)
		}
	}

	// Set arguments and run main function
	os.Args = []string{"go run client.go", "TRACE"}
	client.InitializeClient()

	// Execute the same grep command locally, storing the output in a buffer
	localGrepCommand := exec.Command("grep", "-rH", "TRACE", localMachineSampleLog)
	var localGrepOutput bytes.Buffer
	localGrepCommand.Stdout = &localGrepOutput
	err = localGrepCommand.Run()

	// Check if execution failed
	if err != nil {
		for node, machineInfo := range nodes {
			if machineInfo.ip == currNode {
				continue
			}
			stopRemoteServer(node)
		}
		log.Fatalf("Command failed with error: %v", err)
	}

	os.Stdout = origStdout

	distributedLogFile.Seek(0, 0)
	pattern := `^(\.\./\.\./\.\./logs/machine\.)(\d+)(\.log):` // Adjusted the pattern

	distributedResultOutput, err := fetchLinesStartingWithPattern(distributedLogFile, pattern)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	localResultOutput, err := fetchLinesStartingWithPattern(&localGrepOutput, pattern)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	for _, correctMachineOutput := range localResultOutput {
		if !compareOutputs(correctMachineOutput, distributedResultOutput) {
			t.Fatalf("Incorrect output")
		}
	}

	// Since same log file in all nodes, line count from the distributed command
	// should be equivalent to 10 times the local line count excluding the extraneous lines printed
	// detailing how many lines came from each server

	for node, MachineInfo := range nodes {
		if MachineInfo.ip == currNode {
			continue
		}
		stopRemoteServer(node)
	}
}

func compareOutputs(localOutput []string, distributedOutput map[string][]string) bool {
	for _, output := range distributedOutput {
		if len(localOutput) != len(output) {
			return false
		}
		for i := 0; i < len(localOutput); i++ {
			if localOutput[i] != output[i] {
				return false
			}
		}
	}
	return true
}

func fetchLinesStartingWithPattern(fileStream io.Reader, pattern string) (map[string][]string, error) {
	distributedOutput := make(map[string][]string)
	re := regexp.MustCompile(pattern)
	scanner := bufio.NewScanner(fileStream)
	for scanner.Scan() {
		line := scanner.Text()
		matchedLine := re.FindStringSubmatch(line)
		if matchedLine != nil {
			processedLine := re.ReplaceAllLiteralString(line, "")
			machineNumber := matchedLine[2]
			distributedOutput[machineNumber] = append(distributedOutput[machineNumber], processedLine)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return distributedOutput, nil
}

// Helper function to generate a random log file
func generateLogFiles(machineNumber string) {
	const entries = 800

	rand.Seed(time.Now().UnixNano())
	fileToCreate := fmt.Sprintf("../../../logs/machine.%s.log", machineNumber)
	file, err := os.Create(fileToCreate)
	if err != nil {
		fmt.Println("Unable to create log file:", err)
	}

	defer file.Close()
	logTypes := []string{"INFO", "WARNING", "TRACE", "ERROR"}
	messageTypes := []string{"Starting Application", "Application is too crazy", "Finding process in application", "Application deleted all files"}
	for i := 0; i < entries; i++ {
		timeStamp := time.Now().UTC()
		logType := logTypes[rand.Intn(4)]
		messageType := messageTypes[rand.Intn(4)]

		message := fmt.Sprintf("%s %s %s\n", timeStamp, logType, messageType)

		_, err := file.Write([]byte(message))
		if err != nil {
			fmt.Println("Unable to write to sample log:", err)
		}
	}
}

func startRemoteServer(address string) {
	// Prepare the command
	cmd := exec.Command("ssh", address, "cd /home/asehgal4/MPs/mp1/src/server && ./server")

	// Start the command
	err := cmd.Start()
	if err != nil {
		log.Fatalf("Failed to start the command: %v", err)
	}
}

func stopRemoteServer(address string) {
	cmd := exec.Command("ssh", address, "pkill server")

	// Start the command
	err := cmd.Run()
	if err != nil {
		log.Fatalf("Failed to run the command: %v", err)
	}
}
