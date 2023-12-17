package maplejuice

import (
	"bufio"
	"crypto/rand"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"log"
	"math/big"
	"net"
	"os"

	mapleutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/MapleJuice/mapleJuiceUtils"
	gossiputils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	sdfsfuncs "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs"
	sdfsutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs/sdfsUtils"
)

func InitiateMaplePhase(localExecFile string, nMaples uint32, sdfsPrefix string, sdfsSrcDataset string, execFileArgs []string) {

	// locations, locationErr := sdfsclient.SdfsClientMain(SdfsSrcDataset)
	// if locationErr != nil {
	// 	fmt.Println("Error with sdfsclient main. Aborting Get command: ", locationErr)
	// 	return
	// }
	ipsToConnections := make(map[string]net.Conn)

	// sdfsclient.InitiateGetCommand(SdfsSrcDataset, SdfsSrcDataset, locations)
	mapleIps := GetMapleIps(nMaples)

	mapleTask := mapleutils.MapleJuiceTask{
		Type:              mapleutils.MAPLE,
		SdfsPrefix:        sdfsPrefix,
		SdfsExecFile:      localExecFile,
		NumberOfMJTasks:   nMaples,
		ExecFileArguments: execFileArgs,
	}

	filesRead := make([]*os.File, 0)

	sdfsFileNames := sdfsfuncs.InitiateLsWithPrefix(sdfsSrcDataset)
	for _, sdfsFile := range sdfsFileNames {
		blockLocations, locationErr := sdfsfuncs.SdfsClientMain(sdfsFile, true)
		if locationErr != nil {
			fmt.Println("Error with sdfsclient main. Aborting Get command: ", locationErr)
			return
		}
		log.Println(sdfsFile)
		randomHash, _ := GenerateRandomHash()
		sdfsfuncs.InitiateGetCommand(sdfsFile, randomHash+sdfsFile, blockLocations)

		fp := mapleutils.OpenFile(randomHash+sdfsFile, os.O_RDONLY)
		if fp == nil {
			continue
		}
		defer fp.Close()
		filesRead = append(filesRead, fp)

		for ipIdx, ip := range mapleIps {
			conn, err := sdfsutils.OpenTCPConnection(ip, mapleutils.MAPLE_JUICE_PORT)
			if err != nil {
				mapleIps[ipIdx] = "redo"
			}
			ipsToConnections[ip] = conn
			mapleTask.NodeDesignation = uint32(ipIdx)

			_, err = conn.Write(mapleTask.Marshal())
			if err != nil {
				mapleIps[ipIdx] = "redo"
			}
			_, err = conn.Write([]byte{'\n'})
			if err != nil {
				mapleIps[ipIdx] = "redo"
			}
			sdfsutils.ReadSmallAck(conn)
		}

		ipsToConnections = sendAllLinesInAFile(mapleIps, ipsToConnections, fp, mapleTask)
	}

	for {
		numFailures := 0
		maplesToRedo := make([]string, 0)

		for _, ip := range mapleIps {
			if ip == "redo" {
				numFailures++
				maplesToRedo = append(maplesToRedo, GetMapleIps(uint32(1))[0])
			} else {
				maplesToRedo = append(maplesToRedo, "")
			}
		}

		if numFailures != 0 {
			for _, fp := range filesRead {
				sendAllLinesInAFile(maplesToRedo, ipsToConnections, fp, mapleTask)
			}
		} else {
			break
		}
	}

	for _, conn := range ipsToConnections {
		conn.Close()
		fmt.Println("Closed connection")
	}

	tcpConn, listenError := sdfsutils.ListenOnTCPConnection(mapleutils.MAPLE_JUICE_ACK_PORT)
	if listenError != nil {
		fmt.Printf("Error listening on port %s", mapleutils.MAPLE_JUICE_ACK_PORT)
		return
	}
	defer tcpConn.Close()

	fmt.Println("maplejuiceack client is listening on local machine")

	numAcksRecieved := uint32(0)
	for {
		if numAcksRecieved == nMaples {
			break
		}
		conn, err := tcpConn.Accept()

		if err != nil {
			fmt.Println("Error accepting tcp connection:", err)
			continue
		}

		numAcksRecieved++
		conn.Close()
	}

	// Initiates the Maple phase via client command

	// 1. GET SdfsSrcDataset -> dataset.txt
	// 2. get an array of NMaples IPs from gossip memlist, call it MapleDsts
	// 3. For each line in dataset.txt,
	// 		4. i = hash(line) % NMaples. Also, need to save in append mode this line into a seperate file for later sending
	// 		5. SendMapleTask(MapleDsts[i], CreatedTask) // Need to think about this carefully
}

func sendAllLinesInAFile(mapleIps []string, ipsToConnections map[string]net.Conn, fp *os.File, mapleTask mapleutils.MapleJuiceTask) map[string]net.Conn {
	fp.Seek(0, 0)
	scanner := bufio.NewScanner(fp)

	numlines := 0
	for scanner.Scan() {
		line := scanner.Text()
		ip, ipIdx := getPartitionIp(line, mapleIps)
		if ip == "" || ip == "redo" {
			continue
		}
		_, exists := ipsToConnections[ip]
		log.Printf("Connection doesn't exist")
		if !exists {
			log.Printf(ip)
			conn, err := sdfsutils.OpenTCPConnection(ip, mapleutils.MAPLE_JUICE_PORT)
			if err != nil {
				mapleIps[ipIdx] = "redo"
			}
			ipsToConnections[ip] = conn
			mapleTask.NodeDesignation = ipIdx

			_, err = conn.Write(mapleTask.Marshal())
			if err != nil {
				mapleIps[ipIdx] = "redo"
			}
			_, err = conn.Write([]byte{'\n'})
			if err != nil {
				mapleIps[ipIdx] = "redo"
			}
			sdfsutils.ReadSmallAck(conn)
		}
		log.Printf("Connection exists")

		_, err := ipsToConnections[ip].Write([]byte(line))
		if err != nil {
			mapleIps[ipIdx] = "redo"
		}
		_, err = ipsToConnections[ip].Write([]byte{'\n'})
		if err != nil {
			mapleIps[ipIdx] = "redo"
		}
		numlines += 1
	}
	fmt.Printf("Read %d lines in maple task\n", numlines)
	return ipsToConnections
}

func GetMapleIps(nMaples uint32) []string {
	kRandomIpAddrs := gossiputils.RandomKIpAddrs(int(nMaples), true)
	return kRandomIpAddrs
}

func getPartitionIp(key string, ips []string) (string, uint32) {
	numPartitions := uint32(len(ips))
	ipIdx := hashFunction(key, numPartitions)
	return ips[ipIdx], ipIdx
}

func hashFunction(key string, numPartitions uint32) uint32 {
	hash := crc32.ChecksumIEEE([]byte(key))
	return uint32(hash) % numPartitions
}

// maple map_executable 1 prefix mapTestDir

func GenerateRandomHash() (string, error) {
	randomNum, err := rand.Int(rand.Reader, big.NewInt(100000))
	if err != nil {
		return "", err
	}

	randomStr := randomNum.String()

	hasher := fnv.New32()

	hasher.Write([]byte(randomStr))

	hashSum := hasher.Sum32() % 100000

	hashString := fmt.Sprintf("%05d", hashSum)

	return hashString, nil
}
