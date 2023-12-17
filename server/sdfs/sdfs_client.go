package sdfs

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"os"
	"time"

	gossipUtils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	gossiputils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	utils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs/sdfsUtils"
)

func GetFileSizeByPrefix(prefix string) (uint32, error) {

	var task utils.Task
	task.DataTargetIp = utils.New19Byte("127.0.0.1")
	task.AckTargetIp = utils.New19Byte("127.0.0.1")
	task.OriginalFileSize = 0
	task.BlockIndex = 0
	task.DataSize = 0
	task.ConnectionOperation = utils.SIZE_BY_PREFIX
	task.FileName = utils.New1024Byte(prefix)
	task.IsAck = true
	task.AckTargetIp = utils.New19Byte("127.0.0.1")

	conn := utils.SendAckToMaster(task)
	defer (*conn).Close()

	buf := make([]byte, 4)
	_, err := (*conn).Read(buf)
	if err != nil {
		return 0, err
	}

	// Convert the byte slice to a uint32
	totalSize := binary.BigEndian.Uint32(buf)

	return totalSize, nil
}

func RequestBlockMappings(fileName string) ([][]string, error) {
	// 1. Create a task with the GET_2D block operation, and send to current master. If timeout/ doesn't work, send to 1st submaster, second, and so on.
	// 2. Listen for 2d array on responding connection. Read 2d array and return it.

	var task utils.Task
	task.DataTargetIp = utils.New19Byte("127.0.0.1")
	task.AckTargetIp = utils.New19Byte("127.0.0.1")
	task.OriginalFileSize = 0
	task.BlockIndex = 0
	task.DataSize = 0
	task.ConnectionOperation = utils.GET_2D
	task.FileName = utils.New1024Byte(fileName)
	task.IsAck = true
	task.AckTargetIp = utils.New19Byte("127.0.0.1")

	conn := utils.SendAckToMaster(task)
	defer (*conn).Close()
	locations, err := utils.UnmarshalBlockLocationArr(*conn)

	if err != nil {
		return nil, err // Returning an empty array on failure case
	}

	return locations, nil
}

// Client main function, becomes the entry point for all client operations. This function continuously requests from the master until an operation is finished
func SdfsClientMain(sdfsFilename string, waitForUpdate bool) ([][]string, error) {
	var blockLocationArr [][]string
	workInProgress := true
	var blockErr error

	for workInProgress {
		blockLocationArr, blockErr = RequestBlockMappings(sdfsFilename)

		if blockErr != nil {
			fmt.Println("Could not fetch block locations from master in client main")
			return blockLocationArr, blockErr
		} else if len(blockLocationArr) == 0 {
			fmt.Println("File name dne. Returning empty array")
			return blockLocationArr, nil
		}

		workInProgress = false
		for i := range blockLocationArr {
			for j := range blockLocationArr[i] {
				if blockLocationArr[i][j] == utils.WRITE_OP || blockLocationArr[i][j] == utils.DELETE_OP {
					fmt.Printf("Found WIP Op here: %d %d \n", i, j)
					workInProgress = waitForUpdate
					break
				}
			}
		}

		if workInProgress {
			time.Sleep(time.Millisecond * 250)
			fmt.Println("WAITING DURING UPDATE")
		} else {
			break
		}
	}

	return blockLocationArr, nil
}

func InitiatePutCommand(localFilename string, sdfsFilename string) {
	// 1. Determine the number of blocks that need to be created
	// 2. Randomly select four replica servers for each block
	// 3. Shard the block and send the data to each replica
	fmt.Printf("localFilename: %s sdfs: %s\n", localFilename, sdfsFilename)

	start := time.Now() // Record the start time

	// IF CONNECTION CLOSES WHILE WRITING, WE NEED TO REPICK AN IP ADDR. Can have a seperate function to handle this on failure cases.
	// Ask master when its ok to start writing

	fileSize, _ := utils.GetFileSize(localFilename)

	numberBlocks := utils.CeilDivide(fileSize, utils.BLOCK_SIZE)

	fmt.Println("Num blocks:", numberBlocks)
	fmt.Println("file size:", fileSize)
	fmt.Println("block size:", int64(utils.BLOCK_SIZE))
	for currentBlock := int64(0); currentBlock < numberBlocks; currentBlock++ {
		allMemberIps := gossipUtils.MembershipMap.Keys()
		remainingIps := utils.CreateConcurrentStringSlice(allMemberIps)

		file, err := os.Open(localFilename)

		if err != nil {
			log.Fatalf("error opening local file: %v\n", err)
		}
		startIdx, lengthToWrite := utils.GetBlockPosition(currentBlock, fileSize)

		for currentReplica := int64(0); currentReplica < utils.REPLICATION_FACTOR; currentReplica++ {
			fmt.Printf("start index: %d length to write: %d\n", startIdx, lengthToWrite)

			for {
				if remainingIps.Size() == 0 {
					break
				}

				if ip, ok := remainingIps.PopRandomElement().(string); ok {
					member, _ := gossipUtils.MembershipMap.Get(ip)

					if ip == gossipUtils.Ip || member.State == gossipUtils.DOWN {
						continue
					}

					connIp, err := utils.OpenTCPConnection(ip, utils.SDFS_PORT)
					if err != nil {
						log.Fatalf("error opening follower connection: %v\n", err)
						continue
					}
					blockWritingTask := utils.Task{
						DataTargetIp:        utils.New19Byte(ip),
						AckTargetIp:         utils.New19Byte(utils.LEADER_IP),
						ConnectionOperation: utils.WRITE,
						FileName:            utils.New1024Byte(sdfsFilename),
						OriginalFileSize:    fileSize,
						BlockIndex:          currentBlock,
						DataSize:            lengthToWrite,
						IsAck:               false,
					}
					fmt.Printf("start index: %d length to write: %d\n", startIdx, lengthToWrite)
					fmt.Printf("Expecting size of: %d\n", blockWritingTask.DataSize)

					marshalledBytesWritten, writeError := connIp.Write(blockWritingTask.Marshal())
					connIp.Write([]byte{'\n'})
					if writeError != nil {
						log.Fatalf("Could not write struct to connection in client put: %v\n", writeError)
					}

					utils.ReadSmallAck(connIp)

					totalBytesWritten, writeErr := utils.BufferedWriteToConnection(connIp, file, lengthToWrite, startIdx)
					fmt.Println("------BYTES_WRITTEN------: ", totalBytesWritten)
					fmt.Println("------BYTES_WRITTEN marshalled------: ", marshalledBytesWritten)

					if writeErr != nil { // If failure to write full block, redo loop
						fmt.Println("connection broke early, rewrite block: ", writeErr)
						continue
					}
					utils.ReadSmallAck(connIp)

					break
				}
			}
		}
		file.Close()
	}
	elapsed := time.Since(start) // Calculate the elapsed time

	fmt.Println("INIT PUT COMMAND TOOK :", elapsed.Seconds())
}

func InitiateGetCommand(sdfsFilename string, localFilename string, blockLocationArr [][]string) {
	// 1. Get the locations of all the blocks for a file from the master
	// 2. Open a tcp connection between the client and a random replica storing each block
	// 3. Get the data for each block and store it in the local file
	fmt.Printf("localFilename: %s sdfs: %s\n", localFilename, sdfsFilename)

	var fp *os.File = nil

	log.Println("Unmarshalled block location arr: ", blockLocationArr)

	sdfsFileDataExists := false
	for blockIdx, replicas := range blockLocationArr {
		for {
			randomReplicaIp, err := PopRandomElementInArray(&replicas)
			if err != nil {
				// log.Fatalf("All replicas down ):")
				break
			}
			if randomReplicaIp == "w" {
				continue
			}
			if fp == nil {
				fp, err = os.OpenFile(localFilename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
				if err != nil {
					log.Fatalln("unable to open local file, ", err)
				}
			}
			sdfsFileDataExists = true
			task := utils.Task{
				DataTargetIp:        utils.New19Byte(randomReplicaIp),
				AckTargetIp:         utils.New19Byte(gossipUtils.Ip),
				ConnectionOperation: utils.READ,
				FileName:            utils.New1024Byte(sdfsFilename),
				OriginalFileSize:    0,
				BlockIndex:          int64(blockIdx),
				DataSize:            0,
				IsAck:               false,
			}
			replicaConn, err := utils.OpenTCPConnection(randomReplicaIp, utils.SDFS_PORT)
			if err != nil {
				log.Printf("unable to connect to replica, ", err)
				continue
			}
			err = utils.SendTaskOnExistingConnection(task, replicaConn)
			if err != nil {
				log.Printf("unable to send task to replica, ", err)
				continue
			}

			utils.ReadSmallAck(replicaConn)
			log.Printf("Unmarshaling task\n")

			blockMetadata, _ := utils.Unmarshal(replicaConn)
			utils.SendSmallAck(replicaConn)

			log.Printf("Number of bytes to read from connection: ", blockMetadata.DataSize)
			utils.BufferedReadFromConnection(replicaConn, fp, blockMetadata.DataSize)
			replicaConn.Close()
			// utils.BufferedReadAndWrite(replicaConnBuf, fp, blockMetadata.DataSize, false, 0)
			break
		}
	}

	if sdfsFileDataExists == false {
		log.Printf("sdfs file doesn't exist")
	}
}

func PutBlock(sdfsFilename string, blockIdx int64, ipDst string, originalFileSize int64) {
	fmt.Println("Entering put block")
	_, fileSize, fp, err := utils.GetFilePtr(sdfsFilename, fmt.Sprint(blockIdx), os.O_RDONLY)
	if err != nil {
		fmt.Println("Couldn't get file pointer", err)
	}

	blockWritingTask := utils.Task{
		DataTargetIp:        utils.New19Byte(ipDst),
		AckTargetIp:         utils.New19Byte(utils.LEADER_IP),
		ConnectionOperation: utils.WRITE,
		FileName:            utils.New1024Byte(sdfsFilename),
		OriginalFileSize:    originalFileSize,
		BlockIndex:          blockIdx,
		DataSize:            int64(fileSize),
		IsAck:               false,
	}

	member, ok := gossipUtils.MembershipMap.Get(ipDst)
	if ipDst == gossipUtils.Ip || !ok || member.State == gossipUtils.DOWN {
		return
	}
	fmt.Println("Got member from ip target")

	conn, err := utils.OpenTCPConnection(ipDst, utils.SDFS_PORT)
	if err != nil {
		fmt.Printf("error opening follower connection: %v\n", err)
		return
	}
	fmt.Println("Opened connection to replication target")

	marshalledBytesWritten, writeError := conn.Write(blockWritingTask.Marshal())
	conn.Write([]byte{'\n'})
	if writeError != nil {
		fmt.Printf("Could not write struct to connection in client put: %v\n", writeError)
	}

	utils.ReadSmallAck(conn)
	fmt.Println("Read small ack in put block")

	startIdx := blockIdx * utils.BLOCK_SIZE
	totalBytesWritten, writeErr := utils.BufferedWriteToConnection(conn, fp, int64(fileSize), startIdx)
	fmt.Println("------BYTES_WRITTEN------: ", totalBytesWritten)
	fmt.Println("------BYTES_WRITTEN marshalled------: ", marshalledBytesWritten)

	if writeErr != nil { // If failure to write full block, redo loop
		fmt.Println("connection broke early, rewrite block: ", writeErr)
		return
	}
	utils.ReadSmallAck(conn)
	fmt.Println("Read another small ack in put block")
}

func InitiateDeleteCommand(sdfsFilename string, mappings [][]string) {
	// 1. Get location of all blocks from leader
	// 2. Send delete requests to each replica
	fmt.Printf("sdfs: %s\n", sdfsFilename)
	// IF CONNECTION CLOSES WHILE READING, its all good. We can assume memory was wiped

	var task utils.Task
	task.IsAck = false
	task.ConnectionOperation = utils.DELETE
	task.FileName = utils.New1024Byte(sdfsFilename)

	for i := 0; i < len(mappings); i++ {
		for j := 0; j < len(mappings[i]); j++ {

			if mappings[i][j] == utils.WRITE_OP || mappings[i][j] == utils.DELETE_OP {
				continue
			}

			// Create a delete task struct, with master as ack target, and send to ip addr.
			blockIp := mappings[i][j]
			member, ok := gossipUtils.MembershipMap.Get(blockIp)
			if !ok || member.State == gossiputils.DOWN {
				fmt.Printf("No need to delete on node %s, it's already down. Continuing...", blockIp)
				continue
			}

			task.BlockIndex = int64(i)

			conn, err := utils.OpenTCPConnection(blockIp, utils.SDFS_PORT)
			if err != nil {
				log.Fatalf("Couldn't open tcp conn to leader %v\n", err)
			}
			buffConn := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

			data := task.Marshal()
			_, errMWrite := buffConn.Write(data)
			buffConn.Write([]byte{'\n'})
			buffConn.Flush()

			if errMWrite != nil {
				log.Fatalf("Couldn't write marshalled delete task %v\n", errMWrite)
			}

			fmt.Println("Finished delete task")
		}
	}
}

func InitiateLsCommand(mappings [][]string) {
	fmt.Println(mappings)
}

func InitiateLsWithPrefix(sdfsPrefix string) []string {
	// Returns a list of files with the matching prefix
	var recvData []string
	var task utils.Task
	task.ConnectionOperation = utils.GET_PREFIX
	task.FileName = utils.New1024Byte(sdfsPrefix)
	task.IsAck = true

	conn := utils.SendAckToMaster(task)
	decoder := json.NewDecoder(*conn)
	err := decoder.Decode(&recvData)
	if err != nil {
		fmt.Println("Error decoding data:", err)
		return recvData
	}

	return recvData
}

func InitiateStoreCommand() {
	items, _ := ioutil.ReadDir(utils.FILESYSTEM_ROOT)
	for _, item := range items {
		if !item.IsDir() {
			fmt.Println(item.Name())
		}
	}

}

func PopRandomElementInArray(array *[]string) (string, error) {
	// Get a random index using crypto/rand
	if len(*array) == 0 {
		return "", errors.New("No more elements to pop")
	}
	max := big.NewInt(int64(len(*array)))
	randomIndexBig, err := rand.Int(rand.Reader, max)
	if err != nil {
		panic(err)
	}
	randomIndex := randomIndexBig.Int64()

	randomElement := (*array)[randomIndex]
	*array = append((*array)[:randomIndex], (*array)[randomIndex+1:]...)
	return randomElement, nil
}

func InitiateMultiRead(fileName string, ipsToInitiate []string) {
	for _, ip := range ipsToInitiate {
		task := utils.Task{
			DataTargetIp:        utils.New19Byte(""),
			AckTargetIp:         utils.New19Byte(""),
			ConnectionOperation: utils.FORCE_GET, // READ, WRITE, GET_2D, OR DELETE from sdfs utils
			FileName:            utils.New1024Byte(fileName),
			OriginalFileSize:    0,
			BlockIndex:          0,
			DataSize:            0,
			IsAck:               false,
		}

		_, err := utils.SendTask(task, ip, false)
		if err != nil {
			log.Printf("Failed to send task on multiread with error: ", err)
		}
	}
}
