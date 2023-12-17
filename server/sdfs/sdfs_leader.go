package sdfs

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"regexp"

	cmap "github.com/orcaman/concurrent-map/v2"
	gossiputils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	utils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs/sdfsUtils"
)

var BlockLocations cmap.ConcurrentMap[string, [][]string] = cmap.New[[][]string]()           // filename : [[ip addr, ip addr, ], ], index 2d arr by block index
var FileToOriginator cmap.ConcurrentMap[string, []string] = cmap.New[[]string]()             // filename : [ClientIpWhoCreatedFile, ClientCreationTime]
var FileToBlocks cmap.ConcurrentMap[string, [][2]interface{}] = cmap.New[[][2]interface{}]() // IPaddr : [[blockidx, filename]]
var FileToSize cmap.ConcurrentMap[string, int64] = cmap.New[int64]()                         // sdfsfilename : size

// Initializes a new entry in BlockLocations, so the leader can begin listening for block acks.
func InitializeBlockLocationsEntry(fileName string, fileSize int64) {
	n, m := utils.CeilDivide(fileSize, utils.BLOCK_SIZE), utils.REPLICATION_FACTOR // Size of the 2D array (n rows, m columns)
	newEntry := make([][]string, n)                                                // Create a slice of slices (2D array)

	// Populate the 2D array with arbitrary values
	var i int64
	for i = 0; i < n; i++ {
		newEntry[i] = make([]string, m)
		for j := int64(0); j < m; j++ {
			// Assign sentinal values to the 2D array
			newEntry[i][j] = utils.WRITE_OP
		}
	}

	BlockLocations.Set(fileName, newEntry)
}

// Master functions
func RouteToSubMasters(incomingAck utils.Task) {
	// Route an incoming ack that makes a change to the membership list to the submasters.(Bully git issue) put test/500mb.txt 500
	kLeaders := gossiputils.GetKLeaders()
	for _, leader := range kLeaders {
		if leader != gossiputils.Ip {
			utils.SendTask(incomingAck, leader, true)
		}
	}
}

func HandleAck(incomingAck utils.Task, conn *net.Conn) error {

	if !incomingAck.IsAck {
		return errors.New("ack passed to master for processing was not actually an ack")
	}

	fileName := utils.BytesToString(incomingAck.FileName[:])
	ackSourceIp := utils.BytesToString(incomingAck.AckTargetIp[:])

	var arr []byte
	BlockLocations.UnmarshalJSON(arr)
	fmt.Println("Got ack for delete, block locations are ", string(arr))

	if incomingAck.ConnectionOperation == utils.WRITE {

		fmt.Printf("Got ack for write, the ack source is >{%s}<\n", ackSourceIp)
		fmt.Println("Got ack for write, filename is ", fileName)
		fmt.Println("Got ack for write, File size is ", incomingAck.OriginalFileSize)

		FileToSize.Set(fileName, incomingAck.OriginalFileSize)

		if !BlockLocations.Has(fileName) {
			fmt.Println("Never seen before filename, creating block locations entry")
			InitializeBlockLocationsEntry(fileName, incomingAck.OriginalFileSize)
		}

		blockMap, _ := BlockLocations.Get(fileName)
		if incomingAck.BlockIndex >= int64(len(blockMap)) {
			fmt.Println("Map was not properly initiated. Actual blockmap: ", blockMap)
			return errors.New("Map was not properly initiated, didn't have enough rows")
		}

		fmt.Println("Block map for file in write ack:", blockMap)
		for i := int64(0); i < (utils.REPLICATION_FACTOR); i++ {

			if i >= (int64(len(blockMap[incomingAck.BlockIndex]))) {
				fmt.Println("Map was not properly initiated. blockmap row: ", blockMap[incomingAck.BlockIndex])
				break
			}

			if blockMap[incomingAck.BlockIndex][i] == utils.WRITE_OP || blockMap[incomingAck.BlockIndex][i] == utils.DELETE_OP {
				blockMap[incomingAck.BlockIndex][i] = ackSourceIp
				break
			}
		}
		BlockLocations.Set(fileName, blockMap)
		fmt.Println("Set block locations")

		if mapping, ok := FileToBlocks.Get(ackSourceIp); ok { // IPaddr : [[blockidx, filename]]
			fmt.Println("Mapping ok, appending new value")
			mapping = append(mapping, [2]interface{}{incomingAck.BlockIndex, fileName})
			fmt.Println("Appending a new file+blockidx for ip addr ", ackSourceIp)
			fmt.Println("mapping: ", mapping)
			FileToBlocks.Set(ackSourceIp, mapping)
		} else {
			fmt.Println("Mapping not ok, creating new value")
			initialMapping := make([][2]interface{}, 1)
			initialMapping[0] = [2]interface{}{incomingAck.BlockIndex, fileName}

			fmt.Println("Creating a new file+blockidx for ip addr ", ackSourceIp)
			fmt.Println("mapping: ", initialMapping)

			FileToBlocks.Set(ackSourceIp, initialMapping)
		}
	} else if incomingAck.ConnectionOperation == utils.GET_2D {
		Handle2DArrRequest(fileName, *conn)
	} else if incomingAck.ConnectionOperation == utils.DELETE {

		fmt.Printf("Got ack for delete, the ack source is >{%s}<\n", ackSourceIp)
		fmt.Println("Got ack for delete, filename is ", fileName)
		fmt.Println("Got ack for delete, File size is ", incomingAck.OriginalFileSize)

		FileToSize.Remove(fileName)

		if !BlockLocations.Has(fileName) {
			return errors.New("Never seen before filename, dropping delete operation")
		}

		blockMap, _ := BlockLocations.Get(fileName)

		if incomingAck.BlockIndex >= int64(len(blockMap)) {
			fmt.Println("Block index exceeds block map length")
		} else {
			row := blockMap[incomingAck.BlockIndex]
			for i := int64(0); i < int64(len(row)); i++ {
				if row[i] == ackSourceIp {
					blockMap[incomingAck.BlockIndex][i] = utils.DELETE_OP
				}
			}
		}

		allDeleted := true
		for i := int64(0); i < int64(len(blockMap)); i++ {
			for j := int64(0); j < int64(len(blockMap[i])); j++ {
				if blockMap[i][j] != utils.DELETE_OP {
					allDeleted = false
				}
			}
		}

		if allDeleted {
			BlockLocations.Remove(fileName)
		}

		if mapping, ok := FileToBlocks.Get(ackSourceIp); ok && len(mapping) > 0 { // IPaddr : [[blockidx, filename]]
			var idx uint

			for i, pair := range mapping {
				if pair[0] == int(incomingAck.BlockIndex) && pair[1] == utils.BytesToString(incomingAck.FileName[:]) {
					idx = uint(i)
					break
				}
			}

			fmt.Println("Mapping before delete: ", mapping)
			mapping = append(mapping[:idx], mapping[idx+1:]...)
			fmt.Println("Mapping after delete: ", mapping)

			FileToBlocks.Set(ackSourceIp, mapping)
		}
	} else if incomingAck.ConnectionOperation == utils.GET_PREFIX {
		err := HandleGetPrefixList(fileName, conn)
		if err != nil {
			return err
		}

		fmt.Println("Successfully handled get prefix request")
	} else if incomingAck.ConnectionOperation == utils.SIZE_BY_PREFIX {
		err := HandleGetFileSizeByPrefix(fileName, conn)
		if err != nil {
			return err
		}

		fmt.Println("Successfully handled get size by prefix")
	}

	// 1. Ack for Write operation
	// 		1.a. Forward ack to submaster
	// 		1.b. Navigate to entry in fname:2darr mapping given the IncomingAck.filename and IncomingAck.blockidx, and src IP from IncomingAck.DataTargetIp, and add ip. Ensure there is a WRITE_OP at that location.
	// 		1.c If filename -> 2d arr mapping does not exist, initialize it with empty 2d arr, and add rows based on block idx. For acks that have not arrived, set those entires as WRITE_OPs.
	// 2. Ack for Delete operation
	// 		2.a. Forward ack to submaster
	// 		2.b. Navigate to entry in fname:2darr mapping given the IncomingAck.filename and IncomingAck.blockidx, and src IP from IncomingAck.DataTargetIp, and delete IP. Replace deleted IP with DELETE_OP constant.

	return nil
}

func PrefixList(SdfsPrefix string) ([]string, error) {
	pattern := SdfsPrefix + "*"

	// Compile the regular expression
	regex, err := regexp.Compile(pattern)
	var rv []string
	if err != nil {
		return rv, err
	}

	fmt.Println(BlockLocations.Keys())
	for _, val := range BlockLocations.Keys() {
		if regex.MatchString(val) {
			rv = append(rv, val)
		}
	}

	return rv, nil
}

func HandleGetFileSizeByPrefix(SdfsPrefix string, conn *net.Conn) error {
	rv, err := PrefixList(SdfsPrefix)
	if err != nil {
		return err
	}

	sizes := int64(0)
	for _, val := range rv {
		size, exists := FileToSize.Get(val)
		if exists {
			sizes += size
		}
	}

	sizesUint := uint32(sizes)
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, sizesUint)

	_, err = (*conn).Write(buf)
	if err != nil {
		return err
	}

	return nil
}

func HandleGetPrefixList(SdfsPrefix string, conn *net.Conn) error {
	rv, err := PrefixList(SdfsPrefix)
	if err != nil {
		return err
	}

	fmt.Println(rv)
	encoder := json.NewEncoder(*conn)
	err = encoder.Encode(rv)
	if err != nil {
		fmt.Println("Error encoding and sending data:", err)
		return err
	}

	return nil
}

func Handle2DArrRequest(Filename string, conn net.Conn) {
	// Reply to a connection with the 2d array for the provided filename.
	arr, exists := BlockLocations.Get(Filename)
	allDs := true
	for i := 0; i < len(arr); i++ {
		for j := 0; j < len(arr[i]); j++ {
			if arr[i][j] != utils.DELETE_OP {
				allDs = false
				break
			}
		}
		if !allDs {
			break
		}
	}

	if allDs {
		BlockLocations.Remove(Filename)
		fmt.Printf("Block location filename %s made dne. Continuing\n", Filename)
		var empty [][]string
		arr = empty
	}

	if !exists {
		fmt.Printf("Block location filename %s dne. Continuing\n", Filename)
	}

	returningArr := make([][]string, 0)
	for i := 0; i < len(arr); i++ {
		replicaArr := make([]string, 0)
		for j := 0; j < len(arr[i]); j++ {
			if arr[i][j] != utils.WRITE_OP && arr[i][j] != utils.DELETE_OP {
				replicaArr = append(replicaArr, arr[i][j])
			}
		}
		returningArr = append(returningArr, replicaArr)
	}

	marshalledArray := utils.MarshalBlockLocationArr(returningArr)
	_, err := conn.Write(marshalledArray)
	if err != nil {
		log.Fatalf("Error writing 2d arr to conn: %v\n", err)
	}
}

func HandleDown(DownIpAddr string) {
	// Remove IP addr from BlockLocations. set all with a 'd'.
	for keyval := range BlockLocations.IterBuffered() {
		sdfsFilename := keyval.Key
		blockLocations := keyval.Val

		for blockIdx := range blockLocations {
			for i := 0; i < int(utils.REPLICATION_FACTOR); i++ {
				if blockLocations[blockIdx][i] == DownIpAddr {
					blockLocations[blockIdx][i] = utils.WRITE_OP
				}
			}
		}

		BlockLocations.Set(sdfsFilename, blockLocations)
	}

	// Remove IP addr from FileToblocks. Delete all entries associated.
	FileToBlocks.Pop(DownIpAddr)
}

func HandleReReplication(downIpAddr string) {

	fmt.Println("Entering re replication. DOWN IP ADDRESS: ", downIpAddr)

	if blocksToRereplicate, ok := FileToBlocks.Get(downIpAddr); ok {

		fmt.Println("Handling re-replication on blocks: ", blocksToRereplicate)

		for _, blockMetadata := range blocksToRereplicate {

			if fileName, ok := blockMetadata[1].(string); ok {
				fmt.Println("Handling re-replication file: ", fileName)

				if blockIdx, ok := blockMetadata[0].(int64); ok {
					fmt.Println("Handling re-replication block: ", blockIdx)

					if blockLocations, ok := BlockLocations.Get(fileName); ok {
						if blockIdx >= int64(len(blockLocations)) {
							continue
						}
						fmt.Println("Block locations for found at", blockLocations[blockIdx])

						locations := blockLocations[blockIdx]
						for i, ip := range locations {
							if ip == downIpAddr || ip == utils.WRITE_OP || ip == utils.DELETE_OP {
								if ip == downIpAddr {
									blockLocations[blockIdx][i] = utils.WRITE_OP
								}
								BlockLocations.Set(fileName, blockLocations)
								continue
							}

							allIps := gossiputils.MembershipMap.Keys()
							locationSet := make(map[string]bool)
							for _, item := range locations {
								locationSet[item] = true
							}

							var replicationT string
							for {
								replicationTarget, err := PopRandomElementInArray(&allIps)

								if err != nil {
									fmt.Println("Error popping random ip from all ips. Continuing.", err)
									return
								}
								member, ok := gossiputils.MembershipMap.Get(replicationTarget)

								if !locationSet[replicationTarget] && ok && member.State == gossiputils.ALIVE {
									replicationT = replicationTarget
									break
								}
							}

							fmt.Println("Replication Target ", replicationT)

							// TODO potential bug, if there is a connection that is down, we should try to pick a new one right away, not continue alltogether.
							conn, err := utils.OpenTCPConnection(ip, utils.SDFS_PORT)
							if err != nil {
								log.Println("unable to open connection: ", err)
								continue
							}
							defer conn.Close()

							fmt.Println("Openeed connection to ", ip)
							ogFileSize, ok := FileToSize.Get(fileName)
							if !ok {
								log.Fatalln("This logic is impossible, you should have a file's size if a re replication is happening")
							}

							task := utils.Task{
								DataTargetIp:        utils.New19Byte(replicationT),
								AckTargetIp:         utils.New19Byte(gossiputils.Ip),
								ConnectionOperation: utils.WRITE,
								FileName:            utils.New1024Byte(fileName),
								OriginalFileSize:    ogFileSize,
								BlockIndex:          blockIdx,
								DataSize:            0,
								IsAck:               false,
							}

							err = utils.SendTaskOnExistingConnection(task, conn)
							if err != nil {
								log.Printf("unable to send task on existing connection: ", err)
								continue
							}
							break
						}
					}
				}
			}
		}
	}

	fmt.Println("Cleaning downed node data.")
	HandleDown(downIpAddr)
	fmt.Println("Cleaned downed node data.")
}
