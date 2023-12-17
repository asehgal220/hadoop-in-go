package maplejuice

import (
	"encoding/hex"
	"errors"
	"fmt"
	"sort"

	maplejuiceUtils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/MapleJuice/mapleJuiceUtils"
	mapleutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/MapleJuice/mapleJuiceUtils"
	gossiputils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	sdfs_client "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs"
	sdfsutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs/sdfsUtils"
)

func InitiateJuicePhase(localExecFile string, nJuices uint32, sdfsPrefix string, sdfsDst string, deleteInput bool, partition maplejuiceUtils.PartitioningType) {
	// Initiates the Juice phase via client command

	// 1. GET all sdfs files' names associated with SdfsPrefix (1 file per unique key), call it SdfsPrefixKeys
	sdfsPrefixKeys := sdfs_client.InitiateLsWithPrefix(sdfsPrefix)
	fmt.Println(sdfsPrefixKeys)

	// 2. get an array of NJuices IPs from gossip memlist, call it JuiceDsts
	// log.Printf("Num juices: ", NJuices)
	juiceDsts := gossiputils.RandomKIpAddrs(int(nJuices), true)
	fmt.Println(juiceDsts)

	// 3. Call PartitionKeys(SdfsPrefixKeys, JuiceDsts) that returns a map of IPAddr:[sdfsKeyFile]
	partitionedKeys := PartitionKeys(sdfsPrefixKeys, juiceDsts, partition)
	fmt.Println(partitionedKeys)

	// 4. For each IpAddr in above map:
	var i uint32 = 0
	for ipAddr, sdfsKeyFiles := range partitionedKeys {

		// 5. SendJuiceTask(IpAddr, [sdfsKeyFiles])
		err := SendJuiceTask(ipAddr, sdfsKeyFiles, i, localExecFile, sdfsPrefix, nJuices, sdfsDst)
		if err != nil {
			fmt.Printf("Error with sending juice task to ip addr %s, %v\n", ipAddr, err)
		}

		i++
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
		if numAcksRecieved == nJuices {
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

}

func PartitionKeys(sdfsPrefixKeys []string, juiceDsts []string, partition maplejuiceUtils.PartitioningType) map[string][]string {
	rv := make(map[string][]string)

	if partition == maplejuiceUtils.RANGE {
		sort.Strings(sdfsPrefixKeys)
	}

	dstIdx := 0
	for _, key := range sdfsPrefixKeys {
		var ipaddr string

		if partition == maplejuiceUtils.HASH {
			hash := maplejuiceUtils.CalculateSHA256(key)
			hashint, err := hex.DecodeString(hash)
			if err != nil {
				fmt.Printf("Error in key partitioning, cant get hash of %s: %v\n", hash, err)
			}
			intHash := int(hashint[0])

			// Perform modular arithmetic
			idx := intHash % len(juiceDsts)
			ipaddr = juiceDsts[idx]
		} else if partition == maplejuiceUtils.RANGE {
			idx := dstIdx % len(juiceDsts)
			ipaddr = juiceDsts[idx]
			dstIdx++
		}

		val, exists := rv[ipaddr]
		fmt.Println(ipaddr)
		var arr []string
		if !exists {
			rv[ipaddr] = append(arr, key)
		} else {
			rv[ipaddr] = append(val, key)
		}
	}

	return rv
}

func SendJuiceTask(ipDest string, sdfsKeyFiles []string, nodeIdx uint32, localExecFile string, sdfsPrefix string, nJuices uint32, sdfsDst string) error {
	if ipDest == "" {
		return errors.New("Ip destination was empty for sending juice task")
	}

	for _, sdfsKeyFile := range sdfsKeyFiles {
		conn, err := sdfsutils.OpenTCPConnection(ipDest, maplejuiceUtils.MAPLE_JUICE_PORT)
		if err != nil {
			return err
		}

		Task := maplejuiceUtils.MapleJuiceTask{
			Type:            maplejuiceUtils.JUICE,
			NodeDesignation: nodeIdx,
			SdfsPrefix:      sdfsKeyFile,
			SdfsExecFile:    localExecFile,
			NumberOfMJTasks: nJuices,
			SdfsDst:         sdfsDst,
		}

		arr := Task.Marshal()
		_, err = conn.Write(arr)
		fmt.Println("Sent juice task to end node ", ipDest)
		if err != nil {
			return err
		}

		n, err := conn.Write([]byte("\n"))
		if err != nil || n != 1 {
			return err
		}

		conn.Close()
	}

	return nil
}
