package maplejuice

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"

	maplejuiceutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/MapleJuice/mapleJuiceUtils"
	gossiputils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	sdfs "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs"
	sdfsutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs/sdfsUtils"
)

func HandleJuiceRequest(task *maplejuiceutils.MapleJuiceTask, conn *net.Conn) {
	fmt.Println("Entering handle juice request for ", task.SdfsPrefix)
	sdfsFilename := task.SdfsPrefix // SdfsFilename is the one to pull from SDFS, and run the juice task on.
	juiceExec := task.SdfsExecFile
	dstFile := task.SdfsDst

	// CLI GET file locally
	sdfs.CLIGet(sdfsFilename, sdfsFilename)
	fmt.Println("Got file in juice follower: ", task.SdfsPrefix)

	// Run exec file on input file
	cmd := exec.Command("./"+juiceExec, append([]string{"-f", sdfsFilename}, task.ExecFileArguments...)...)

	output, err := cmd.CombinedOutput()
	fmt.Println("Ran juice cmd", juiceExec)
	if err != nil {
		fmt.Println("Error running command:", err)
		return
	}

	ParseOutput(task.NodeDesignation, string(output), dstFile, task.NumberOfMJTasks*uint32(sdfsutils.BLOCK_SIZE))
	fmt.Println("parsed output on juice task")

	os.RemoveAll(sdfsFilename)

	remoteAddr := (*conn).RemoteAddr()

	// Convert to a TCP address
	tcpAddr, _ := remoteAddr.(*net.TCPAddr)

	sdfsutils.OpenTCPConnection(tcpAddr.IP.String(), maplejuiceutils.MAPLE_JUICE_ACK_PORT)

}

func ParseOutput(nodeIdx uint32, output string, dstSdfsFile string, fileSize uint32) error {
	// Take the output, and append it to the dst sdfs file.
	nodeIdxStr := strconv.FormatUint(uint64(nodeIdx), 10)
	oFileName := sdfsutils.FILESYSTEM_ROOT + nodeIdxStr + "_" + dstSdfsFile

	fmt.Println("Writing juice node to loacl fs: ", oFileName)
	file := maplejuiceutils.OpenFile(oFileName, os.O_CREATE|os.O_APPEND|os.O_RDWR)
	defer file.Close()

	fmt.Println("Created local file")

	writer := bufio.NewWriter(file)

	// Write data to the file
	_, err := writer.WriteString(output)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return err
	}
	fmt.Println("wrote local juice data to file")

	// Flush the writer to ensure all data is written to the file
	err = writer.Flush()
	if err != nil {
		fmt.Println("Error flushing writer:", err)
		return err
	}

	// Send ack to master
	SdfsAck := sdfsutils.Task{
		DataTargetIp:        sdfsutils.New19Byte(gossiputils.Ip),
		AckTargetIp:         sdfsutils.New19Byte(gossiputils.Ip),
		ConnectionOperation: sdfsutils.WRITE,
		FileName:            sdfsutils.New1024Byte(dstSdfsFile),
		OriginalFileSize:    int64(fileSize),
		BlockIndex:          int64(nodeIdx),
		DataSize:            int64(len(output)),
		IsAck:               true,
	}

	sdfsutils.SendAckToMaster(SdfsAck)
	fmt.Println("Sent ack to master from juice follower")

	return nil
}
