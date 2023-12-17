package sdfsutils

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"

	gossiputils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
)

type BlockOperation int

// Potentially use send, receive, write, delete types instead types instead
const (
	READ           BlockOperation = 0
	WRITE          BlockOperation = 1
	DELETE         BlockOperation = 2
	GET_2D         BlockOperation = 3
	FORCE_GET      BlockOperation = 4
	GET_PREFIX     BlockOperation = 5
	SIZE_BY_PREFIX BlockOperation = 6
)

const (
	DELETE_OP string = "d"
	WRITE_OP  string = "w"
)

type Task struct {
	DataTargetIp        [19]byte
	AckTargetIp         [19]byte
	ConnectionOperation BlockOperation // READ, WRITE, GET_2D, OR DELETE from sdfs utils
	FileName            [1024]byte
	OriginalFileSize    int64
	BlockIndex          int64
	DataSize            int64 // TODO change me to int64
	IsAck               bool
}

const KB = int64(1024)
const MB = int64(KB * 1024)
const SDFS_PORT = "4005"
const FILESYSTEM_ROOT = "server/sdfs/sdfsFileSystemRoot/"
const BLOCK_SIZE = int64(20 * MB)
const REPLICATION_FACTOR = int64(4)

var FileSet map[string]bool

var MuLocalFs sync.Mutex
var CondLocalFs = sync.NewCond(&MuLocalFs)
var LEADER_IP string = "172.22.158.162"

type LimitedWriter struct {
	Writer  io.Writer
	Limit   int64
	Written int64
}

func (lw *LimitedWriter) Write(p []byte) (n int, err error) {
	// Check if the limit has been reached
	remaining := lw.Limit - lw.Written
	if remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}

	// Write the data and update the number of bytes written
	n, err = lw.Writer.Write(p)
	lw.Written += int64(n)
	if lw.Written >= lw.Limit {
		return n, io.EOF
	}
	return n, err
}

// Opens a tcp connection to the provided ip address and port, and returns the connection object
func OpenTCPConnection(ipAddr string, port string) (net.Conn, error) {
	// Concatenate IP address and port to form the address string
	address := ipAddr + ":" + port

	// Attempt to establish a TCP connection
	conn, err := net.Dial("tcp", address)
	if err != nil {
		// Handle error if connection fails
		fmt.Println("Error:", err)
		return nil, err
	}

	// Connection successful, return connection object and nil error
	return conn, nil
}

func ListenOnTCPConnection(port string) (net.Listener, error) {

	tcpConn, listenErr := net.Listen("tcp", ":"+port)
	if listenErr != nil {
		fmt.Println("Error listening:", listenErr)
		os.Exit(1)
	}

	return tcpConn, nil
}

func GetFileSize(filePath string) (int64, error) {
	// Get file information
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		fmt.Println("Error:", err)
		return 0, err
	}

	// Get file size in bytes
	fileSize := int64(fileInfo.Size())

	// Print the file size
	return fileSize, nil
}

func CeilDivide(a, b int64) int64 {
	// Perform integer division
	quotient := a / b

	// Check if there is a remainder
	remainder := a % b
	if remainder > 0 {
		// If there is a remainder, round up the quotient
		quotient++
	}

	return quotient
}

func GetFileName(sdfsFilename string, blockidx string) string {
	return fmt.Sprintf("%s%s_%s", FILESYSTEM_ROOT, blockidx, sdfsFilename)
}

func GetFilePtr(sdfsFilename string, blockidx string, flags int) (string, int, *os.File, error) {
	// Specify the file path
	filePath := GetFileName(sdfsFilename, blockidx)
	fileSize, _ := GetFileSize(filePath)
	fmt.Printf("File path to block %s", filePath)
	file, err := os.OpenFile(filePath, flags, 0666)
	if err != nil {
		// Handle the error if the file cannot be opened
		if os.IsNotExist(err) {
			fmt.Println("File does not exist.")
		}
	}

	return filePath, int(fileSize), file, err
}

func BufferedWriteToConnection(conn net.Conn, fp *os.File, size, startIdx int64) (int64, error) {
	// Seek to the starting index in the source file
	_, err := fp.Seek(startIdx, io.SeekStart)
	if err != nil {
		return 0, err
	}

	// Create a LimitedReader to copy a specific number of bytes
	limitedReader := &io.LimitedReader{
		R: fp,
		N: size,
	}

	// Use io.CopyN to copy the specified number of bytes to the connection
	n, err := io.CopyN(bufio.NewWriter(conn), limitedReader, size)
	if err != nil {
		fmt.Println("Error in buffered write to connection:", err)
	}
	return n, err
}

func BufferedReadFromConnection(conn net.Conn, fp *os.File, size int64) (int64, error) {
	// Create a custom writer to limit the number of bytes copied
	limitedWriter := &LimitedWriter{
		Writer: fp,
		Limit:  size,
	}

	// Use io.Copy to copy data, respecting the limit
	n, err := io.Copy(limitedWriter, conn)
	if err != nil && err != io.EOF {
		panic(err)
	}
	log.Printf("Size: %d, Read: %d", size, n)
	if n < size {
		log.Printf("didn't read enough data from connection")
	}
	return n, nil
}

func SendTask(task Task, ipAddr string, ack bool) (*net.Conn, error) {
	conn, tcpOpenError := OpenTCPConnection(ipAddr, SDFS_PORT)
	if tcpOpenError != nil {
		return nil, tcpOpenError
	}

	bufferConn := bufio.NewWriter(conn)

	task.IsAck = ack
	arr := task.Marshal()
	bytesWritten, err := bufferConn.Write(arr)
	if err != nil {
		return nil, err
	} else if bytesWritten != len(arr) {
		return nil, io.ErrShortWrite
	}
	bufferConn.Write([]byte{'\n'})
	bufferConn.Flush()

	fmt.Println("Sent task to leader ip:", ipAddr)

	return &conn, nil
}

func SendTaskOnExistingConnection(task Task, conn net.Conn) error {
	arr := task.Marshal()
	bytesWritten, err := conn.Write(arr)
	if err != nil {
		return err
	} else if bytesWritten != len(arr) {
		return io.ErrShortWrite
	}
	conn.Write([]byte{'\n'})

	return nil
}

func SendAckToMaster(task Task) *net.Conn {
	leaderIp := gossiputils.GetLeader()

	fmt.Printf("detected Leader ip: %s\n", leaderIp)
	task.AckTargetIp = New19Byte(gossiputils.Ip)
	val, ok := gossiputils.MembershipMap.Get(leaderIp)
	if ok && (val.State == gossiputils.ALIVE || val.State == gossiputils.SUSPECTED) {
		conn, connectionError := SendTask(task, leaderIp, true)

		if connectionError != nil {
			log.Println("Pick")
			return SendAckToMaster(task)
		}

		return conn
	} else {
		newLeader := gossiputils.GetLeader()
		conn, _ := SendTask(task, newLeader, true)

		return conn
	}
}

func New19Byte(data string) [19]byte {
	var byteArr [19]byte
	copy(byteArr[:], []byte(data))
	return byteArr
}

func New1024Byte(data string) [1024]byte {
	var byteArr [1024]byte
	copy(byteArr[:], []byte(data))
	return byteArr
}

func BytesToString(data []byte) string {
	return strings.TrimRight(string(data), "\x00")
}

func GetBlockPosition(blockNumber int64, fileSize int64) (int64, int64) {
	currentByteIdx := blockNumber * int64(BLOCK_SIZE)

	if BLOCK_SIZE < fileSize-currentByteIdx {
		return currentByteIdx, BLOCK_SIZE
	} else {
		return currentByteIdx, fileSize - currentByteIdx
	}
}

func GetMinInt64(a int64, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (task Task) Marshal() []byte {
	marshaledTask, err := json.Marshal(task)
	if err != nil {
		log.Fatalf("error marshaling task: %v\n", err)
	}
	return marshaledTask
}

func Unmarshal(conn net.Conn) (*Task, int64) {
	var task Task

	buffConn := bufio.NewReader(conn)

	// Read from the connection until a newline is encountered
	data, err := buffConn.ReadBytes('\n')
	if err != nil {
		log.Fatalf("Error reading from connection: %v\n", err)
	}
	data = data[:len(data)-1]

	err = json.Unmarshal([]byte(data), &task)

	if err != nil {
		log.Fatalf("Error unmarshalling task: %v\n", err)
	}

	return &task, int64(len(data))
}

func MarshalBlockLocationArr(array [][]string) []byte {
	jsonData, err := json.Marshal(array)
	if err != nil {
		log.Fatalf("error marshaling 2d arr: %v\n", err)
	}
	return jsonData
}

func UnmarshalBlockLocationArr(conn net.Conn) ([][]string, error) {
	var locations [][]string

	decoder := json.NewDecoder(conn)
	err := decoder.Decode(&locations)

	if err != nil {
		log.Fatalf("Error unmarshalling 2d arr: %v\n", err)
		return nil, err
	}

	return locations, nil
}

func SendSmallAck(conn net.Conn) {
	_, err := conn.Write([]byte("A"))
	if err != nil {
		log.Fatalln("err: ", err)
	}
}

func ReadSmallAck(conn net.Conn) {
	buffer := make([]byte, 1)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			log.Print("Error reading from connection: ", err)
			break
		}
		if n > 0 {
			break
		}
	}
}
