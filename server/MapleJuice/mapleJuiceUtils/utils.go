package maplejuiceutils

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
)

type MapleJuiceType int
type PartitioningType int
type SQLCommandType uint16

const HASH PartitioningType = 0
const RANGE PartitioningType = 1

const MAPLE MapleJuiceType = 0
const JUICE MapleJuiceType = 1

const COMMAND_1 SQLCommandType = 0
const COMMAND_2 SQLCommandType = 1
const INVALID_COMMAND SQLCommandType = 2

const MAPLE_JUICE_PORT = "4000"
const MAPLE_JUICE_ACK_PORT = "4001"

type MapleJuiceTask struct {
	Type              MapleJuiceType
	NodeDesignation   uint32 // The 'index' of the node recieving the maplejuice task
	SdfsPrefix        string
	SdfsExecFile      string // The name of the executable that exists in sdfs
	NumberOfMJTasks   uint32
	SdfsDst           string
	SqlCommand        SQLCommandType
	ExecFileArguments []string
	// We also need to somehow track
}

func CalculateSHA256(input string) string {
	// Create a new SHA-256 hash
	hasher := sha256.New()

	// Write the string to the hash
	hasher.Write([]byte(input))

	// Get the hashed bytes
	hashedBytes := hasher.Sum(nil)

	// Convert the bytes to a hexadecimal string
	hashedString := hex.EncodeToString(hashedBytes)

	return hashedString
}

func (task MapleJuiceTask) Marshal() []byte {
	marshaledTask, err := json.Marshal(task)
	if err != nil {
		log.Fatalf("error marshaling task: %v\n", err)
	}
	return marshaledTask
}

func UnmarshalMapleJuiceTask(conn net.Conn) (*MapleJuiceTask, int64) {
	var task MapleJuiceTask

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

func ReadAllDataFromConn(conn net.Conn, outputFileName string) {
	fp := OpenFile(outputFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC)
	defer fp.Close()
	n, err := io.Copy(fp, conn)
	log.Println("Number of bytes read: ", n)
	if err != nil {
		log.Println("Error copying data:", err)
	}

	log.Println("Data copied successfully")
}

func OpenFile(fileName string, permissions int) *os.File {
	fp, err := os.OpenFile(fileName, permissions, 0644)
	if err != nil {
		return nil
	}
	return fp
}
