package distributedgrepserver

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
)

var SERVER_TCP_PORT = ":5432"
var CLIENT_TCP_PORT string = ":2345"

var MAX_TCP_REQUEST_LEN int = 1024

func InitializeServer() {
	machine_file := "../logs/machine.txt"

	content, read_err := os.ReadFile(machine_file)
	if read_err != nil {
		fmt.Println("SERVER:99 = Error:", read_err)
		return
	}

	// Convert the file content to an integer
	machine_number, convert_err := strconv.Atoi(string(content[0]))
	if convert_err != nil {
		fmt.Println("SERVER:106 = Error:", convert_err)
		return
	}

	cache := createCache(100000)
	Server(machine_number, cache)
}

func Server(machine_num int, _cache *Cache) {
	tcpAddr, resolve_err := net.ResolveTCPAddr("tcp", SERVER_TCP_PORT)
	if resolve_err != nil {
		fmt.Println("Error:", resolve_err)
		return
	}

	ln, listen_err := net.ListenTCP("tcp", tcpAddr)
	if listen_err != nil {
		fmt.Println("Error:", listen_err)
		return
	}
	defer ln.Close()

	fmt.Println("TCP server is listening on " + SERVER_TCP_PORT)

	for {
		conn, err := ln.Accept()
		// conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))

		if err != nil {
			fmt.Println("Error reading:", err)
			continue
		}

		go HandleGrepRequest(conn.RemoteAddr().String(), conn, machine_num, _cache)
	}
}

// execute the grep request from the packet, and send the data back to the address under a different TCP connection
func HandleGrepRequest(addr string, conn net.Conn, machine_num int, _cache *Cache) {
	defer conn.Close()
	fmt.Printf("Received TCP connection from %s:\n", addr)
	NumberBytes := 0
	ByteGrepCommand := make([]byte, MAX_TCP_REQUEST_LEN)
	for {
		TempN, err := conn.Read(ByteGrepCommand)
		NumberBytes += TempN
		if err == io.EOF || TempN < MAX_TCP_REQUEST_LEN {
			break
		}
		if err != nil {
			log.Fatalf("Error reading info from socket: %v", err)
			return
		}
	}
	GrepCommand := ""
	for i := 0; i < NumberBytes; i++ {
		GrepCommand += string(ByteGrepCommand[i])
	}

	grepCacheOutput := get(_cache, GrepCommand)
	var data string

	if grepCacheOutput == nil {
		exec_query := fmt.Sprintf("grep -rH %s ../logs/machine.%d.log", GrepCommand, machine_num)
		cmd := exec.Command("bash", "-c", exec_query)

		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err := cmd.Run()

		if err != nil {
			if _, ok := err.(*exec.ExitError); ok {
				// exitCode := exitError.ExitCode()
				// if exitCode != 1 {
				// 	// fmt.Println("File not found, are u sure the queried server is querying the right number logfile?: ", err)
				// }
				data = ""
			} else {
				fmt.Println("Command failed:", err)
			}
		} else {
			data = stdout.String()
			put(_cache, GrepCommand, data)
		}

	} else {
		switch valstr := grepCacheOutput.(type) {
		case string:
			data = valstr
		default:
			fmt.Printf("Something went wrong with the cache output\n")
			return
		}
	}

	// Send data over the connection
	write_err := BufferedWrite(conn, []byte(data))
	if write_err != nil {
		fmt.Println("Error sending data:", write_err)
		return
	}

	fmt.Println("Data sent successfully!")
}

func BufferedWrite(conn net.Conn, data []byte) error {
	w := bufio.NewWriter(conn)
	chunkSize := 1000 * 1024 // 1 MB

	for len(data) > 0 {
		// Determine the size of the next chunk to write
		writeSize := chunkSize
		if len(data) < chunkSize {
			writeSize = len(data)
		}

		// Write the next chunk to the buffer
		_, err := w.Write(data[:writeSize])
		if err != nil {
			// Handle the error (e.g., log or return it)
			return err
		}

		// Flush the buffer to the connection
		err = w.Flush()
		if err != nil {
			// Handle the error (e.g., log or return it)
			return err
		}

		// Move to the next chunk
		data = data[writeSize:]
	}

	return nil
}
