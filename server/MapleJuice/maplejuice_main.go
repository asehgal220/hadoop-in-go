package maplejuice

import (
	"fmt"
	"net"

	followerutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/MapleJuice/follower"
	maplejuiceutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/MapleJuice/mapleJuiceUtils"
	sdfsutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs/sdfsUtils"
)

func MapleJuiceMainListener() {
	// The main listener where we listen for maplejuice requests

	tcpConn, listenError := sdfsutils.ListenOnTCPConnection(maplejuiceutils.MAPLE_JUICE_PORT)
	if listenError != nil {
		fmt.Printf("Error listening on port %s", maplejuiceutils.MAPLE_JUICE_PORT)
		return
	}
	defer tcpConn.Close()

	fmt.Println("maplejuice client is listening on local machine")

	for {
		// Read data from the TCP connection
		conn, err := tcpConn.Accept()
		// conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))

		if err != nil {
			fmt.Println("Error accepting tcp connection:", err)
			continue
		}

		go HandleConnection(conn)
	}
}

func HandleConnection(conn net.Conn) {
	mapleJuiceTask, _ := maplejuiceutils.UnmarshalMapleJuiceTask(conn)
	sdfsutils.SendSmallAck(conn) // TODO unsure

	if mapleJuiceTask.Type == maplejuiceutils.MAPLE {
		followerutils.HandleMapleRequest(mapleJuiceTask, conn)
	} else if mapleJuiceTask.Type == maplejuiceutils.JUICE {
		followerutils.HandleJuiceRequest(mapleJuiceTask, &conn)
	}

	conn.Close()
}
