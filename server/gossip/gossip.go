package gossip

import (
	"fmt"
	"log"
	"net"
	"time"

	cmap "github.com/orcaman/concurrent-map/v2"
	utils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	sdfsleader "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs"
)

// Our main entry point to begin gossiping
func InitializeGossip() {
	utils.Ip = GetOutboundIP().String()
	timestamp := time.Now().UnixNano()

	newMember := utils.Member{
		Ip:                utils.Ip,
		Port:              utils.GOSSIP_PORT,
		CreationTimestamp: timestamp,
		HeartbeatCounter:  0,
		State:             utils.ALIVE,
	}

	// Index by hostname
	utils.MembershipMap = cmap.New[utils.Member]()
	utils.MembershipUpdateTimes = cmap.New[int64]()
	utils.FailureHandler = cmap.New[bool]()

	utils.MembershipMap.Set(utils.Ip, newMember)
	utils.MembershipUpdateTimes.Set(utils.Ip, timestamp)

	if utils.Ip != utils.INTRODUCER_IP { // if we're not the introducer, we've gotta ping the introducer
		PingServer(utils.INTRODUCER_IP, "")
	}

	go SendMembershipList()
	go PruneNodeMembers()
	ListenForLists()
}

func MemberPrint(m utils.Member) string {
	return fmt.Sprintf("IP: %s, Port: %s, Timestamp: %d, State: %d, Type: %d", m.Ip, m.Port, m.CreationTimestamp, m.State, m.Type)
}

func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

// Check if nodes need to be degraded from ALIVE or DOWN statuses
func PruneNodeMembers() {
	for {
		
		// Go through all currently stored nodes and check their lastUpdatedTimes
		for info := range utils.MembershipUpdateTimes.IterBuffered() {
			nodeIp, lastUpdateTime := info.Key, info.Val

			if nodeIp == utils.Ip {
				continue
			}

			if node, ok := utils.MembershipMap.Get(nodeIp); ok {
				// If node has left the network, don't do any additional pruning
				if node.State == utils.LEFT {
					continue
				}
				// If the time elasped since last updated is greater than 6 (Tfail + Tcleanup), mark node as DOWN
				if time.Now().UnixNano()-lastUpdateTime >= utils.Tfail+utils.Tcleanup {
					if node.State != utils.DOWN {
						mssg := fmt.Sprintf("SETTING NODE WITH IP %s AS DOWN\n", nodeIp)
						utils.LogFile.WriteString(mssg)
					}
					node.State = utils.DOWN

					val, ok := utils.FailureHandler.Get(node.Ip)
					machineType := utils.MachineType()
					if (!ok || !val) && machineType == utils.LEADER {
						utils.FailureHandler.Set(node.Ip, false)
						fmt.Println("AT THE MASTER, NEED TO HANDLE NODE FAILURE FOR MEMBER", node.Ip)
						sdfsleader.HandleReReplication(node.Ip) // TODO UNTESTED
						
						utils.FailureHandler.Set(node.Ip, true)
					}
				} else if utils.ENABLE_SUSPICION && time.Now().UnixNano()-lastUpdateTime >= utils.Tfail { // If the time elasped since last updated is greater than Tfail, mark node as SUSPECTED
					// If the node is not already suspicious, log it as so
					if node.State != utils.SUSPECTED {
						mssg := fmt.Sprintf("SETTING NODE WITH IP %s AS SUSPICIOUS\n", nodeIp)
						utils.LogFile.WriteString(mssg)

						currentTime := time.Now()
						unixTimestamp := currentTime.UnixNano()

						fmt.Printf("SETTING NODE WITH IP %s AS SUSPICIOUS AT TIME %d\n", nodeIp, unixTimestamp)
					}
					node.State = utils.SUSPECTED
				} else if !utils.ENABLE_SUSPICION && time.Now().UnixNano()-lastUpdateTime >= utils.Tfail { // If suspicion is disabled, mark the node as down as soon as time > Tfail
					if node.State != utils.DOWN {
						mssg := fmt.Sprintf("SETTING NODE WITH IP %s AS DOWN\n", nodeIp)
						utils.LogFile.WriteString(mssg)
					}
					node.State = utils.DOWN

					val, ok := utils.FailureHandler.Get(node.Ip)
					machineType := utils.MachineType()
					if (!ok || !val ) && machineType == utils.LEADER {
						utils.FailureHandler.Set(node.Ip, false)
						fmt.Println("AT THE MASTER, NEED TO HANDLE NODE FAILURE FOR MEMBER", node.Ip)
						sdfsleader.HandleReReplication(node.Ip)
						
						utils.FailureHandler.Set(node.Ip, true)
					}
				} else {
					node.State = utils.ALIVE
				}
				utils.MembershipMap.Set(nodeIp, node)
			}
		}
	}
}

func PrintMembership() {
	for info := range utils.MembershipMap.IterBuffered() {
		if member, ok := utils.MembershipMap.Get(info.Key); ok {
			fmt.Println("Member string: ", MemberPrint(member))
		}
	}
}
