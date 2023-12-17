package gossip

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	cmap "github.com/orcaman/concurrent-map/v2"
	utils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
)

func DeserializeStruct(serializedData []byte) (cmap.ConcurrentMap[string, utils.Member], error) {
	var data cmap.ConcurrentMap[string, utils.Member] = cmap.New[utils.Member]()

	err := data.UnmarshalJSON(serializedData)

	return data, err
}

// With merge, only need to check if incoming member info has more recent data. If local member info has more data, changes will be reflected in push
func Merge(newMemberInfo cmap.ConcurrentMap[string, utils.Member]) {

	// Iterate through the incoming membership list
	for info := range newMemberInfo.IterBuffered() {
		newMemberIp, newMemberVersion := info.Key, info.Val
		if newMemberIp == utils.Ip {
			continue
		}
		// Check if the a node in the current membership list matches a found node in the incoming membership list
		if localMemberVersion, exists := utils.MembershipMap.Get(newMemberIp); exists {

			// Call update membership to get most up to date information on node
			upToDateMember := UpdateMembership(localMemberVersion, newMemberVersion)

			// Set current membership list to most updated node membership
			utils.MembershipMap.Set(newMemberIp, upToDateMember)
		} else { // If its a new node not currently in the membership list
			// Update the local membership list's version history and update time
			utils.MembershipMap.Set(newMemberIp, newMemberVersion)
			utils.MembershipUpdateTimes.Set(newMemberIp, time.Now().UnixNano())
			mssg := fmt.Sprintf("NODE WITH IP %s JUST JOINED\n", newMemberIp)
			utils.LogFile.WriteString(mssg)
		}
	}

}

// Returns updated member and if updated member needs to be added to list. Member creation timestamp created on originating machine
func UpdateMembership(localMember utils.Member, newMember utils.Member) utils.Member {
	// If both members are the same version of a node
	if localMember.CreationTimestamp == newMember.CreationTimestamp {
		// If either version is marked down, return the node as down
		if localMember.State == utils.DOWN || newMember.State == utils.DOWN {
			localMember.State = utils.DOWN
			return localMember
		}

		// If either version is marked left, return the node as left
		if localMember.State == utils.LEFT || newMember.State == utils.LEFT {
			// If the local member still thinks a node is there, but a node with more recent history knows a node left, log that the node left
			if localMember.State != utils.LEFT {
				mssg := fmt.Sprintf("SETTING NODE %s AS LEFT\n", localMember.Ip)
				utils.LogFile.WriteString(mssg)
			}
			localMember.State = utils.LEFT
			return localMember
		}

		// Find the current most up to date member by heartbeats
		upToDateMember, sameHeartbeatCount := utils.CurrentMember(localMember, newMember)
		if sameHeartbeatCount {
			localMember.State = utils.Max(localMember.State, newMember.State) // If they have the same heartbeat count, set the state as whatever is the most prominent (ALIVE < SUS < DOWN)
		} else {
			// If the newest isn't the local member, update the local member
			if localMember != upToDateMember && upToDateMember.State == utils.ALIVE {
				// Set that the node has been updated at the most recent local time
				utils.MembershipUpdateTimes.Set(localMember.Ip, time.Now().UnixNano())
			}
			localMember.HeartbeatCounter = upToDateMember.HeartbeatCounter
			localMember.State = upToDateMember.State
		}
		// Return the updated local member and that a new node doesn't needed to be added to the version history
		return localMember
	} else if localMember.CreationTimestamp < newMember.CreationTimestamp { // If the local version is lower than the new version, return that the new member needs to be added to the local version history
		// Update the local update time for the node
		if newMember.State == utils.ALIVE {
			utils.MembershipUpdateTimes.Set(localMember.Ip, time.Now().UnixNano())
		}
		// Return that the incoming node is a new node version
		return newMember
	}
	// If the local node version is ahead of the external node version, then nothing needs to happen. Changes will be made to the network when current node pushes to other nodes
	return localMember
}

// This function keeps a udp socket open for membership lists
func ListenForLists() {
	serverAddr, resolveErr := net.ResolveUDPAddr("udp", ":"+utils.GOSSIP_PORT)
	if resolveErr != nil {
		fmt.Println("Error resolving address:", resolveErr)
		os.Exit(1)
	}

	udpConn, listenErr := net.ListenUDP("udp", serverAddr)
	if listenErr != nil {
		fmt.Println("Error listening:", listenErr)
		os.Exit(1)
	}
	defer udpConn.Close()

	fmt.Println("gossip client is listening on", serverAddr)

	buffer := make([]byte, utils.MLIST_SIZE)
	for {
		// Read data from the UDP connection
		n, _, err := udpConn.ReadFromUDP(buffer)
		randomNum := utils.RandomNumInclusive()

		if err != nil {
			fmt.Println("Error reading from UDP connection:", err)
			continue
		} else if randomNum <= utils.MessageDropRate {
			continue
		}

		data := buffer[:n]

		if strings.Compare(string(data), utils.ENABLE_SUSPICION_MSG) == 0 {
			utils.ENABLE_SUSPICION = true
		} else if strings.Compare(string(data), utils.DISABLE_SUSPICION_MSG) == 0 {
			utils.ENABLE_SUSPICION = false
		} else {
			newlist, errDeseriealize := DeserializeStruct(data)
			if errDeseriealize != nil {
				fmt.Println("Inbound data was not a membership list: ", errDeseriealize)
			} else {
				Merge(newlist)
			}
		}
	}
}
