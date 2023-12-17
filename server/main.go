package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	maplejuice "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/MapleJuice"
	maplejuiceclient "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/MapleJuice/client"
	maplejuiceutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/MapleJuice/mapleJuiceUtils"
	sqlcommands "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/MapleJuice/sqlCommands"
	"gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip"
	utils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	"gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs"
	sdfsclient "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs"
)

type CLICommand string

const (
	PUT       CLICommand = "put"
	GET       CLICommand = "get"
	DELETE    CLICommand = "delete"
	LS        CLICommand = "ls"
	STORE     CLICommand = "store"
	LIST_MEM  CLICommand = "list_mem"
	LIST_SELF CLICommand = "list_self"
	LEAVE     CLICommand = "leave"
	EN_SUS    CLICommand = "enable_sus"
	D_SUS     CLICommand = "disable_sus"
	MULTIREAD CLICommand = "multiread"
	MAPLE     CLICommand = "maple"
	JUICE     CLICommand = "juice"
	SELECT    CLICommand = "SELECT"
)

// Send suspicion flip message to all machines
func setSendingSuspicionFlip(enable bool) {
	utils.ENABLE_SUSPICION = enable

	// Send enable messages to all nodes
	for info := range utils.MembershipMap.IterBuffered() {
		nodeIp, _ := info.Key, info.Val

		if enable { // if enabling, suspicion, send a certain string. else, send a differnt one
			gossip.PingServer(nodeIp, utils.ENABLE_SUSPICION_MSG)
		} else {
			gossip.PingServer(nodeIp, utils.DISABLE_SUSPICION_MSG)
		}
	}
}

func RunCLI() {
	for {
		reader := bufio.NewReader(os.Stdin) // Our reader to handle userinputted commands
		command, err := reader.ReadString('\n')
		fmt.Println(command)
		if err != nil {
			log.Fatal(err)
		}

		commandArgs := strings.Split(command, " ")
		numArgs := len(commandArgs)

		if strings.Contains(commandArgs[0], string(LIST_MEM)) && numArgs == 1 {
			gossip.PrintMembership()
		} else if strings.Contains(commandArgs[0], string(LIST_SELF)) && numArgs == 1 {
			if selfMember, ok := utils.MembershipMap.Get(utils.Ip); ok {
				fmt.Printf("%d\n", selfMember.CreationTimestamp)
			}
		} else if strings.Contains(commandArgs[0], string(LEAVE)) && numArgs == 1 {
			if member, ok := utils.MembershipMap.Get(utils.Ip); ok {
				member.State = utils.LEFT
				utils.MembershipMap.Set(utils.Ip, member)
				time.Sleep(time.Second)
			}
			os.Exit(0)
		} else if strings.Contains(commandArgs[0], string(EN_SUS)) && numArgs == 1 {
			setSendingSuspicionFlip(true)
		} else if strings.Contains(commandArgs[0], string(D_SUS)) && numArgs == 1 {
			setSendingSuspicionFlip(false)
		} else if strings.Contains(commandArgs[0], string(PUT)) && numArgs == 3 {
			localfilename := strings.TrimSpace(commandArgs[1])
			sdfsFileName := strings.TrimSpace(commandArgs[2])

			sdfsclient.CLIPut(localfilename, sdfsFileName)
		} else if strings.Contains(commandArgs[0], string(GET)) && numArgs == 3 {
			localfilename := strings.TrimSpace(commandArgs[1])
			sdfsFileName := strings.TrimSpace(commandArgs[2])

			sdfs.CLIGet(sdfsFileName, localfilename)
		} else if strings.Contains(commandArgs[0], string(DELETE)) && numArgs == 2 {
			sdfsFileName := strings.TrimSpace(commandArgs[1])

			mappings, mappingsErr := sdfsclient.SdfsClientMain(sdfsFileName, true)
			if mappingsErr != nil {
				fmt.Println("Error with sdfsclient main. Aborting Get command: ", mappingsErr)
				return
			}
			fmt.Println("FOUND MAPPING: ", mappings)

			sdfsclient.InitiateDeleteCommand(sdfsFileName, mappings)

		} else if strings.Contains(commandArgs[0], string(LS)) && numArgs == 2 {
			sdfsFileName := strings.TrimSpace(commandArgs[1])

			mappings, mappingsErr := sdfsclient.SdfsClientMain(sdfsFileName, true)
			if mappingsErr != nil {
				fmt.Println("Error with sdfsclient main. Aborting Get command: ", mappingsErr)
				return
			}

			sdfsclient.InitiateLsCommand(mappings)

		} else if strings.Contains(commandArgs[0], string(STORE)) && numArgs == 1 {
			sdfsclient.InitiateStoreCommand()
		} else if strings.Contains(commandArgs[0], string(MULTIREAD)) {
			for i, part := range commandArgs {
				part = strings.TrimSpace(part)
				commandArgs[i] = part
			}
			sdfsclient.InitiateMultiRead(commandArgs[1], commandArgs[2:])
		} else if strings.Contains(commandArgs[0], string(MAPLE)) && numArgs == 5 {
			fmt.Println("GOT MAPLE")
			for i, part := range commandArgs {
				part = strings.TrimSpace(part)
				commandArgs[i] = part
			}
			numMapleTasks, _ := strconv.ParseUint(commandArgs[2], 10, 32)
			maplejuiceclient.InitiateMaplePhase(commandArgs[1], uint32(numMapleTasks), commandArgs[3], commandArgs[4], make([]string, 0))
			// func InitiateMaplePhase(LocalExecFile string, NMaples uint32, SdfsPrefix string, SdfsSrcDataset string) {

		} else if strings.Contains(command, string(JUICE)) && numArgs == 7 {
			for i, part := range commandArgs {
				part = strings.TrimSpace(part)
				commandArgs[i] = part
			}
			numJuiceTasks, _ := strconv.ParseUint(commandArgs[2], 10, 32)
			deleteInput := commandArgs[5] == string(0)

			var pt maplejuiceutils.PartitioningType
			if commandArgs[6] == "hash" {
				pt = maplejuiceutils.HASH
			} else if commandArgs[6] == "range" {
				pt = maplejuiceutils.RANGE
			}

			maplejuiceclient.InitiateJuicePhase(commandArgs[1], uint32(numJuiceTasks), commandArgs[3], commandArgs[4], deleteInput, pt)
		} else if strings.Contains(commandArgs[0], string(SELECT)) {
			sqlcommands.ProcessSQLCommand(command)
		} else if strings.Contains(commandArgs[0], "composition") {
			sqlcommands.ProcessCompositionCommand(commandArgs)

		} else {
			error_msg := `
			Command not understood. Available commands are as follows:
				_____________________________________________________
				GOSSIP COMMANDS:
				list_mem # list the membership list
				list_self # list this node's entry
				leave # leave the network
				<percentage from 0.0 -> 1.0> # induce a network drop rate 
				ds # Disable suspicion
				es # Disable suspicion
				_____________________________________________________
				_____________________________________________________
				SDFS COMMANDS:
				put <localfilename> <sdfsFileName> # put a file from your local machine into sdfs
				get <sdfsFileName> <localfilename> # get a file from sdfs and write it to local machine
				delete <sdfsFileName> # delete a file from sdfs
				ls sdfsFileName # list all vm addresses where the file is stored
				store # at this machine, list all files paritally or fully stored at this machine
				_____________________________________________________
				_____________________________________________________
				MAPLEJUICE COMMANDS:
				maple <local_exec_file> <N maples> <sdfs prefix> <sdfs src dataset>
				juice <local_exec_file> <N juices> <sdfs prefix> <sdfs dst dataset> <delete input 0 | 1> <HASH | RANGE>
				_____________________________________________________
			`
			float, err_parse := strconv.ParseFloat(command[:len(command)-1], 32)
			if err_parse != nil {
				fmt.Println(error_msg)
			} else {
				utils.MessageDropRate = float32(float)
			}

		}
	}
}

func main() {
	go gossip.InitializeGossip()
	go sdfs.InitializeSdfsProcess()
	go maplejuice.MapleJuiceMainListener()

	RunCLI()
}
