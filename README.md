# cs425 mps

## Distributed Batch Processing System

### Usage
Usage is the same as the other mps. Simply start main.go, and run one of the following commands for maple or juice:

```bash
maple <local_exec_file> <N maples> <sdfs prefix> <sdfs src dataset>
juice <local_exec_file> <N juices> <sdfs prefix> <sdfs dst dataset> <delete input 0 | 1> <HASH | RANGE>

SELECT ALL FROM <sdfs dataset> WHERE <pattern>
```

## Distributed File System

### Usage

In order to initialize the SDFS, you have to start the introducer node (currently hardcoded to ```172.22.158.162```, or ```fa23-cs425-4902.cs.illinois.edu```). 

1. To do that, ssh into that server, and navigate to the directory 
```
~/cs425mps/server
```
2. (Optional) to setup grep, create a logfile with the following command
```
touch ../logs/machine.txt
vi ../logs/machine.txt # Add the node num in this file. This is purely for distributed grep, and will not have an impact on gossip.
```
3. Start the introducer server with the command 
```
go run main.go
```
4. Repeat steps 1 and 3 for all other machines. Machines will automatically join the network through the hardcoded introcuder. See below for a list of commands you can provide any client (In addition to the gossip client):
```
put <localfilename> <sdfs_filename> # put a file from your local machine into sdfs

get <sdfs_filename> <localfilename> # get a file from sdfs and write it to local machine

delete <sdfs_filename> # delete a file from sdfs

ls sdfs_filename # list all vm addresses where the file is stored

store # at this machine, list all files paritally or fully stored at this machine

multiread <sdfs_filename> [<ip1> <ip2> <ip3> ....] # Initiate a concurrent get on some file at all the specified ip addresses. They must be in the network.
```

Codes remain the same as in the gossip functionality. Additionally, the node 'Type' is determined as the following:

```
const (
	LEADER     SdfsNodeType = 2
	SUB_LEADER SdfsNodeType = 1
	FOLLOWER   SdfsNodeType = 0
)
```

## Faliure Detector

### Usage

In order to start the failure detector, you must first start the introducer server (currently hardcoded to ```172.22.158.162```, or ```fa23-cs425-4902.cs.illinois.edu```). 

1. To do that, ssh into that server, and navigate to the directory 
```
~/cs425mps/server
```
2. (Optional) to setup grep, create a logfile with the following command
```
touch ../logs/machine.txt
vi ../logs/machine.txt # Add the node num in this file. This is purely for distributed grep, and will not have an impact on gossip.
```
3. Start the introducer server with the command 
```
go run main.go
```
4. Repeat steps 1 and 3 for all other machines. Machines will automatically join the network through the hardcoded introcuder. See below for a list of commands you can provide any gossip client:
```
list_mem # list the membership list
list_self # list this node's entry
leave # leave the network
<percentage from 0.0 -> 1.0> # induce a network drop rate on this machine
ds # Disable suspicion in network
es # Disable suspicion in network
```

##### Understanding codes
The following struct outlines the meaning of the 'state' property within the membership list
```
const (
	ALIVE     = 0
	SUSPECTED = 1
	DOWN      = 2
	LEFT      = 3
)
```

### Getting started on a new machine

To configure a new machine, simply clone the repo
```
git clone https://gitlab.engr.illinois.edu/asehgal4/cs425mps.git
```
and treat it like any non-introducer machine, the process of which is outlined in the other machines.



## Distributed Logger

### Usage

In order to successfully run the distributed grep, complete the following steps:

1. ssh into all instances and navigate to the server directory
```
mp1/src/server
```
2. Build and run the server
```
go build
./server
```
3. This will start the server on each instance. Now, from any machine, run distributed grep by navigating to the client directory
```
mp1/src/client
```
And running 
```
go build
./client <pattern>
```
You need only build the client or server once, if cloning the code on a new machine. For convenience, the setup is completely done with the provided log files on piazza on all of the vms, within awangoo2's filesystem.

### Getting started on a new machine

To configure the repository on a new machine
1. Clone the repo with git clone
2. Set the machine 'number' in the ```mp1/logs/machine.txt``` file
3. Make sure the mapping from machine to id exists in all clients in the cluster, (see the bottom of ```client.go```)
```
	nodes["172.22.156.162"] = 1
	nodes["172.22.158.162"] = 2
	nodes["172.22.94.162"] = 3
	nodes["172.22.156.163"] = 4
	nodes["172.22.158.163"] = 5
	nodes["172.22.94.163"] = 6
	nodes["172.22.156.164"] = 7
	nodes["172.22.158.164"] = 8
	nodes["172.22.94.164"] = 9
	nodes["172.22.156.165"] = 10
    // Add more here...
```
5. Copy the log file you want to run grep on into the ```mp1/logs/machine.<i>.log``` file, which you will need to create.

### Running unit tests

To run unit tests, log into asehgal4's vms. The test cases have hardcoded domains to ssh into with asehgal4's doman asehgal4@fa23-cs425-4906.cs.illinois.edu. 

1. First ensure that all 10 vms have the latest version of the codebase pulled.
2. Navigate to the ```mp1/src/server``` directory of each vm.
3. Run ```go build``` to build an executable of the server. 
3. Navigate to the ```mp1/src/client/``` directory.
4. Run ```go test``` to run all cases, or ```go test <test name>``` for specific tests.
