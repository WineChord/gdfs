// Copyright 2020 Qizhou Guo
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"log"
	"net/rpc"
	"os"

	"github.com/WineChord/gdfs/config"
	"github.com/WineChord/gdfs/datanode"
	"github.com/WineChord/gdfs/namenode"
	"github.com/WineChord/gdfs/utils"
)

var c *rpc.Client

func printHelp() {
	fmt.Printf("Usage:\n")
	fmt.Printf("\t-appendToFile <localsrc> ... <dst>\n")
	fmt.Printf("\t-calMeanVar <dst>\n")
	fmt.Printf("\t-cat <src>\n")
	fmt.Printf("\t-checksum <src> ...\n")
	fmt.Printf("\t-copyFromLocal <localsrc> <dst>\n")
	fmt.Printf("\t-copyToLocal <src> <localdst>\n")
	fmt.Printf("\t-cp <src> ... <dst>\n")
	fmt.Printf("\t-head <file>\n")
	fmt.Printf("\t-help [cmd ...]\n")
	fmt.Printf("\t-ls <path>\n")
	fmt.Printf("\t-mkdir [-p] <path>\n")
	fmt.Printf("\t-moveFromLocal <localsrc> ... <dst>\n")
	fmt.Printf("\t-moveToLocal <src> <localdst>\n")
	fmt.Printf("\t-mv <src> ... <dst>\n")
	fmt.Printf("\t-rm <src> ...\n")
	fmt.Printf("\t-rmdir <dir> ...\n")
	fmt.Printf("\t-stat <path> ...\n")
	fmt.Printf("\t-tail <file>\n")
	fmt.Printf("\t-touch <path> ...\n")
	fmt.Printf("\t-usage [cmd ...]\n")
}

func runCalMeanVar() {
	start := utils.GetCurrentTimeInMs()
	log.Printf("runCalMean\n")
	if len(os.Args) != 3 {
		log.Fatalf("calMean expects 1 argument <dst>, got %v\n",
			len(os.Args)-2)
	}
	dfsPath := os.Args[2]
	args := namenode.CommandArgs{}
	args.CommandType = config.CalMeanVar
	args.DPath = dfsPath
	reply := namenode.CommandReply{}
	log.Printf("called with args: %v\n", args)
	err := c.Call("NameNode.RunCommand", &args, &reply)
	if err != nil {
		log.Fatal("Calling: ", err)
	}
	log.Printf("result returned from server: %v\n", reply.Result)
	log.Printf("time elapsed: %v ms\n", utils.GetCurrentTimeInMs() - start)
}

func runCat() {
	log.Printf("enter runCat\n")
}

func runCopyFromLocal() {
	log.Printf("enter runCopyFromLocal\n")
	if len(os.Args) != 4 {
		log.Fatalf("copyFromLocal expects 2 arguments <localsrc> <dst>, got %v\n",
			len(os.Args)-2)
	}
	// name.txt, /
	localPath, dfsPath := os.Args[2], os.Args[3]
	fileinfo, err := os.Stat(localPath)
	if err != nil {
		log.Fatal("error when get file information", err)
	}
	fileSize := fileinfo.Size() // size in byte
	args := namenode.CommandArgs{}
	args.CommandType = config.CopyFromLocal
	args.DPath = dfsPath // '/'
	args.FileSize = fileSize
	args.FileName = fileinfo.Name()
	reply := namenode.CommandReply{}
	log.Printf("called with args: %v\n", args)
	err = c.Call("NameNode.RunCommand", &args, &reply)
	if err != nil {
		log.Fatal("Calling: ", err)
	}
	log.Printf("reply from server (segment name: [list of nodes]):\n")
	for _, seg := range reply.BlkList {
		log.Printf("%v: %v\n", seg, reply.BlkToDataNodes[seg])
	}
	/** Here we've got:
	 * list of segment names: [segname0, segnamt1, ...]
	 * seg to list of nodes:
	 * 		segname0: [node0, node1, node2]
	 * 		segname1: [node1, node3, node4]
	 *  ...
	 * Then the client will do the actual data splitting,
	 * It will split the file data into segments of fixed length (e.g. 4KB).
	 * For each segment, it will calculate its checksum, then send the
	 * information below to the datanodes in list:
	 * 		1. BlkID (string) format: filename-index-timestamp-random
	 * 		2. BlockData ([]byte)
	 * 		3. checksum (uint32)
	 * */
	// For each segment:
	file, err := os.Open(localPath)
	if err != nil {
		log.Printf("error when opening local file of path %v: %v\n",
			localPath, err)
	}
	for _, blkID := range reply.BlkList {
		data := make([]byte, config.BlkSize)
		n, err := file.Read(data)
		if err != nil {
			log.Printf("reading block %v in file %v: %v\n", blkID, localPath, err)
		}
		checksum := crc32.ChecksumIEEE(data)
		// send [blkId, data, checksum] to each datanode
		for _, addr := range reply.BlkToDataNodes[blkID] {
			args1 := utils.BlkData{}
			args1.BlkID = blkID
			args1.Checksum = checksum
			args1.Data = data
			args1.Length = n
			reply1 := datanode.SendBlkReply{}
			c, err := rpc.DialHTTP("tcp", addr)
			log.Printf("sending %v to %v\n", blkID, addr)
			if err != nil {
				log.Fatal("dialing: ", err)
			}
			err = c.Call("DataNode.SendBlk", &args1, &reply1)
			if err != nil {
				log.Fatal("Calling: ", err)
			}
		}
	}
	// when namenode did the segment naming, it only records file -> segName map
	// but didn't update segName -> [nodes] map, this is because it is possible
	// that the data tranfer happened between client and datanode is broken.
	// Therefore, it is more appropriate to notify namenode after successful
	// transmission of data. notify here in namenode is a simple urgent request
	// for block report to each datanodes.
	notifyNameNode()
}

func notifyNameNode() {
	log.Printf("notify namenode\n")
	args := namenode.NotifyArgs{}
	reply := namenode.NotifyReply{}
	c, err := rpc.DialHTTP("tcp", config.NameNodeAddress)
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	err = c.Call("NameNode.Notify", &args, &reply)
	if err != nil {
		log.Fatal("Calling: ", err)
	}
}

func runCopyToLocal() {
	log.Printf("enter runCopyToLocal\n")
	if len(os.Args) != 4 {
		log.Fatalf("copyToLocal expects 2 arguments <dst> <localsrc>, got %v\n",
			len(os.Args)-2)
	}
	/** copyToLocal will first send request to namenode with dfsPath
	 * namenode stores
	 * 	1. dfsPath -> [segmentFiles] mapping
	 *  2. segmentFiles -> [datanodes] mapping
	 * we retrieve [segmentFiles] of this file and [datanotes] for
	 * each segment.
	 * we request each segment on the list of datanodee and append
	 * each segment to local disk.
	 * */
	dfsPath, localFilePath := os.Args[2], os.Args[3]
	args := namenode.CommandArgs{}
	args.CommandType = config.CopyToLocal
	args.DPath = dfsPath // '/'
	reply := namenode.CommandReply{}
	log.Printf("called with args: %v\n", args)
	err := c.Call("NameNode.RunCommand", &args, &reply)
	if err != nil {
		log.Fatal("Calling: ", err)
	}
	log.Printf("retrieve dfs file segments and datanodes:\n")
	for _, seg := range reply.BlkList {
		log.Printf("%v: %v\n", seg, reply.BlkToDataNodes[seg])
	}
	/** Now we've got two things from reply:
	 * 1. blk list for a dfs file
	 * 2. datanodes list for each block
	 * now we need to perform the following operations:
	 * For each block:
	 * 	1. select a datanode from list
	 *  2. request data segment from that datanode
	 *  3. will receive: data, timestamp, checksum
	 * 	   then calculate the checksum of the data to compare
	 *     with the received checksum
	 *  4. if checksum do not match, or the datanode takes too
	 *     long time to respond, request another datanode
	 *  5. when we've got intact segment, append it to local file
	 * */
	file, err := os.Create(localFilePath)
	if err != nil {
		log.Printf("error when creating local file: %v\n", err)
	}
	log.Printf("start request segments\n")
	for _, seg := range reply.BlkList {
		log.Printf("reply.BlkToDataNodes[seg]: %v\n", reply.BlkToDataNodes[seg])
		log.Printf("len: %v\n", len(reply.BlkToDataNodes[seg]))
		for _, addr := range reply.BlkToDataNodes[seg] {
			if addr == "" {
				continue
			}
			log.Printf("addr: %v\n", addr)
			data, length, ok := readRemoteBlk(seg, addr)
			if ok { // ok means the data is intact
				writeLocalFile(file, data, length)
			}
		}
	}
	file.Sync()
	file.Close()
	log.Printf("write to local file done\n")
}

func readRemoteBlk(seg, addr string) ([]byte, int, bool) {
	/** we need to request block from addr (a datanode)
	 * the argument is segment name
	 * the reply is BlkData
	 * */
	log.Printf("request block %v from datanode %v\n", seg, addr)
	args := datanode.RequestBlkArgs{}
	args.BlkID = seg
	reply := utils.BlkData{}
	c, err := rpc.DialHTTP("tcp", addr)
	log.Printf("request %v from %v\n", seg, addr)
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	err = c.Call("DataNode.RequestBlk", &args, &reply)
	if err != nil {
		log.Fatal("Calling: ", err)
	}
	checksum := crc32.ChecksumIEEE(reply.Data)
	// if checksum mismatch, corrupted!
	if checksum != reply.Checksum {
		log.Printf("data is corrupted for %v from %v!\n", seg, addr)
		return []byte{}, 0, false
	}
	log.Printf("data is ok for %v from %v\n", seg, addr)
	return reply.Data, reply.Length, true
}

func writeLocalFile(file *os.File, data []byte, length int) {
	// write bytes to local file
	_, err := file.Write(data[:length])
	if err != nil {
		log.Printf("error writing to local file: %v\n", err)
	}
}

func runLs() {
	log.Printf("enter runLs\n")
	if len(os.Args) != 3 {
		log.Fatalf("ls expects 1 argument, got %v\n", len(os.Args)-2)
	}
	path := os.Args[2]
	args := namenode.CommandArgs{}
	args.CommandType = config.Ls
	args.DPath = path
	reply := namenode.CommandReply{}
	err := c.Call("NameNode.RunCommand", &args, &reply)
	if err != nil {
		log.Fatal("Calling: ", err)
	}
	if reply.Files != nil {
		for _, file := range reply.Files {
			fmt.Printf("%v\t", file)
		}
	}
	fmt.Printf("\n")
}

func runMkdir() {
	log.Printf("enter runMkdir\n")
	if len(os.Args) < 3 {
		log.Fatalf("Insufficient number of argument\n")
	}
	if os.Args[2] == "-p" && len(os.Args) == 4 {
		// cool. mkdir -p somepath
	} else if os.Args[2] != "-p" && len(os.Args) == 3 {
		// super cool. mkdir somepath
	} else { // bad :(
		log.Fatalf("Invalid argument\n")
	}
	args := namenode.CommandArgs{}
	if os.Args[2] == "-p" {
		args.CommandType = config.MkdirP
		args.DPath = os.Args[3]
	} else {
		args.CommandType = config.Mkdir
		args.DPath = os.Args[2]
	}
	reply := namenode.CommandReply{}
	err := c.Call("NameNode.RunCommand", &args, &reply)
	if err != nil {
		log.Fatal("Calling: ", err)
	}
}

func runRm() {
	log.Printf("enter runRm\n")
}

func runRmdir() {
	log.Printf("enter runRmdir\n")
	if len(os.Args) < 3 {
		log.Fatalf("Insufficient number of argument\n")
	}
	args := namenode.CommandArgs{}
	reply := namenode.CommandReply{}
	args.CommandType = config.Rmdir
	args.DPaths = os.Args[2:]
	err := c.Call("NameNode.RunCommand", &args, &reply)
	if err != nil {
		log.Fatal("Calling: ", err)
	}
}

func runTouch() {
	log.Printf("enter runTouch\n")
}

func runFormat() {
	log.Printf("enter runFormat\n")
	if len(os.Args) != 2 {
		log.Fatalf("format expects no argument, got %v\n", len(os.Args)-2)
	}
	args := namenode.CommandArgs{}
	args.CommandType = config.Format
	reply := namenode.CommandReply{}
	err := c.Call("NameNode.RunCommand", &args, &reply)
	if err != nil {
		log.Fatal("Calling: ", err)
	}
	log.Printf("Format succeed!\n")
}

func main() {
	gob.Register(utils.BlkData{})
	if len(os.Args) == 1 {
		printHelp()
	}
	var err error
	c, err = rpc.DialHTTP("tcp", config.NameNodeAddress)
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	defer c.Close()
	switch os.Args[1] {
	case "-calMeanVar":
		runCalMeanVar()
	case "-cat":
		runCat()
	case "-copyFromLocal":
		runCopyFromLocal()
	case "-copyToLocal":
		runCopyToLocal()
	case "-help", "help", "-h":
		printHelp()
	case "-ls":
		runLs()
	case "-mkdir":
		runMkdir()
	case "-rm":
		runRm()
	case "-rmdir":
		runRmdir()
	case "-touch":
		runTouch()
	case "format", "-format":
		runFormat()
	default:
		fmt.Printf("%q is not a valid command.\n", os.Args[1])
		os.Exit(2)
	}
}
