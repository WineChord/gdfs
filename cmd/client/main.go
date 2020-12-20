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
	"fmt"
	"log"
	"net/rpc"
	"os"

	"github.com/WineChord/gdfs/config"
	"github.com/WineChord/gdfs/namenode"
)

var c *rpc.Client

func printHelp() {
	fmt.Printf("Usage:\n")
	fmt.Printf("\t-appendToFile <localsrc> ... <dst>\n")
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
}

func runCopyToLocal() {
	log.Printf("enter runCopyToLocal\n")
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
