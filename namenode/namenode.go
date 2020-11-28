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

package namenode

import (
	"bufio"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"

	"github.com/WineChord/gdfs/config"
	"github.com/WineChord/gdfs/utils"
)

// NameNode stores namespace tree and block to datanodes map
// namespace tree will be persistent on disk
// block to datanodes map is non-persistent,
// it is gathered by receiving reports from datanodes
type NameNode struct {
	DFSRootPath    string
	BlkToDatanodes map[string][]string
	diskSpaceQuote float32
	NamespaceID    int
}

// NewNameNode initializes a namenode
func NewNameNode() *NameNode {
	n := &NameNode{}
	n.init()
	return n
}

func (n *NameNode) init() {
	n.DFSRootPath = config.DFSRootPath
	ex, err := utils.Exists(n.DFSRootPath)
	if err != nil {
		log.Printf("error with dfs root path: %v\n", err)
	}
	if !ex {
		log.Printf("auto format dfs on start\n")
		os.MkdirAll(n.DFSRootPath, 0700)
	}
	ex, err = utils.Exists(config.NNamespaceIDPath)
	if err != nil {
		log.Printf("error with namenode nid file: %v\n", err)
	}
	if ex {
		n.readNID()
	} else {
		n.initNID()
	}
}

func (n *NameNode) readNID() {
	f, err := os.Open(config.NNamespaceIDPath)
	defer f.Close()
	if err != nil {
		log.Fatal("error when opening nid for namenode: %v\n", err)
	}
	s := bufio.NewScanner(f)
	if s.Scan() { // file not empty, read nid directly
		n.NamespaceID, err = strconv.Atoi(s.Text())
		if err != nil { // file content not int
			log.Printf("error when reading from nid file")
			n.NamespaceID = 1
			n.dumpNID()
		}
	} else { // file empty, dump directly
		n.NamespaceID = 1
		n.dumpNID()
	}
}

func (n *NameNode) initNID() {
	f, err := os.Create(config.NNamespaceIDPath)
	defer f.Close()
	if err != nil {
		log.Fatalf("error when creating nid for namenode: %v\n", err)
	}
	w := bufio.NewWriter(f)
	n.NamespaceID = 1 // initialize namespace id to 1
	w.WriteString(strconv.Itoa(n.NamespaceID))
}

func (n *NameNode) dumpNID() {
	f, err := os.Open(config.NNamespaceIDPath)
	defer f.Close()
	if err != nil {
		log.Fatalf("error when creating nid for namenode: %v\n", err)
	}
	w := bufio.NewWriter(f)
	w.WriteString(strconv.Itoa(n.NamespaceID))
}

func (n *NameNode) format() {
	os.RemoveAll(n.DFSRootPath)
	os.MkdirAll(n.DFSRootPath, 0700)
	// namespace id should change when formatted
	// and it should be persistent to disk
	n.NamespaceID++
	f, err := os.Open(config.NNamespaceIDPath)
	defer f.Close()
	if err != nil {
		log.Fatalf("error when opening nid for namenode: %v\n", err)
	}
	w := bufio.NewWriter(f)
	w.WriteString(strconv.Itoa(n.NamespaceID))
}

// Run starts a RPC server
func (n *NameNode) Run() {
	serv := rpc.NewServer()
	serv.Register(n)
	oldMux := http.DefaultServeMux
	mux := http.NewServeMux()
	http.DefaultServeMux = mux
	serv.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	http.DefaultServeMux = oldMux
	l, e := net.Listen("tcp", config.NameNodeAddress)
	log.Printf("NameNode listening to %v\n", config.NameNodeAddress)
	if e != nil {
		log.Fatal("listen err: ", e)
	}
	go http.Serve(l, mux)
	for {
		// wait
	}
}
