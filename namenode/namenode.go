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
	"sync"
	"time"

	"github.com/WineChord/gdfs/config"
	"github.com/WineChord/gdfs/utils"
)

// NameNode stores namespace tree and block to datanodes map
// namespace tree will be persistent on disk
// block to datanodes map is non-persistent,
// it is gathered by receiving reports from datanodes
type NameNode struct {
	// meta/gdfs
	DFSRootPath string
	// maps to storage id rather that address
	BlkToDatanodes map[string][]string
	diskSpaceQuote float32
	NamespaceID    int
	// map storage id to address(ip:port)
	SID2Addr map[string]string
	// map address to storage id
	Addr2SID   map[string]string
	RequestBlk bool
	Format     bool
	mu         sync.Mutex
}

// NewNameNode initializes a namenode
func NewNameNode() *NameNode {
	n := &NameNode{}
	n.BlkToDatanodes = make(map[string][]string)
	n.SID2Addr = make(map[string]string)
	n.Addr2SID = make(map[string]string)
	n.init()
	return n
}

func (n *NameNode) init() {
	log.Printf("namenode starts to initialize\n")
	n.DFSRootPath = config.DFSRootPath
	n.RequestBlk = false
	ex, err := utils.Exists(n.DFSRootPath)
	if err != nil {
		log.Printf("error with dfs root path: %v\n", err)
	}
	log.Printf("set dfs root path as %v\n", n.DFSRootPath)
	if !ex {
		log.Printf("auto format dfs on start\n")
		os.MkdirAll(n.DFSRootPath, 0700)
	}
	ex, err = utils.Exists(config.NNamespaceIDPath)
	if err != nil {
		log.Printf("error with namenode nid file: %v\n", err)
	}
	if ex {
		log.Printf("namenode NamespaceID file %v exists, starts reading\n",
			config.NNamespaceIDPath)
		n.readNID()
	} else {
		log.Printf("namenode NamespaceID file %v doesn't exist, starts creating\n",
			config.NNamespaceIDPath)
		n.initNID()
	}
}

func (n *NameNode) readNID() {
	f, err := os.Open(config.NNamespaceIDPath)
	defer f.Close()
	if err != nil {
		log.Fatalf("error when opening nid for namenode: %v\n", err)
	}
	s := bufio.NewScanner(f)
	if s.Scan() { // file not empty, read nid directly
		n.NamespaceID, err = strconv.Atoi(s.Text())
		if err != nil { // file content not int
			log.Printf("error when reading from nid file, dump new nid\n")
			n.NamespaceID = 1
			n.dumpNID()
		}
	} else { // file empty, dump directly
		log.Printf("nid file is empty, dump new nid\n")
		n.NamespaceID = 1
		n.dumpNID()
	}
	log.Printf("readNID reads %v\n", n.NamespaceID)
}

func (n *NameNode) initNID() {
	f, err := os.Create(config.NNamespaceIDPath)
	defer f.Close()
	if err != nil {
		log.Fatalf("error when creating nid for namenode: %v\n", err)
	}
	w := bufio.NewWriter(f)
	n.NamespaceID = 1 // initialize namespace id to 1
	var cnt int
	cnt, err = w.WriteString(strconv.Itoa(n.NamespaceID))
	log.Printf("%v bytes written to nid file\n", cnt)
	if err != nil {
		log.Printf("error when writing nid to file: %v\n", err)
	}
	err = w.Flush()
	if err != nil {
		log.Printf("error when flush nid to disk: %v\n", err)
	}
	log.Printf("initNID init nid %v\n", n.NamespaceID)
}

func (n *NameNode) dumpNID() {
	log.Printf("insed dumpNID: dump nid %v to %v\n", n.NamespaceID, config.NNamespaceIDPath)
	f, err := os.OpenFile(config.NNamespaceIDPath, os.O_RDWR, 0700)
	defer f.Close()
	if err != nil {
		log.Fatalf("error when creating nid for namenode: %v\n", err)
	}
	w := bufio.NewWriter(f)
	var cnt int
	cnt, err = w.WriteString(strconv.Itoa(n.NamespaceID))
	log.Printf("%v bytes dump to nid file\n", cnt)
	if err != nil {
		log.Printf("error when writing nid file: %v\n", err)
	}
	err = w.Flush()
	if err != nil {
		log.Printf("error when flushing nid to disk: %v\n", err)
	}
}

func (n *NameNode) format() {
	log.Printf("start formatting\n")
	os.RemoveAll(n.DFSRootPath) // meta/gdfs
	os.MkdirAll(n.DFSRootPath, 0700)
	// erase in memory blk -> datanodes map
	n.BlkToDatanodes = make(map[string][]string)
	// namespace id should change when formatted
	// and it should be persistent to disk
	n.NamespaceID++
	n.dumpNID()
	log.Printf("NamespaceID changes to %v after formatting\n", n.NamespaceID)
	n.setFormat()
}

func (n *NameNode) setFormat() {
	log.Printf("set format\n")
	n.mu.Lock()
	n.Format = true
	n.mu.Unlock()

	// eps := time.Duration(config.HeartBeatInSec * 4 / 5)
	eps := time.Duration(50)
	time.Sleep(time.Second*time.Duration(config.HeartBeatInSec) + eps)

	n.mu.Lock()
	n.Format = false
	n.mu.Unlock()
	log.Printf("unset format\n")
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
