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

package datanode

import (
	"bufio"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"syscall"
	"time"

	"github.com/WineChord/gdfs/config"
	"github.com/WineChord/gdfs/namenode"
	"github.com/WineChord/gdfs/utils"
)

// DataNode contains block names and
// block name to metadata mapping
// IDToMetaData will be persistent on disk
// IDList is restored as IDToMetaData.keys()
type DataNode struct {
	DataPath string
	// Assigned after each format.
	// When DataNode first starts, it will perform a
	// handshake with NameNode. During this process
	// NamespaceID will be verified. (also software version
	// as described in paper, but I omit it)
	NamespaceID int
	// Persistent to disk, generated when DataNode first
	// registers with NameNode
	StorageID string
	HostName  string // e.g. thumm02
	IP        string
	Port      string
	Addr      string
	/* Each block has tow files on DataNode:
	 * 1. metadata file
	 * 2. actual data file
	 * Since DataNode will be requested with block id to
	 * retrieve data on disk, there is no need to store
	 * these meta information in memory.
	 * When a DataNode starts, it performs the following
	 * actions:
	 * 1. perform handshake with NameNode, verify NamespaceID.
	 *    NamespaceID will be reassigned after each format action.
	 * 2. register with NameNode, DataNode get a unique
	 *    StorageID, which is persistent to disk. So if
	 *    the DataNode restart with different IP, it will
	 *    still be able to work.
	 * 3. send a block report to NameNode. Report each block's
	 *    blockID, generation stamp(here I use timestamp instead)
	 *	  and block length. block report then is send periodically
	 *    to NameNode. (every hour as described in paper)
	 * 4. start sending heartbeats to NameNode. (every 3 seconds in paper)
	 *    Each heartbeat carries: total storage capacity, fraction
	 *    of storage in use, # of data transfer in progress.
	 *    Also report corrupt block to NameNode.
	 *    DataNode is considered died if NameNode hasn't received
	 *    its heartbeat for a very long time. (10 mins in paper)
	 */
	// IDList       []string
	// IDToMetaData map[string]MetaData
}

// MetaData stores checksum and timestamp of a file
type MetaData struct {
	Checksum  uint32 // crc checksum
	Timestamp int64  // timestamp in millisecond
}

// NewDataNode retrieve NamespaceID and StorageID on disk
// (if exist)
func NewDataNode() *DataNode {
	d := &DataNode{}
	d.init()
	return d
}

func (d *DataNode) init() {
	log.Printf("start initializing datanode...\n")
	d.DataPath = config.DataPath
	ex, err := utils.Exists(d.DataPath)
	if err != nil {
		log.Printf("error with data node path: %v\n", err)
	}
	d.NamespaceID = -1
	d.StorageID = ""
	if !ex {
		log.Printf("create datapath for datanode: %v\n", d.DataPath)
		os.MkdirAll(d.DataPath, 0700)
	} else {
		// try read NamespaceID and StorageID from disk
		d.tryReadNamespaceID()
		d.tryReadStorageID()
	}
	d.getAddress()
	log.Printf("datanode %v is successfully initialized\n", d.HostName)
	log.Printf("addr: %v, datapath: %v, nid: %v, sid: %v", d.Addr, d.DataPath,
		d.NamespaceID, d.StorageID)
}

func (d *DataNode) getAddress() {
	name, err := os.Hostname() // should be thumm0[1-5] :)
	if err != nil {
		log.Printf("error when getting hostname: %v\n", err)
	}
	d.HostName = name
	addrs, err := net.LookupHost(name)
	if err != nil {
		log.Printf("error when looking up %v: %v\n", name, err)
	}
	d.IP = addrs[0] // I will take the first one :)
	d.Port = config.DataNodePort
	d.Addr = d.IP + ":" + d.Port
	log.Printf("datanode information: %v %v:%v\n", name, d.IP, d.Port)
}

func (d *DataNode) tryReadNamespaceID() {
	log.Printf("try to read NamespaceID on disk from %v\n", config.NamespaceIDPath)
	f, err := os.Open(config.NamespaceIDPath)
	defer f.Close()
	if err == nil {
		s := bufio.NewScanner(f)
		if s.Scan() {
			n, err := strconv.Atoi(s.Text())
			if err == nil {
				d.NamespaceID = n
				log.Printf("got NamespaceID from disk: %v\n", d.NamespaceID)
			}
		}
	}

}

func (d *DataNode) tryReadStorageID() {
	log.Printf("try to read StorageID on disk from %v\n", config.StorageIDPath)
	f, err := os.Open(config.StorageIDPath)
	defer f.Close()
	if err == nil {
		s := bufio.NewScanner(f)
		if s.Scan() {
			d.StorageID = s.Text()
			log.Printf("got StorageID from disk: %v\n", d.StorageID)
		}
	}
}

func (d *DataNode) dumpNID() {
	log.Printf("dump NamespaceID to disk\n")
	f, err := os.Create(config.NamespaceIDPath)
	defer f.Close()
	if err != nil {
		log.Fatalf("err when creating nid file for datanode: %v\n", err)
	}
	w := bufio.NewWriter(f)
	w.WriteString(strconv.Itoa(d.NamespaceID))
	w.Flush()
	log.Printf("dump NamespaceID done\n")
}

func (d *DataNode) dumpSID() {
	log.Printf("dump StorageID to disk\n")
	f, err := os.Create(config.StorageIDPath)
	defer f.Close()
	if err != nil {
		log.Fatalf("err when creating sid file for datanode: %v\n", err)
	}
	w := bufio.NewWriter(f)
	w.WriteString(d.StorageID)
	w.Flush()
	log.Printf("dump StorageID done\n")
}

func (d *DataNode) handshakeWithNameNode() {
	log.Printf("%v starts to handshake with namenode with nid: %v, addr: %v\n",
		d.HostName, d.NamespaceID, d.Addr)
	args := namenode.HandshakeArgs{NamespaceID: d.NamespaceID, Addr: d.Addr,
		HostName: d.HostName}
	reply := namenode.HandshakeReply{}
	c, err := rpc.DialHTTP("tcp", config.NameNodeAddress)
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	err = c.Call("NameNode.Handshake", &args, &reply)
	if err != nil {
		log.Fatal("Calling: ", err)
	}
	d.NamespaceID = reply.NamespaceID // update nid
	log.Printf("%v got NamespaceID from namenode: %v", d.HostName, d.NamespaceID)
	if args.NamespaceID != reply.NamespaceID {
		d.dumpNID() // persistent to disk
	}
}

func (d *DataNode) registerWithNameNode() {
	// register with NameNode, DataNode get a unique
	// StorageID, which is persistent to disk. So if
	// the DataNode restart with different IP, it will
	// still be able to work.
	// First we should check whether we have got a
	// StorageID locally. If true, that means we have
	// already assigned a storage id by the namenode.
	// Then all we have to do is to report our storage
	// id to namenode. Otherwise we report our storage
	// id with an empty string to request name to assign
	// one.
	log.Printf("%v starts to register with namenode with sid: %v, addr: %v\n",
		d.HostName, d.StorageID, d.Addr)
	args := namenode.RegisterArgs{}
	args.HostName = d.HostName
	args.Addr = d.Addr
	args.StorageID = d.StorageID
	reply := namenode.RegisterReply{}
	c, err := rpc.DialHTTP("tcp", config.NameNodeAddress)
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	err = c.Call("NameNode.Register", &args, &reply)
	if err != nil {
		log.Fatal("Calling: ", err)
	}
	d.StorageID = reply.StorageID // update nid
	log.Printf("%v got StorageID from namenode: %v", d.HostName, d.StorageID)
	if args.StorageID == "" {
		d.dumpSID() // persistent to disk
	}
}

func (d *DataNode) sendHeartBeat() {
	log.Printf("sends heartbeat to namenode\n")
	var stat syscall.Statfs_t
	wd, err := os.Getwd()
	if err != nil {
		log.Printf("error when getting root path name: %v\n", err)
	}
	err = syscall.Statfs(wd, &stat)
	if err != nil {
		log.Printf("error when getting fs stat: %v\n", err)
	}
	// total size in bytes = total block number * block size
	TotalSize := stat.Blocks * uint64(stat.Bsize) // uint64
	// fraction in use = available blocks / total blocks
	FracInUse := float64(stat.Blocks-stat.Bavail) / float64(stat.Blocks) // float64
	// number of data transfer in progress
	NumDataTrans := 0 // int
	args := namenode.HeartBeatArgs{}
	args.HostName = d.HostName
	args.Addr = d.Addr
	args.TotalCapacity = TotalSize
	args.FracInUse = FracInUse
	args.NumDataTrans = NumDataTrans
	reply := namenode.HeartBeatReply{}
	c, err := rpc.DialHTTP("tcp", config.NameNodeAddress)
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	err = c.Call("NameNode.HeartBeat", &args, &reply)
	if err != nil {
		log.Fatal("Calling: ", err)
	}
	log.Printf("heartbeat reply from namenode:\n"+
		"len(RepBlk): %v, len(RmBlk): %v, ReRegister: %v, ShutDown: %v"+
		"ReqBlkRep: %v\n", len(reply.RepBlkToNodes), len(reply.RmBlk),
		reply.ReRegister, reply.Shutdown, reply.ReqBlkReport)
}

// Run first perform handshake with NameNode,
// then register with NameNode to get storage id
func (d *DataNode) Run() {
	log.Printf("datanode starts running...\n")
	// perform handshake with NameNode
	d.handshakeWithNameNode()
	d.registerWithNameNode()
	for {
		d.sendHeartBeat()
		time.Sleep(time.Second * time.Duration(config.HeartBeatInSec))
	}
}
