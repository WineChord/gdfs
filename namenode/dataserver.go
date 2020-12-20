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

// Package namenode server requests from datanode and
// client. This file (dataserver.go) mainly serves requests
// from datanodes.
package namenode

import (
	"errors"
	"log"
	"math/rand"
	"strconv"

	"github.com/WineChord/gdfs/utils"
)

// HandshakeArgs is argument for handshake from datanodes
type HandshakeArgs struct {
	NamespaceID int
	Addr        string
	HostName    string
}

// HandshakeReply is reply for handshake from datanodes
type HandshakeReply struct {
	NamespaceID int
}

// Handshake check whether datanode's nid is ok
func (n *NameNode) Handshake(args *HandshakeArgs, reply *HandshakeReply) error {
	log.Printf("namenode receives handshake from %v, %v with %v\n",
		args.HostName, args.Addr, args.NamespaceID)
	if args.NamespaceID == -1 { // datanode newly joined
		log.Printf("datanode %v newly joined, give it %v\n", args.HostName,
			n.NamespaceID)
		// no problem, give it namenode's nid
		reply.NamespaceID = n.NamespaceID
	} else if args.NamespaceID != n.NamespaceID {
		log.Printf("datanode nid %v mismatches namenode nid %v, refuse to join\n",
			args.NamespaceID, n.NamespaceID)
		// too bad, you cannot join this cluster :(
		return errors.New("NID mismatch")
	} else {
		log.Printf("NamespaceID matches: %v, accept join\n", n.NamespaceID)
		// nid match, you can join the cluster :)
		reply.NamespaceID = n.NamespaceID
	}
	return nil
}

// RegisterArgs is argument for datanode to register
// with namenode
type RegisterArgs struct {
	HostName  string
	Addr      string
	StorageID string
}

// RegisterReply contains StorageID uniquely generated
// by namenode
type RegisterReply struct {
	StorageID string
}

// Register handles datanode's registration with namenode
// namenode will generate a unique storage id for datanode
// if datanode doesn't have one. Storage id will be persistent
// both on namenode and datanode.
func (n *NameNode) Register(args *RegisterArgs, reply *RegisterReply) error {
	if args.StorageID == "" { // need to generate a new storage id
		// generate a random unique token
		// send to datanode and persist to disk
		reply.StorageID = generateSID(args.HostName)
		// store the map between storage id and address
	} else {
		reply.StorageID = args.StorageID
	}
	n.SID2Addr[reply.StorageID] = args.Addr
	n.Addr2SID[args.Addr] = reply.StorageID
	return nil
}

func generateSID(hostname string) string {
	// generate a unique storage id for host
	// format: hostname-timestamp-random
	timestamp := strconv.Itoa(int(utils.GetCurrentTimeInMs()))
	randstr := strconv.Itoa(int(rand.Int31()))
	return hostname + "-" + timestamp + "-" + randstr
}

// HeartBeatArgs contains total storage capacity, fraction of
// storage in use and # of data transfer in progress for datanodes
type HeartBeatArgs struct {
	HostName      string
	Addr          string
	TotalCapacity uint64  // in bytes
	FracInUse     float64 // fraction in use
	NumDataTrans  int     // number of data in transfer
}

// HeartBeatReply contains
// 1. instruction to replicate blocks to other nodes
// 2. remove local block replicas
// 3. re-register or shutdown the node
// 4. send an immediate block report
type HeartBeatReply struct {
	// key: block id (string)
	// value: node address (ip in string)
	RepBlkToNodes map[string]string
	// remove local blocks on datanode
	RmBlk []string
	// re-register with namenode
	ReRegister bool
	// shutdown immediately
	Shutdown bool
	// request immediate block report
	ReqBlkReport bool
}

// HeartBeat serves heartbeat message from datanode
// datanode -> namenode with
//  1. total storage capacity
//  2. fraction of storage in use
//  3. number of data transfer in progress
// namenode reply data with
//  1. instruction to replicate blocks to other nodes
//  2. remove local block replicas
//  3. re-register or shutdown the node
//  4. request datanode to send an immediate block report
func (n *NameNode) HeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) error {
	log.Printf("receive heartbeat from %v %v, with \n\ttot cap:%v, "+
		"frac: %v, data trans: %v\n", args.HostName, args.Addr, args.TotalCapacity,
		args.FracInUse, args.NumDataTrans)
	reply.RepBlkToNodes = make(map[string]string)
	reply.RmBlk = make([]string, 0)
	reply.ReRegister = false
	reply.ReqBlkReport = false
	return nil
}

// ReportBlockArgs contains id to metadata information
// map from datanode. metadata contains blockid(key), checksum,
// timestamp and block length
type ReportBlockArgs struct {
	HostName     string
	Addr         string
	IDToMetaData map[string]utils.MetaData
}

// ReportBlockReply contains status: true or false
type ReportBlockReply struct {
	Status bool
}

// ReportBlock will update namenode's BlkToDatanodes
func (n *NameNode) ReportBlock(args *ReportBlockArgs, reply *ReportBlockReply) error {
	log.Printf("receive block report from %v of length: %v\n", args.HostName, len(args.IDToMetaData))
	for id := range args.IDToMetaData {
		if n.BlkToDatanodes[id] == nil {
			n.BlkToDatanodes[id] = make([]string, 0)
		}
		// BlkToDatanodes maps block id to storage id
		n.BlkToDatanodes[id] = append(n.BlkToDatanodes[id], n.Addr2SID[args.Addr])
	}
	reply.Status = true
	return nil
}
