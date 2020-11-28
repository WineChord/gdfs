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

import "errors"

// HandshakeArgs is argument for handshake from datanodes
type HandshakeArgs struct {
	NamespaceID int
	Addr        string
}

// HandshakeReply is reply for handshake from datanodes
type HandshakeReply struct {
	NamespaceID int
}

// Handshake check whether datanode's nid is ok
func (n *NameNode) Handshake(args *HandshakeArgs, reply *HandshakeReply) error {
	if args.NamespaceID == -1 { // datanode newly joined
		// no problem, give it namenode's nid
		reply.NamespaceID = n.NamespaceID
	} else if args.NamespaceID != n.NamespaceID {
		// too bad, you cannot join this cluster :(
		return errors.New("NID mismatch")
	} else {
		// nid match, you can join the cluster :)
		reply.NamespaceID = n.NamespaceID
	}
	return nil
}

// RegisterArgs is argument for datanode to register
// with namenode
type RegisterArgs struct {
	StorageID string
	Addr      string
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
	}
	return nil
}
