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

package config

import "os"

var (
	thumm01      = "192.168.0.101"
	thumm02      = "192.168.0.102"
	thumm03      = "192.168.0.103"
	thumm04      = "192.168.0.104"
	thumm05      = "192.168.0.105"
	nameNodeHost = thumm01
	// NameNodePort is the port for name node
	NameNodePort = "21170"
	// DataNodePort is the port for data node
	DataNodePort = "11170"
	// NameNodeAddress is the address for name node
	NameNodeAddress = nameNodeHost + ":" + NameNodePort
	dataNodeHosts   = []string{thumm01, thumm02, thumm03, thumm04, thumm05}
	// DFSRootPath is the local path to file system metadata
	DFSRootPath = "meta/gdfs"
	// NNamespaceIDPath is NameNode's namespace id path
	NNamespaceIDPath = "meta" + string(os.PathSeparator) + "nid"
	// DataPath for datanode to store data block replicas
	DataPath = "data"
	// NamespaceIDPath specifies the path of namespace id
	NamespaceIDPath = DataPath + string(os.PathSeparator) + "nid"
	// StorageIDPath specifies the path of storage id
	StorageIDPath = DataPath + string(os.PathSeparator) + "sid"
	// IDToMetaDataPath is the path for metadata on datanode
	IDToMetaDataPath = DataPath + string(os.PathSeparator) + "id2meta"
	// ActualDataPath is the path for actual data on datanode
	ActualDataPath = DataPath + string(os.PathSeparator) + "actdata"
	// ReplicationFactor specifies number of replicas for each block
	ReplicationFactor = 3
	// BlkSize in byte
	BlkSize = 4096 // 4KB
	// HeartBeatInSec is the frequency of datanode notifies namenode
	HeartBeatInSec = 60
	// BlkReportInSec is the frequency of datanode reporting to namenode
	BlkReportInSec = 60
)

const (
	// Cat for command type
	Cat = iota
	// CopyFromLocal is type number for copyFromLocal command
	CopyFromLocal
	// CopyToLocal is type number for copyToLocal command
	CopyToLocal
	// Ls is type number for ls command
	Ls
	// Mkdir for make directory
	Mkdir
	// MkdirP for make directory with parents
	MkdirP
	// Touch for init an empty file
	Touch
	// Rm remove a list of files
	Rm
	// Rmdir remove a list of dirs
	Rmdir
	// Format for init the dfs
	Format
)
