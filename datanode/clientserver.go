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
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/WineChord/gdfs/utils"
)

// SendBlkReply contains status, the argument is BlkData
type SendBlkReply struct {
	Status bool
}

// SendBlk is called by client
// Upon receiving the block data [BlkID, Data, Checksum], datanode will
// store the meta data in metadata path (data/id2meta)
// the actual data will be stored in actual data path (data/actdata)
// for each block, these two files have the same file name: BlkID
// which is of format: filename-index-timestamp-random
// datanode will also update its in memory map: IDToMetaData
func (d *DataNode) SendBlk(args *utils.BlkData, reply *SendBlkReply) error {
	blkID, checksum, data, length := args.BlkID, args.Checksum, args.Data, args.Length
	timestamp := getTimestamp(blkID)
	log.Printf("receive block from client: %v, len: %v\n", blkID, length)
	d.saveMeta(blkID, timestamp, checksum, length)
	d.saveData(blkID, data)
	reply.Status = true
	log.Printf("successfully saved blkData: %v\n", blkID)
	return nil
}

func (d *DataNode) saveData(blkID string, data []byte) {
	log.Printf("start save actual data to file: %v\n", blkID)
	file, err := os.Create(filepath.Join(d.ActPath, blkID))
	if err != nil {
		log.Printf("error when creating actual data file: %v\n", err)
	}
	_, err = file.Write(data)
	if err != nil {
		log.Printf("error when writing actual data file: %v\n", err)
	}
	file.Sync()
	file.Close()
	log.Printf("saved actual data to file %v\n", blkID)
}

func (d *DataNode) saveMeta(blkID, timestamp string, checksum uint32, length int) {
	log.Printf("start save meta data to file: %v\n", blkID)
	meta := utils.MetaData{}
	var err error
	meta.Timestamp, err = strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		log.Printf("error when converting timestamp: %v\n", err)
	}
	meta.Checksum = checksum
	meta.Length = int64(length)
	d.IDToMetaData[blkID] = meta
	file, err := os.Create(filepath.Join(d.MetaPath, blkID))
	if err != nil {
		log.Printf("error when creating metadata file: %v\n", err)
	}
	bytes, err := json.Marshal(meta)
	if err != nil {
		log.Printf("error when marshaling meta data to json: %v\n", err)
	}
	_, err = file.Write(bytes)
	if err != nil {
		log.Printf("error when writing metadata to file: %v\n", err)
	}
	file.Sync()
	file.Close()
	log.Printf("saved meta data to file %v\n", blkID)
}

func getTimestamp(blkID string) string {
	// blkID of format:
	//    filename-index-timestamp-random
	return strings.Split(blkID, "-")[2]
}

func (d *DataNode) serveClients() {
	serv := rpc.NewServer()
	serv.Register(d)
	oldMux := http.DefaultServeMux
	mux := http.NewServeMux()
	http.DefaultServeMux = mux
	serv.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	http.DefaultServeMux = oldMux
	l, e := net.Listen("tcp", d.Addr) // ip:11170 (datanode port)
	log.Printf("DataNode listening to %v\n", d.Addr)
	if e != nil {
		log.Fatal("listen err: ", e)
	}
	go http.Serve(l, mux)
	for {
		// wait
	}
}
