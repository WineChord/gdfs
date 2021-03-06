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
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/WineChord/gdfs/config"
	"github.com/WineChord/gdfs/utils"
)

// CommandArgs stores command argument for RPC
type CommandArgs struct {
	CommandType int      // type number of command, see config package
	DPath       string   // path in distributed file system
	DPaths      []string // paths in distributed file system
	FileName    string   // file name (both local and dist)
	FileSize    int64    // file size in byte
}

// CommandReply stores reply for RPC
type CommandReply struct {
	Result         string
	Files          []string
	BlkList        []string            // the block names of a file
	BlkToDataNodes map[string][]string // map blockname to datanodes list
}

// RunCommand runs a command on data node
func (n *NameNode) RunCommand(args *CommandArgs, reply *CommandReply) error {
	log.Printf("inside RunCommand\n")
	switch args.CommandType {
	case config.CalMeanVar:
		return n.runCalMeanVar(args, reply)
	case config.Cat:
		return n.runCat(args, reply)
	case config.CopyFromLocal:
		return n.runCopyFromLocal(args, reply)
	case config.CopyToLocal:
		return n.runCopyToLocal(args, reply)
	case config.Ls:
		return n.runLs(args, reply)
	case config.Mkdir:
		return n.runMkdir(args, reply)
	case config.MkdirP:
		return n.runMkdirP(args, reply)
	case config.Rm:
		return n.runRm(args, reply)
	case config.Rmdir:
		return n.runRmdir(args, reply)
	case config.Touch:
		return n.runTouch(args, reply)
	case config.Format:
		return n.runFormat(args, reply)
	default:
		return errors.New("Unsupport command type")
	}
}

func (n *NameNode) runCalMeanVar(args *CommandArgs, reply *CommandReply) error {
	log.Printf("inside runCalMeanVar\n")
	// path := n.makePath(args.DPath) // meta/gdfs/perline.txt
	blkList := n.readDfsFile(args.DPath)
	/** In order to calculate the mean and variance, we need map and reduce
	 * tasks. For map tasks, each segment gets calculated by the datanode holding
	 * that segment. The results are count, mean, and mean square for each segment.
	 * This will result in three files for each segment (count/mean/meansq)
	 * Then we start two reduce tasks:
	 * 	1. read every count and mean files to calculate MEAN (mean of total) and MEAN^2
	 *  2. read every count and meansq files to calculate MEANSQ (mean square of total)
	 * finally we can get variance by MEANSQ - MEAN^2
	 * */
	// Now we've got list of segments to process
	totCnt := int64(0)
	totMean := float64(0)
	totSQ := float64(0)
	var mu sync.Mutex
	finished := 0
	cond := sync.NewCond(&mu)
	for _, blk := range blkList {
		nodes := n.BlkToDatanodes[blk]
		go func(s string, ns []string) {
			for _, nd := range ns {
				if nd == "" {
					continue
				}
				reply, ok := n.reqCalMeanVar(s, n.SID2Addr[nd])
				if ok {
					log.Printf("map result for %v: %v\n", s, reply)
					totCnt += reply.Cnt
					totMean += reply.Mean * float64(reply.Cnt)
					totSQ += reply.MeanSQ * float64(reply.Cnt)
					break
				}
			}
			finished++
			cond.Broadcast()
		}(blk, nodes)
	}
	mu.Lock()
	for finished != len(blkList) {
		cond.Wait()
		log.Printf("calMeanVar map done %v\n", finished)
	}
	mu.Unlock()
	totMean /= float64(totCnt)
	totSQ /= float64(totCnt)
	variance := totSQ - totMean*totMean
	reply.Result = fmt.Sprintf("mean: %v, variance: %v\n", totMean, variance)
	return nil
}

func (n *NameNode) reqCalMeanVar(blk string, addr string) (utils.CalMVReply, bool) {
	args := utils.CalMVArgs{}
	args.BlkID = blk
	reply := utils.CalMVReply{}
	c, err := rpc.DialHTTP("tcp", addr)
	log.Printf("request calMeanVar for %v from %v\n", blk, addr)
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	err = c.Call("DataNode.CalMeanVarMap", &args, &reply)
	if err != nil {
		log.Fatal("Calling: ", err)
	}
	return reply, true
}

func (n *NameNode) runCat(args *CommandArgs, reply *CommandReply) error {
	//
	log.Printf("inside runCat\n")
	return nil
}

func (n *NameNode) runCopyFromLocal(args *CommandArgs, reply *CommandReply) error {
	log.Printf("inside runCopyFromLocal\n")
	path := n.makePath(args.DPath) // meta/gdfs/
	fileinfo, err := os.Stat(path)
	if err != nil {
		return err
	}
	if fileinfo.IsDir() == false {
		return errors.New("The destination of copyFromLocal should be a directory")
	}
	distFilePath := filepath.Join(path, args.FileName)
	// distFilePath := path + string(os.PathSeparator) + args.FileName // meta/gdfs//
	log.Printf("local file name: %v\n", args.FileName)
	log.Printf("distFilePath: %v\n", distFilePath)
	fileinfo, err = os.Stat(distFilePath)
	if err == nil && fileinfo.IsDir() == false {
		return errors.New("File exists")
	}
	/** Should divide files into segments, segment size see configuration (e.g. 4KB)
	 * We maintain a file -> list of segments map
	 * each segment's name is of format:
	 * 	originalFileName-00000000-timestamp-random  (8 numbers, configurable)
	 * 	originalFileName-00000001-timestamp-random
	 *   ...
	 * for each segment, we randomly select R (replica number) nodes to store
	 * the segment. the nodes is stored as address(ip:port) for convenience.
	 * Therefore, each segment has a list:
	 *     [addr1, addr2, addr3]
	 * Overall, it looks like:
	 * 	  segmentFileName0: [addr1, addr2, addr3]
	 *    segmentFileName1: [arrr3, addr4, addr2]
	 *    ...
	 * this map is returned back to client.
	 * That is, we will not split data here, the namenode will only calculate
	 * how each segment is placed on datanodes. It will not try to do the actual
	 * data split and it will not send any data segments directly to datanode.
	 * Therefore, the only crucial thing in argument from client is FileSize.
	 * */
	numBlks := int((args.FileSize-1)/int64(config.BlkSize) + 1)
	reply.BlkToDataNodes = make(map[string][]string)
	reply.BlkList = make([]string, 0)
	log.Printf("number of blocks: %v, totalsize: %v, block size: %v\n", numBlks,
		args.FileSize, config.BlkSize)
	log.Printf("current nodes available: %v\n", len(n.Addr2SID))
	log.Printf("%v\n", n.Addr2SID)
	for i := 0; i < numBlks; i++ {
		segmentName := generateSegName(args.FileName, i)
		// reply.BlkList is needed because we need an orded list of segment
		// file names. The map itself is unordered.
		reply.BlkList = append(reply.BlkList, segmentName)
		nodeList := make([]string, 0)
		for addr := range n.Addr2SID {
			// because map is random in Go, therefore we directly use for to
			// generate 3 random nodes
			if len(nodeList) >= config.ReplicationFactor {
				break
			}
			nodeList = append(nodeList, addr)
		}
		reply.BlkToDataNodes[segmentName] = nodeList
		log.Printf("%v seg: %v, list: %v\n", args.FileName, segmentName, nodeList)
	}
	// here namenode should not update its BlkToDatanodes map, since data hasn't
	// been stored on datanode yet. the information will be updated when datanode
	// has stored the replica.
	// However, it will store the file->blocks map on disk
	// file->blocks will be stored as json files on disk
	file, err := os.Create(distFilePath)
	if err != nil {
		log.Printf("error when creating dist file: %v\n", err)
	}
	bytes, err := json.Marshal(reply.BlkList)
	_, err = file.Write(bytes)
	if err != nil {
		log.Printf("error when writing seg names to json file: %v\n", err)
	}
	file.Sync()
	file.Close()
	return nil
}

func generateSegName(filename string, index int) string {
	timestamp := strconv.Itoa(int(utils.GetCurrentTimeInMs()))
	random := strconv.Itoa(rand.Int())
	// of format: filename-index-timestamp-random
	return filename + "-" + fmt.Sprintf("%08d", index) + "-" + timestamp + "-" + random
}

func (n *NameNode) runCopyToLocal(args *CommandArgs, reply *CommandReply) error {
	log.Printf("inside runCopyToLocal\n")
	/** called by client, the crucial argument is dfs path
	 * namenode will retrieve [segment files] from that file (json format)
	 * and the construct a map from segment file -> [datanods]
	 * */
	dfsPath := args.DPath
	reply.BlkList = n.readDfsFile(dfsPath)
	reply.BlkToDataNodes = make(map[string][]string)
	for _, blk := range reply.BlkList {
		reply.BlkToDataNodes[blk] = make([]string, 0)
		for _, sid := range n.BlkToDatanodes[blk] {
			reply.BlkToDataNodes[blk] = append(reply.BlkToDataNodes[blk], n.SID2Addr[sid])
		}
	}
	return nil
}

func (n *NameNode) readDfsFile(dfsPath string) []string {
	log.Printf("read dfs file %v\n", dfsPath)
	path := n.makePath(dfsPath) // meta/gdfs/mytext.txt
	log.Printf("read dfs actual path: %v\n", path)
	file, err := os.Open(path)
	if err != nil {
		log.Printf("error when opening dfs file: %v\n", err)
	}
	var res []string
	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("error reading dfs file: %v\n", err)
	}
	json.Unmarshal(bytes, &res)
	log.Printf("reading dfs file seg list: %v\n", res)
	return res
}

func (n *NameNode) runLs(args *CommandArgs, reply *CommandReply) error {
	log.Printf("inside runLs\n")
	reply.Result = "running ls"
	path := n.makePath(args.DPath)
	fileinfo, err := os.Stat(path)
	if err != nil {
		return errors.New("No such file or directory")
	}
	if fileinfo.IsDir() == false {
		return errors.New("Not a directory")
	}
	files, err := ioutil.ReadDir(path)
	if reply.Files == nil {
		reply.Files = []string{}
	}
	for _, file := range files {
		reply.Files = append(reply.Files, file.Name())
	}
	return err
}

func (n *NameNode) runMkdir(args *CommandArgs, reply *CommandReply) error {
	//
	log.Printf("inside runMkdir\n")
	reply.Result = "running mkdir"
	err := os.Mkdir(n.makePath(args.DPath), 0700)
	return err
}

func (n *NameNode) runMkdirP(args *CommandArgs, reply *CommandReply) error {
	//
	log.Printf("inside runMkdirP\n")
	reply.Result = "running mkdirP"
	err := os.MkdirAll(n.makePath(args.DPath), 0700)
	return err
}

func (n *NameNode) runRm(args *CommandArgs, reply *CommandReply) error {
	//
	log.Printf("inside runRm\n")
	reply.Result = "running rm"
	for _, file := range args.DPaths {
		err := os.Remove(n.makePath(file))
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *NameNode) runRmdir(args *CommandArgs, reply *CommandReply) error {
	//
	log.Printf("inside runRmdir\n")
	reply.Result = "running rmdir"
	for _, dir := range args.DPaths {
		err := os.RemoveAll(n.makePath(dir))
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *NameNode) runTouch(args *CommandArgs, reply *CommandReply) error {
	//
	log.Printf("inside runTouch\n")
	reply.Result = "running touch"
	return nil
}

func (n *NameNode) runFormat(args *CommandArgs, reply *CommandReply) error {
	//
	log.Printf("inside runFormat\n")
	reply.Result = "running format"
	n.format()
	return nil
}

func (n *NameNode) makePath(path string) string {
	return filepath.Join(n.DFSRootPath, path)
}

// NotifyArgs for client to notify namenode
type NotifyArgs struct {
	// empty
}

// NotifyReply reply status
type NotifyReply struct {
	Status bool
}

func (n *NameNode) notify() {
	n.mu.Lock()
	n.RequestBlk = true
	n.mu.Unlock()

	time.Sleep(time.Second * time.Duration(config.HeartBeatInSec))

	n.mu.Lock()
	n.RequestBlk = false
	n.mu.Unlock()
}

// Notify is called by client
func (n *NameNode) Notify(args *NotifyArgs, reply *NotifyReply) error {
	go n.notify()
	reply.Status = true
	return nil
}
