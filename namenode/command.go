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
	"errors"
	"io/ioutil"
	"log"
	"os"

	"github.com/WineChord/gdfs/config"
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

func (n *NameNode) runCat(args *CommandArgs, reply *CommandReply) error {
	//
	log.Printf("inside runCat\n")
	return nil
}

func (n *NameNode) runCopyFromLocal(args *CommandArgs, reply *CommandReply) error {
	//
	log.Printf("inside runCopyFromLocal\n")
	path := n.makePath(args.DPath)
	fileinfo, err := os.Stat(path)
	if err != nil {
		return err
	}
	if fileinfo.IsDir() == false {
		return errors.New("The destination of copyFromLocal should be a directory")
	}
	fileinfo, err = os.Stat(path + string(os.PathSeparator) + args.FileName)
	if err == nil && fileinfo.IsDir() == false {
		return errors.New("File exists")
	}
	return nil
}

func (n *NameNode) runCopyToLocal(args *CommandArgs, reply *CommandReply) error {
	//
	log.Printf("inside runCopyToLocal\n")
	return nil
}

func (n *NameNode) runLs(args *CommandArgs, reply *CommandReply) error {
	//
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
	return n.DFSRootPath + string(os.PathSeparator) + path
}
