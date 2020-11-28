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
	"time"
)

// INodeType represents type of inode
type INodeType int

const (
	// Dir for directory type
	Dir INodeType = iota
	// File for normal file type
	File
)

type inode struct {
	inodeTyep INodeType

	// file properties
	sizeInKB uint64   // only file type has size and fileBlks
	fileBlks []string // file block list
	// dir properties
	children []*inode // only for directory

	// common
	name         string
	permission   uint32
	modification bool
	accessTime   time.Time
}
