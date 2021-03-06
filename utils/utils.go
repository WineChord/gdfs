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

package utils

import (
	"os"
	"time"
)

// CalMVArgs is argument for calculating mean and avriance
type CalMVArgs struct {
	BlkID string
}

// CalMVReply is result for each subtask
type CalMVReply struct {
	Cnt    int64
	Mean   float64
	MeanSQ float64 // (\sum x^2)/n
}

// MetaData stores checksum and timestamp of a file
type MetaData struct {
	Checksum  uint32 // crc checksum
	Timestamp int64  // timestamp in millisecond
	Length    int64  // block length
}

// BlkData is used by client to send block data to datanodes
type BlkData struct {
	BlkID    string // of format filename-index-timestamp-random
	Data     []byte // data in bytes
	Checksum uint32 // checksum of data
	Length   int
}

// Exists checks whether a path exist
func Exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil // exists
	}
	if os.IsNotExist(err) {
		return false, nil // file or dir not exists
	}
	return false, err // other error (exclude not exists)
}

// GetCurrentTimeInMs return unix time in ms
func GetCurrentTimeInMs() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
