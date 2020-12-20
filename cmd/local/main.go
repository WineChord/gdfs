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

package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"

	"github.com/WineChord/gdfs/utils"
)

func main() {
	start := utils.GetCurrentTimeInMs()
	file, _ := os.Open("perline.txt")
	s := bufio.NewScanner(file)
	cnt := int64(0)
	tot := int64(0)
	sq := float64(0)
	for s.Scan() {
		n, _ := strconv.Atoi(s.Text())
		cnt++
		tot += int64(n)
		sq += float64(n) * float64(n)
	}
	mean := float64(tot) / float64(cnt)
	fmt.Printf("mean: %v, var: %v\n", mean, sq/float64(cnt)-mean*mean)
	fmt.Printf("time elapsed: %v ms\n", utils.GetCurrentTimeInMs()-start)
}
