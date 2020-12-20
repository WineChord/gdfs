# Copyright 2020 Qizhou Guo
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#/bin/bash 

DFSPATH="gdfs"
USERNAME="2019211170"
for ((i=0;i<5;i=i+1));do
    # ssh thumm0$(($i+1)) "mkdir -p $DFSPATH/bin"
    # scp -r bin/datanode thumm0$(($i+1)):~/${DFSPATH}/bin/ 
    ssh thumm0$(($i+1)) \
        "ps -U ${USERNAME} -x | grep bin/datanode | grep -v grep | awk '{print $1}' | xargs kill &"&
done