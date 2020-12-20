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

# helpFunction()
# {
#    echo ""
#    echo "Usage: $0 -n" # parameterA -b parameterB -c parameterC"
#    echo -e "\t-n do not copy binaries to other nodes"
# #    echo -e "\t-b Description of what is parameterB"
# #    echo -e "\t-c Description of what is parameterC"
#    exit 1 # Exit script after printing help
# }

# while getopts "n:" opt
# do
#    case "$opt" in
#       n ) COPY="true" ;;
#       ? ) helpFunction ;; # Print helpFunction in case parameter is non-existent
#    esac
# done

# Print helpFunction in case parameters are empty
# if [ -z "$parameterA" ] || [ -z "$parameterB" ] || [ -z "$parameterC" ]
# then
#    echo "Some or all of the parameters are empty";
#    helpFunction
# fi
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -n) COPY=1 ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

DFSPATH="gdfs"
for ((i=0;i<5;i=i+1));do
    if [ -z "$COPY" ]
    then 
        ssh thumm0$(($i+1)) "mkdir -p $DFSPATH/bin"
        scp -r bin/datanode thumm0$(($i+1)):~/${DFSPATH}/bin/ 
    fi 
    ssh thumm0$(($i+1)) "cd $DFSPATH && (bin/datanode >> datanode.log 2>&1&)" &
done
