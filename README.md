[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://golang.org/)

# gDFS
gDFS is a distributed file system in Go.

## Getting Started 

* Download Go 1.15

* clone code 

```shell
$ git clone https://github.com/WineChord/gdfs.git
$ cd gdfs 
```

* inside one terminal, 

```shell
$ make # this will build namenode, datanode, client 
$ make snamenode # this will start the namenode 
```

* inside another terminal,

```shell
$ ./startdatanodes.sh # this will start datanodes on every node 
$ # stopdatanodes.sh # this will stop all datanodes 
```

* inside the 3rd terminal,

```shell
$ bin/client -format # this will format the dfs
$ bin/client -ls / # see whether / dir is empty
$ bin/client -copyFromLocal somefile / # copy local file to dfs /
$ bin/client -copyToLocal /somefile . # copy dfs file to local dir .
$ bin/client -calMeanVal /somefile # calculate mean and variance of the file (list of numbers)
```

## License 

gDFS is under the  Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.