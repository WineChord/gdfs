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

PROJECT=gdfs
GOPATH ?= $(shell go env GOPATH)

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""	
  $(error Please set the environment variable GOPATH before running `make`)
endif

GO := GO111MODULE=on go 
GOBUILD := $(GO) build $(BUILD_FLAG) 
# -v: print the names of packages as they are compiled.
# -count n
#     Run each test and benchmark n times (default 1).
#     If -cpu is set, run n times for each GOMAXPROCS value.
#     Examples are always run once.
# https://golang.org/pkg/cmd/go/internal/test/:
# The rule for a match in the cache is that the run involves the same
# test binary and the flags on the command line come entirely from a
# restricted set of 'cacheable' test flags, defined as -cpu, -list,
# -parallel, -run, -short, and -v. If a run of go test has any test
# or non-test flags outside this set, the result is not cached. To
# disable test caching, use any test flag or argument other than the
# cacheable flags. The idiomatic way to disable test caching explicitly
# is to use -count=1.
GOTEST := $(GO) test -v --count=1 --parallel=1 -p=1

TEST_LDFLAGS := "" 

PACKAGE_LIST := go list ./... | grep -vE "cmd" 
PACKAGES := $$($(PACKAGE_LIST))

CURDIR := $(shell pwd) 
export PATH := $(CURDIR)/bin/:$(PATH) 

# Targets 
.PHONY: clean test dev datanode namenode client

default: namenode datanode client

dev: default test 

test:
	@echo "Running test in native mode."
	@export TZ='Asia/Shanghai';\
	LOG_LEVEL=fatal $(GOTEST) -cover $(PACKAGES)

snamenode:
	@echo "Starting namenode"
	bin/namenode

client:
	$(GOBUILD) -o bin/client cmd/client/main.go 

datanode:
	$(GOBUILD) -o bin/datanode cmd/datanode/main.go 

namenode:
	$(GOBUILD) -o bin/namenode cmd/namenode/main.go

ci: default 
	@echo "Checking formatting"
	@test -z "$$(gofmt -s -l $$(find . -name '*.go' -type f -print) | tee /dev/stderr)"
	@echo "Running Go vet"
	@go vet ./...

format:
	@gofmt -s -w `find . -name '*.go' -type f ! -path '*/_tools/*' -print`

clean:
	$(GO) clean -i ./...
	rm -rf ./bin 
