SHELL := /bin/bash
PROJECT=minidb
GOPATH ?= $(shell go env GOPATH)

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif

GO                  := GO111MODULE=on go
GOTEST              := $(GO) test -v --count=1 --parallel=1 -p=1
TEST_CLEAN          := rm -rf /tmp/minidb*
BENCH_CLEAN          := rm -rf ./benchmark/minidb-bench

test:
	$(TEST_CLEAN)
	$(GOTEST) .

bench:
	$(BENCH_CLEAN)
	$(GOTEST) ./benchmark -bench=. -benchtime=100000x
