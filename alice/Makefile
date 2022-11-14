# Copyright (c) HashiCorp, Inc
# SPDX-License-Identifier: MPL-2.0

WORK_DIR=$(shell pwd)

WORKLOAD ?= append

.PHONY: test test-local

test: bin/workload bin/checker
	docker run --privileged \
		--cap-add=SYS_PTRACE \
		--memory=8gb \
		--shm-size=2gb \
		-v $(WORK_DIR):/app \
		-it ghcr.io/banks/alice:master \
		./run-workload.sh ${WORKLOAD}

test-local:
	mkdir -p ${WORK_DIR}/workload_dir
	echo " -> Running init (${WORKLOAD})"
	go run ${WORK_DIR}/workload/main.go \
		-dir ${WORK_DIR}/workload_dir \
		-workload ${WORKLOAD} \
		-init
	echo " -> Running workload (${WORKLOAD})"
	go run ${WORK_DIR}/workload/main.go \
		-dir ${WORK_DIR}/workload_dir \
		-workload ${WORKLOAD} | tee ${WORK_DIR}/workload_dir/stdout.txt
	echo " -> Running checker"
	go run ${WORK_DIR}/checker/main.go \
		${WORK_DIR}/workload_dir \
		${WORK_DIR}/workload_dir/stdout.txt

bin/workload: workload/main.go
	GOOS=linux go build -o bin/workload workload/main.go

bin/checker: checker/main.go
	GOOS=linux go build -o bin/checker checker/main.go
