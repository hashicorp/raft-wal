# Copyright (c) HashiCorp, Inc
# SPDX-License-Identifier: MPL-2.0

WORK_DIR=$(shell pwd)

.PHONY: test

test: workload checker
	docker run --privileged \
		--cap-add=SYS_PTRACE \
		--memory=8gb \
		--shm-size=2gb \
		-v $(WORK_DIR):/app \
		-it ghcr.io/banks/alice:master \
		./run-workload.sh

workload:
	GOOS=linux go build -o bin/workload workload/main.go

checker:
	GOOS=linux go build -o bin/checker checker/main.go

# init:
# 	GOOS=linux go build -o bin/init init/main.go
