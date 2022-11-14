#!/bin/bash

# Copyright (c) HashiCorp, Inc
# SPDX-License-Identifier: MPL-2.0

set -e
# trap 'error ${LINENO}' ERR

rm -rf /workload_dir traces_dir
mkdir /workload_dir traces_dir

env GOMAXPROCS=1 alice-record --workload_dir /workload_dir \
  --traces_dir traces_dir \
  bin/workload -dir /workload_dir

alice-check --traces_dir=traces_dir --checker=bin/checker
