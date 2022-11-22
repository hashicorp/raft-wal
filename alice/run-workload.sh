#!/bin/bash

# Copyright (c) HashiCorp, Inc
# SPDX-License-Identifier: MPL-2.0

set -e
# trap 'error ${LINENO}' ERR

WORKLOAD=$1

bold=$(tput bold)
green=$(tput setf 2)
normal=$(tput sgr0)

echo "${green}==> Running Workload ${bold}${WORKLOAD}${normal}"

echo " -> Cleaning up dirs"
rm -rf /workload_dir traces_dir
mkdir /workload_dir traces_dir

echo " -> Running init"
bin/workload -dir /workload_dir -workload "$WORKLOAD" -init

echo " -> Running alice-record"
env GOMAXPROCS=1 alice-record --workload_dir /workload_dir \
  --traces_dir traces_dir \
  bin/workload -dir /workload_dir -workload "$WORKLOAD"

echo " -> Running alice-check"
alice-check --traces_dir=traces_dir --checker=bin/checker
