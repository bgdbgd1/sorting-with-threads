#!/bin/bash

sudo /cm/shared/package/utils/bin/run-tc qdisc add dev eth1 root handle 1: prio priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
sudo /cm/shared/package/utils/bin/run-tc qdisc add dev eth1 parent 1:2 handle 20: netem rate 400mbit
sudo /cm/shared/package/utils/bin/run-tc filter add dev eth1 parent 1:0 protocol ip u32 match ip dport 9000 0xffff flowid 1:2
