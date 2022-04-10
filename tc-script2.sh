#!/bin/bash

sudo /cm/shared/package/utils/bin/run-tc qdisc add dev eth4 root handle 1: htb
sudo /cm/shared/package/utils/bin/run-tc class add dev eth4 parent 1: classid 1:50 htb rate 50mbit

sudo /cm/shared/package/utils/bin/run-tc filter add dev eth4 parent 1:0 protocol ip handle 400mbit fw flowid 1:50
