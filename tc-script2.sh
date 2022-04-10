#!/bin/bash

tc qdisc add dev eth0 root handle 1: htb
tc class add dev eth0 parent 1: classid 1:50 htb rate 50mbit

tc filter add dev eth0 parent 1:0 protocol ip handle 400mbit fw flowid 1:50