#!/bin/bash

catalog=`dirname $0`

cd ${catalog}/../

if [ $# != 2 ] ; then 
	echo "USAGE: $0 [collector|receiver] [conf]" 
	exit 0
fi 

GOMAXPROCS=0

if [ $GOMAXPROCS != 0 ] ; then 
	./bin/hypervisor --daemon --exec="GOMAXPROCS=$GOMAXPROCS ./bin/$1 -conf=$2 2>&1 1>> $1.output" 2>&1 1>>hypervisor.output
else
	./bin/hypervisor --daemon --exec="./bin/$1 -conf=$2 2>&1 1>> $1.output" 2>&1 1>>hypervisor.output
fi
