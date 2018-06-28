#!/bin/bash

catalog=`dirname $0`

cd ${catalog}/../

if [ $# != 1 ] ; then 
	echo "USAGE: $0 [conf]" 
	exit 0
fi 

name="collector"

if [ "Darwin" == `uname -s` ];then
	echo "\nWARNING !!! MacOs doesn't supply to use this script, please use \"./$name -conf=config_file_name\" manual command to run\n"
    exit 1
fi

GOMAXPROCS=0

if [ $GOMAXPROCS != 0 ] ; then 
	./bin/hypervisor --daemon --exec="GOMAXPROCS=$GOMAXPROCS ./bin/$name -conf=$1 2>&1 1>> $name.output" 2>&1 1>>hypervisor.output
else
	./bin/hypervisor --daemon --exec="./bin/$name -conf=$2 2>&1 1>> $name.output" 2>&1 1>>hypervisor.output
fi
