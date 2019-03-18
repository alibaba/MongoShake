#!/usr/bin/env bash

catalog=$(dirname "$0")

cd "${catalog}"/../ || exit 1

if [ $# != 1 ] ; then
	echo "USAGE: $0 [conf]"
	exit 0
fi

name="collector"

if [ "Darwin" == "$(uname -s)" ];then
	printf "\\nWARNING !!! MacOs doesn't supply to use this script, please use \"./%s -conf=config_file_name\" manual command to run\\n" "$name"
    exit 1
fi

GOMAXPROCS=0

if [ $GOMAXPROCS != 0 ] ; then
	./bin/hypervisor --daemon --exec="GOMAXPROCS=$GOMAXPROCS ./bin/$name -conf=$1 2>&1 1>> $name.output" 1>>hypervisor.output 2>&1
else
	./bin/hypervisor --daemon --exec="./bin/$name -conf=$1 2>&1 1>> $name.output" 1>>hypervisor.output 2>&1
fi
