#!/usr/bin/env bash

catalog=$(dirname "$0")

cd "${catalog}" || exit 1

if [ $# != 1 ] ; then
	echo "USAGE: $0 [conf]"
	exit 0
fi

name="collector.linux"

if [ "Darwin" == "$(uname -s)" ];then
	printf "\\nWARNING !!! MacOs doesn't supply to use this script, please use \"./%s -conf=config_file_name\" manual command to run\\n" "$name"
    exit 1
fi

GOMAXPROCS=0

if [ $GOMAXPROCS != 0 ] ; then
	./hypervisor --daemon --exec="GOMAXPROCS=$GOMAXPROCS ./$name -conf=$1 1>> $name.output 2>&1" 1>>hypervisor.output 2>&1
else
	./hypervisor --daemon --exec="./$name -conf=$1 1>> $name.output 2>&1" 1>>hypervisor.output 2>&1
fi
