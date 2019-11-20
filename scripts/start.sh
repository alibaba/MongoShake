#!/usr/bin/env bash

catalog=$(dirname "$0")

cd "${catalog}" || exit 1

if [ $# != 1 ] ; then
	echo "USAGE: $0 [conf]"
	exit 0
fi

name="collector"

if [ "Darwin" == "$(uname -s)" ];then
	(./$name.darwin -conf=$1 1>> $name.output 2>&1 &)
elif [ "Linux" == "$(uname -s)" ];then
  (./$name.linux -conf=$1 1>> $name.output 2>&1 &)
else
  (./$name.windows -conf=$1 1>> $name.output 2>&1 &)
fi

#GOMAXPROCS=0
#
#if [ $GOMAXPROCS != 0 ] ; then
#	./hypervisor --daemon --exec="GOMAXPROCS=$GOMAXPROCS ./$name -conf=$1 1>> $name.output 2>&1" 1>>hypervisor.output 2>&1
#else
#	./hypervisor --daemon --exec="./$name -conf=$1 1>> $name.output 2>&1" 1>>hypervisor.output 2>&1
#fi
