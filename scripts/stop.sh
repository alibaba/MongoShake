#!/usr/bin/env bash
#kill -9 "$(cat "$1")"
if [ $# != 1 ] ; then
    echo "USAGE: $0 [pidfile]"
    exit 0
fi
ppid=$(ps -ef | awk '{if ($2=='`cat $1`') print $3}')
[ -z $ppid ] && echo "[Fail] No process number for $(cat "$1")." && exit 1
if [ $ppid -eq 1 ];then
    kill -9 "$(cat "$1")"
else
    kill -9 "$(cat "$1")" "$ppid"
fi
