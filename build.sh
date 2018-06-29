#!/usr/bin/env bash

set -o errexit

# compile specified module
modules=(@collector)

tags=""

# older version Git don't support --short !
#branch=`git symbolic-ref --short -q HEAD`
branch=$(git symbolic-ref -q HEAD | awk -F'/' '{print $3;}')
cid=$(git rev-parse HEAD)
version=$branch","$cid

output=./bin/

# make sure we're in the directory where the script lives
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

rm -rf ${output}

GOPATH=$(pwd)
export GOPATH

#compile_line='-race'
compile_line=''
if [ -z "$DEBUG" ]; then
    DEBUG=0
fi

info="mongoshake/common.VERSION=$version"
# inject program information about compile
if [ $DEBUG -eq 1 ]; then
	echo "[ BUILD DEBUG ]"
	info=$info',debug'
else
	echo "[ BUILD RELEASE ]"
	info=$info",release"
fi

# golang version
goversion=$(go version | awk -F' ' '{print $3;}')
info=$info","$goversion

t=$(date "+%Y-%m-%d_%H:%M:%S")
info=$info","$t

run_builder='go build -v'

for i in "${modules[@]}" ; do
	echo "Build ""$i"
	if [ $DEBUG -eq 1 ]; then
		$run_builder ${compile_line} -ldflags "-X $info" -gcflags='-N -l' -o "bin/$i" -tags "debug" "src/mongoshake//$i/main/$i.go"
	else
		$run_builder ${compile_line} -ldflags "-X $info" -o "bin/$i" "src/mongoshake//$i/main/$i.go"
	fi

	# execute and show compile messages
	if [ -f ${output}/"$i" ];then
		${output}/"$i"
	fi
done

# *.sh
cp scripts/start.sh ${output}/
cp scripts/stop.sh ${output}/
cp scripts/mongoshake-stat ${output}/


if [ "Linux" == "$(uname -s)" ];then
	# hypervisor
	gcc -Wall -O3 scripts/hypervisor.c -o ${output}/hypervisor -lpthread
elif [ "Darwin" == "$(uname -s)" ];then
	printf "\\nWARNING !!! MacOs doesn't supply hypervisor\\n"
fi
