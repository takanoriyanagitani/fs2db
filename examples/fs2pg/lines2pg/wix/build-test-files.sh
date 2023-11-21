#!/bin/sh

build(){
	local lines=$1
	local prefix=${2:-index-}
	local original=${3:-enwiki-20231101-pages-articles-multistream-index.txt}
	local name="${prefix}${lines}.txt"
	readonly lines
	readonly prefix
	readonly name

	test -f "${name}" && return

	test -f "${original}" || exec echo original file "${original}" missing

	echo building "${name}"...
	head --lines="${lines}" "${original}" > "${name}"
	sync .
}

build 1024
build 16384
build 131072
build 1048576
build 16777216
