#!/bin/sh

listen_addr=127.0.0.1:11301
src_path_rt="${PWD}"

ENV_SOURCE_PATH_ROOT="${src_path_rt}" \
	ENV_LISTEN_ADDR="${listen_addr}" \
	./wix
