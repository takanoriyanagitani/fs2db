#!/bin/sh

listen_addr=127.0.0.1:11301
protodir="fs2db-proto"

sel() {
	jq \
		-n \
		-c '{
			bkt: {
				bucket: "aW5kZXgtMTAyNC50eHQ="
			}
	}' |
		grpcurl \
			-plaintext \
			-d @ \
			-import-path "${protodir}" \
			-proto fs2db/proto/source/v1/source.proto \
			"${listen_addr}" \
			fs2db.proto.source.v1.SelectService/All |
		jq \
			-c \
			--raw-output \
			'.key' |
		tail |
		while read line; do
			echo "${line}" | base64 --decode
			echo
		done
}

sel
