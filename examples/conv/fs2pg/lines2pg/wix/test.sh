#!/bin/sh

listen_addr=127.0.0.1:11301
protodir="fs2db-proto"
bname="${ENV_BNAME:-index-1024.txt}"
bucket=$( echo -n "${bname}" | base64 )

sel() {
	jq \
		-n \
		--arg bkt "${bucket}" \
		-c '{
			bkt: {
				bucket: $bkt
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
