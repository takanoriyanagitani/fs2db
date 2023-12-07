#!/bin/sh

getos() {
	local raw
	raw=$(uname -o)
	readonly raw

	case "${raw}" in
	GNU/Linux)
		echo linux
		;;
	Darwin)
		echo osx
		;;
	*)
		echo linux
		;;
	esac
}

getcpu() {
	local raw
	raw=$(uname -m)
	readonly raw

	case "${raw}" in
	arm64)
		echo aarch_64
		;;
	x86_64)
		echo x86_64
		;;
	*)
		echo x86_64
		;;
	esac
}

os=$(getos)
cpu=$(getcpu)

pver=25.1
prefix=https://github.com/protocolbuffers/protobuf/releases/download
zipname="protoc-${pver}-${os}-${cpu}.zip"
url="${prefix}/v${pver}/${zipname}"

echo "${url}"
test -f "${zipname}" || curl \
    --progress-bar \
	--remote-name \
	--location \
	--fail \
	--show-error \
	"${url}"

unzip -p "${zipname}" bin/protoc >./protoc
chmod 755 protoc
which protoc && exec echo 'protoc already exists'
sudo cp -i -v ./protoc /usr/local/bin/
