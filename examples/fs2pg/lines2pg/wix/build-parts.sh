#!/bin/sh

ixname=enwiki-20231101-pages-articles-multistream-index.txt

cat "${ixname}" |
	sed \
	-n \
	-f \
	build-parts.sed
