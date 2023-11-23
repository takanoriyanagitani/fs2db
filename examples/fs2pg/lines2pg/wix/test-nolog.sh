#!/bin/sh

dbname=testdb_2023_11_19_09_49_03

export PGUSER=postgres

listdb() {
	echo "
		SELECT datname
		FROM pg_database
		WHERE datname = :'dbname'
	" | psql \
		--variable=dbname="${dbname}" \
		--no-align \
		--tuples-only \
	  | fgrep \
	  	--silent \
		"${dbname}"
}

createdb(){
	echo "
		CREATE DATABASE :dbname
	" | psql \
		--variable=dbname="${dbname}"
}

droptab(){
	echo "
		DROP TABLE IF EXISTS wix
	" | psql
}

createtab(){
	echo "
		CREATE UNLOGGED TABLE IF NOT EXISTS wix (
			ofst BIGINT NOT NULL,
			id BIGINT NOT NULL,
			title TEXT NOT NULL,
			CONSTRAINT wix_pkc PRIMARY KEY (ofst, id)
		)
	" | psql
}

run(){
	local filename=$1
	readonly filename

	test -f "${filename}" || exec echo test file "${filename}" missing

	ENV_DBNAME="${dbname}" \
	ENV_INPUT_FILENAME="${filename}" \
	time --verbose ./wix \
		2>&1 \
		| grep \
			-e 'User time' \
			-e 'System time' \
			-e 'Elapsed'
}

listdb || createdb

export PGDATABASE="${dbname}"

droptab
createtab

#run index-1024.txt
#run index-16384.txt
run index-131072.txt
