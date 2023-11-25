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

init(){
	pgbench \
	--unlogged-tables \
	--initialize
}

run(){
	pgbench \
	--client=4 \
	--jobs=1 \
	--protocol=prepared \
	--progress=1 \
	--report-latencies \
	--time=64
}

listdb || createdb

export PGDATABASE="${dbname}"

init && run
