#!/usr/bin/env sh
set -eu

MANAGER="${MANAGER:-http://127.0.0.1:9000}"

mkdir -p tmp
printf "distributed fs demo\n" > tmp/input.txt

./bin/fs nodes -manager "$MANAGER"
./bin/fs put -manager "$MANAGER" demo.txt tmp/input.txt

# Wait for the background replication loop to copy pending replicas.
sleep 2

./bin/fs stat -manager "$MANAGER" demo.txt
./bin/fs get -manager "$MANAGER" demo.txt tmp/output.txt
cat tmp/output.txt

./bin/fs delete -manager "$MANAGER" demo.txt
