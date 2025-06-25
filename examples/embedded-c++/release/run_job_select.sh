#!/bin/bash


if [ -z "$1" ]; then
    echo "Usage: $0 <query>"
    exit 1
fi

query=$1
thread_num=1
if [ -n "$2" ]; then
    thread_num=$2
fi

job_run_path="./job_script"
query_path="/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/query/parsed/"
parsed_path="${query}"

sync; echo 3 > /proc/sys/vm/drop_caches
$job_run_path "$thread_num" 0 "$query_path" "$parsed_path" 1 > log
python3 parse_time.py 

for queue_thr in 10 100 500 1000; do
    sync; echo 3 > /proc/sys/vm/drop_caches
    $job_run_path "$thread_num" 0 "$query_path" "$parsed_path" 0 0 _select 1 "$queue_thr" > log
    python3 parse_time.py 
    if [ $? -ne 0 ]; then
        echo "Error: Command failed for i=$i, j=$j"
        # exit 1
    fi
    sleep 2
done


for queue_thr in 10 100 500 1000; do
    sync; echo 3 > /proc/sys/vm/drop_caches
    $job_run_path "$thread_num" 0 "$query_path" "$parsed_path" 0 0 _ultra 1 "$queue_thr" > log
    python3 parse_time.py 
    if [ $? -ne 0 ]; then
        echo "Error: Command failed for i=$i, j=$j"
        # exit 1
    fi
    sleep 2
done
