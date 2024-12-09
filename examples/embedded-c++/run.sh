#!/bin/bash

# 设置查询变量
query=9a

# 设置路径变量
job_run_path="./job_run"
query_path="/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/query/"
parsed_path="parsed/${query}.sql"

# 检查 job_run 可执行文件是否存在
if [ ! -f "$job_run_path" ]; then
    echo "Error: $job_run_path not found."
    exit 1
fi
files=$(find . -type f -name 'materialize_info*')

# 循环执行命令
for file in $files; do
    # 提取文件名中的数字
    if [[ $file =~ materialize_info([0-9]+) ]]; then
        i=${BASH_REMATCH[1]}
        echo "Running command for i=$i"
        for j in {1..1}; do
            # 执行命令并检查返回值
            $job_run_path 1 0 "$query_path" "$parsed_path" 0 "$i"
            if [ $? -ne 0 ]; then
                echo "Error: Command failed for i=$i, j=$j"
                exit 1
            fi
        done
    else
        echo "No number found in file name: $file"
    fi
done

echo "All commands executed successfully."