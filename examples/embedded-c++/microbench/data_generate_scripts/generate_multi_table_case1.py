import sys
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import random
import string
import math
import os
import duckdb

#select k1, k2, C.v1 from A, B, C where A.k1 = B.k1 and B.k2 = C.k2
# |C|<|B|, |C x B| < |A|


A_size = 10000000
B_size = 6000000
C_size = 4000000
B_C_sel = 0.01
A_B_sel = 0.1
payload_size = 10

def gen_k2():
    k2 = np.arange(0, 100000000)
    C_k2 = np.random.choice(k2, int(C_size), replace=False)
    hit_key = np.random.choice(C_k2, int(B_size*B_C_sel), replace=True)
    non_hit_key = np.setdiff1d(k2, C_k2)
    probe_non_hit = np.random.choice(non_hit_key, int(B_size - int(B_size*B_C_sel)), replace=True)
    B_k2 = np.concatenate((hit_key, probe_non_hit))
    return B_k2, C_k2

def gen_k1():
    k1 = np.arange(0, 100000000)
    B_k1_hit = np.random.choice(k1, int(B_size*B_C_sel), replace=False)
    B_k1_non_hit = np.random.choice(k1, B_size-int(B_size*B_C_sel), replace=True)
    hit_key = np.random.choice(B_k1_hit, int(A_size*A_B_sel), replace=True)
    non_hit_key = np.setdiff1d(k1, B_k1_hit)
    probe_non_hit = np.random.choice(non_hit_key, int(A_size - int(A_size*A_B_sel)), replace=True)
    B_k1 = np.concatenate((B_k1_hit, B_k1_non_hit))
    A_k1 = np.concatenate((hit_key, probe_non_hit))
    return A_k1, B_k1

def gen_random_val(payload_size,size):
    if payload_size == 4:
        v = [random.randint(0, 2**31-1) for i in range(size)]
    elif payload_size == 8:
        v = [random.randint(0, 2**63-1) for i in range(size)]
    else:
        v = [''.join(random.choices(string.ascii_uppercase + string.digits, k=payload_size)) for i in range(size)]
    return v
        

def gen_data():
    B_k2, C_k2 = gen_k2()
    A_k1, B_k1 = gen_k1()
    C_v1 = gen_random_val(payload_size, C_size)
    
    A = pd.DataFrame({'k1': A_k1, 'rowid': np.arange(0, A_size)})
    B = pd.DataFrame({'k1': B_k1, 'k2': B_k2, 'rowid': np.arange(0, B_size)})
    C = pd.DataFrame({'k2': C_k2, 'v1': C_v1, 'rowid': np.arange(0, C_size)})
    
    A.to_parquet('A.parquet',row_group_size=122880)
    B.to_parquet('B.parquet',row_group_size=122880)
    C.to_parquet('C.parquet',row_group_size=122880)

gen_data()

con = duckdb.connect(database = "/home/yihao/duckdb/origin/duckdb/examples/embedded-c++/release/micro_test.db")

for file_name in ['A', 'B', 'C']:
    file_path = '/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/microbench/data_generate_scripts/'
    build_file_name = file_name 
    data_build_path = file_path + build_file_name +'.parquet'
    con.execute("PRAGMA force_compression='auto';")
    con.execute(f"Drop TABLE IF EXISTS {build_file_name}")
    con.execute(f"CREATE TABLE {build_file_name} AS SELECT * FROM '{data_build_path}'")

con = duckdb.connect(database = "/home/yihao/duckdb/origin/duckdb/examples/embedded-c++/release/micro_uncom_test.db")

for file_name in ['A', 'B', 'C']:
    file_path = '/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/microbench/data_generate_scripts/'
    build_file_name = file_name 
    data_build_path = file_path + build_file_name +'.parquet'
    con.execute("PRAGMA force_compression='uncompressed';")
    con.execute(f"Drop TABLE IF EXISTS {build_file_name}")
    con.execute(f"CREATE TABLE {build_file_name} AS SELECT * FROM '{data_build_path}'")