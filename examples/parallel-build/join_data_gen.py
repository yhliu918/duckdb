import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import os

# currently all build side key is unique
# thus, the result size <= probe_size. only if build side has duplicate key, the result size can be larger than probe_size
probe_size = int(sys.argv[1])
build_size_list = [int(sys.argv[2]), int(sys.argv[3])]
#later consider different data distribution (this would influence the compression ratio -> bsearch efficiency)
    
total_unique_value = (2<<27) # probe key data range

def gen_build_key(size):
    # join_key = np.random.choice(total_unique_value, size, replace=False)
    join_key = np.arange(0, size)
    np.random.shuffle(join_key)
    return join_key

def gen_probe_key(size, build_key):
    hit_keys = np.random.choice(build_key, size, replace=True)
    return hit_keys

def gen_rowid(size):
    return np.array(range(size))

length = len(build_size_list)

build_key_list = [gen_build_key(build_size) for build_size in build_size_list]
build_side_rowid = [gen_rowid(build_size) for build_size in build_size_list]
probe_key_list = [gen_probe_key(probe_size, build_key) for build_key in build_key_list]

build_table_list = [(build_key_list[i], build_side_rowid[i]) for i in range(length)]
probe_table = probe_key_list

build_column_types = [pa.int32(), pa.int64()]

probe_column_types = [pa.int32()] * length

build_arrays_list = [
    [pa.array(column, type=col_type) for column, col_type in zip(build_table, build_column_types)]
    for build_table in build_table_list
]
probe_arrays = [pa.array(column, type=col_type) for column, col_type in zip(probe_table, probe_column_types)]

build_column_names_list = [['build_key_' + str(i), 'build_side_rowid_' + str(i)] for i in range(length)]
probe_column_names = ['probe_key_' + str(i) for i in range(length)]

build_pa_table_list = [pa.Table.from_arrays(build_arrays, names=build_column_names)
    for build_arrays, build_column_names in zip(build_arrays_list, build_column_names_list)]
probe_pa_table = pa.Table.from_arrays(probe_arrays, names=probe_column_names)

data_path = '/home/hangrui/parallel-build/gen_data/probe' + str(probe_size) + '_build' + '_'.join([str(i) for i in build_size_list])
if not os.path.exists(data_path):
    os.makedirs(data_path)
else:
    print("data path already exists")
for i in range(length):
    pq.write_table(build_pa_table_list[i], data_path+"/build{}.parquet".format(i))
pq.write_table(probe_pa_table, data_path+"/probe.parquet")
