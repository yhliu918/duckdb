import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import os

# currently all build side key is unique
# thus, the result size <= probe_size. only if build side has duplicate key, the result size can be larger than probe_size
probe_size = int(sys.argv[1])
build_size = int(sys.argv[2])
selectivity = float(sys.argv[3]) # selectivity is size of result / probe side size. 
unique_key_portion = float(sys.argv[4]) # x% of probe side hit access the heavy hitter key set
heavy_hitter_portion = float(sys.argv[5]) # y% of the unique build key is heavy hitter
#later consider different data distribution (this would influence the compression ratio -> bsearch efficiency)


#int32: 0, int64: 1, fixed size string (additional config: string length): 2, varchar:3
payload_config = {}
with open('payload.config', 'r') as f:
    config = f.read().splitlines()
    for i in range(len(config)):
        tmp_config = config[i].split(' ')
        payload_config[int(tmp_config[0])] = int(tmp_config[1])

    
total_unique_value = (2<<27) # probe key data range
unique_build_key = []
non_hit_unique_key = []
heavy_hitter = []
non_heavy_hitter = []

def gen_build_key(size):
    # join_key = np.random.choice(total_unique_value, size, replace=False)
    join_key= np.arange(0, size)
    global unique_build_key 
    unique_build_key = join_key
    # unique_build_key = np.unique(join_key)
    global non_hit_unique_key 
    non_hit_unique_key = np.arange(size+1, total_unique_value)
    # non_hit_unique_key = np.setdiff1d(np.arange(0, total_unique_value), unique_build_key)
    global heavy_hitter
    global non_heavy_hitter
    heavy_hitter = np.random.choice(unique_build_key, int(len(unique_build_key)*heavy_hitter_portion), replace=False)
    non_heavy_hitter = np.setdiff1d(unique_build_key, heavy_hitter)
    np.random.shuffle(join_key)
    print(f"unique_build_key: {len(unique_build_key)}, non_hit_unique_key: {len(non_hit_unique_key)}")
    return join_key

def gen_probe_key(size):
    hit_key_size = int(size * selectivity)
    non_hit_key_size = size - hit_key_size
    print(f"hit_key_size: {hit_key_size}, non_hit_key_size: {non_hit_key_size}")
    # hit_keys_idx = np.random.zipf(unique_key_portion, hit_key_size)
    #create hit keys
    heavy_hitter_size = int(hit_key_size*unique_key_portion)
    non_heavy_hitter_size = hit_key_size - heavy_hitter_size
    heavy_hitter_idx = np.random.choice(len(heavy_hitter), heavy_hitter_size, replace=True)
    heavy_hitters = [heavy_hitter[i] for i in heavy_hitter_idx]
    non_heavy_hitter_idx = np.random.choice(len(non_heavy_hitter), non_heavy_hitter_size, replace=False)
    non_heavy_hitters = [non_heavy_hitter[i] for i in non_heavy_hitter_idx]
    hit_keys = np.concatenate((heavy_hitters, non_heavy_hitters))
    print(hit_keys)
    # hit_keys_idx = np.random.choice(len(unique_build_key), hit_key_size, replace=True)
    # print(hit_keys_idx)
    # hit_keys = [unique_build_key[i] for i in hit_keys_idx]
    non_hit_keys = np.random.choice(non_hit_unique_key, non_hit_key_size, replace=False)
    total_probe_key = np.concatenate((hit_keys, non_hit_keys))
    np.random.shuffle(total_probe_key)
    print(f"hit_key_size: {len(hit_keys)}, non_hit_key_size: {len(non_hit_keys)}")
    return total_probe_key

def generate_values(type_descriptor, size):
    match type_descriptor[0]:
        case 0:
            print(f"generate int32 with size {size}")
            return np.random.randint(0, 2<<30, size)
        case 1:
            print(f"generate int64 with size {size}")
            return np.random.randint(0, 2<<60, size)
        case 2:
            print(f"generate fixed size string with size {size}")
            return generate_fixed_size_string(type_descriptor[1], size)
        case 3:
            print(f"generate varchar with size {size}")
            return generate_varchar(type_descriptor[1], size)

def generate_fixed_size_string(length, size):
    return np.array([''.join(np.random.choice(list('abcdefghijklmnopqrstuvwxyz'), length)) for _ in range(size)])

def generate_varchar(max_length,size):
    return np.array([''.join(np.random.choice(list('abcdefghijklmnopqrstuvwxyz'), np.random.randint(10, max_length))) for _ in range(size)])

def gen_rowid(size):
    return np.array(range(size))

build_key = gen_build_key(build_size)
build_side_rowid = gen_rowid(build_size)

probe_key = gen_probe_key(probe_size)

build_table = [build_key, build_side_rowid]
for [type_, size] in payload_config.items():
    values = generate_values([type_, size], build_size)
    build_table.append(values)

probe_table = [probe_key]

build_column_types = [pa.int32(), pa.int64()]
for [type_, size] in payload_config.items():
    if type_ == 0:
        build_column_types.append(pa.int32())
    elif type_ == 1:
        build_column_types.append(pa.int64())
    else:
        build_column_types.append(pa.string())

probe_column_types = [pa.int32()]

build_arrays = [pa.array(column, type=col_type) for column, col_type in zip(build_table, build_column_types)]
probe_arrays = [pa.array(column, type=col_type) for column, col_type in zip(probe_table, probe_column_types)]

build_column_names = ['build_key', 'build_side_rowid'] + [f'payload_{idx}' for idx in range(len(payload_config))]
probe_column_names = ['probe_key']

build_pa_table = pa.Table.from_arrays(build_arrays, names=build_column_names)
probe_pa_table = pa.Table.from_arrays(probe_arrays, names=probe_column_names)

payload_col_num = len(payload_config)
payload_tuple_size = 0
for [type_, size] in payload_config.items():
    match type_:
        case 0:
            payload_tuple_size += 4
        case 1:
            payload_tuple_size += 8
        case 2:
            payload_tuple_size += size
        case 3:
            payload_tuple_size += size
data_path = '/home/yihao/data_gen/probe{}_build{}_sel{}_skew{}_{}_payload{}_{}'.format(probe_size, build_size, int(selectivity*100), int(heavy_hitter_portion*100), int(unique_key_portion*100), payload_col_num, payload_tuple_size)
if not os.path.exists(data_path):
    os.makedirs(data_path)
else:
    os.system(f"rm -r {data_path}")
    os.makedirs(data_path)
row_group_size = 1228800
pq.write_table(build_pa_table, data_path+"/build.parquet",row_group_size=row_group_size)
pq.write_table(probe_pa_table, data_path+"/probe.parquet",row_group_size=row_group_size)

idx = 2
result_idx = 1
with open(data_path+"/config", "w") as f:
    for [type_, size] in payload_config.items():
        match type_:
            case 0:
                f.write(f"{idx} {result_idx} 13\n")
            case 1:
                f.write(f"{idx} {result_idx} 14\n")
            case 2:
                f.write(f"{idx} {result_idx} 25 {size}\n")
            case 3:
                f.write(f"{idx} {result_idx} 25\n")
        idx+=1
        result_idx+=1
    

# df.to_parquet('output.parquet', engine='pyarrow')
