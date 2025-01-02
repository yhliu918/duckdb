import sys
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import random
import string
import math
import os

# probe_size = 20000000
#parse the input arguments
build_size = int(sys.argv[1])
probe_size = int(sys.argv[2])
sel = float(sys.argv[3])
payload_size = int(sys.argv[4])
probe_distribution = sys.argv[5] # uniform, zipfian, (single hot key)
build_side_hit_ratio = float(sys.argv[6]) # [122880/build_size,1]
build_key_pattern = sys.argv[7] # random, sorted, clustered (only when build_side is not all hit)
probe_key_pattern = sys.argv[8] # random, sorted
key_set_file = ''
if len(sys.argv)>9:
    key_set_file = sys.argv[9] # real-world data file name, if not None, read the key set from the file
payload_file = ''
if len(sys.argv)>10:
    payload_file = sys.argv[10] # real-world data file name, if not None, read the payload set from the file
    
#! build_{size}_{hit_ratio}_{key_pattern}_{payload_size}.csv
#! probe_{size}_{selectivity}_{key_pattern}_{probe_distribution}.csv

#! unique_key_set & non_hit_key_set: {payload_size}, two sets both have more than max {build_size} keys
file_path = '/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/microbench/'
build_file_name = f'build_{build_size}_{int(build_side_hit_ratio*100)}_{build_key_pattern}_{payload_size}'
probe_file_name = f'probe_{build_size}_{probe_size}_{int(sel*10000)}_{probe_key_pattern}_{probe_distribution}_{int(build_side_hit_ratio*100)}'
if key_set_file != '':
    build_file_name += f'_{key_set_file}'
    probe_file_name += f'_{key_set_file}'
if payload_file != '':
    build_file_name += f'_{payload_file}'

build_file_name += '.parquet'
probe_file_name += '.parquet'

generate_build_side = True
generate_probe_side = True
if os.path.exists(file_path+build_file_name):
    generate_build_side = False
if os.path.exists(file_path+probe_file_name):
    generate_probe_side = False
    
#generate a unique key set, from file or random generation
def generate_unique_hit_key(size, non_hit_size, key_set_file=''):
    if key_set_file=='':
        key_set_file = '32B'
    non_hit_key_file = key_set_file+'_non_hit'
    key_set = []
    non_hit_key_set = []
    with open('/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/microbench/unique_hit_key_set/'+key_set_file+".txt", 'r') as f:
        key_set = [int(line[:-1]) for line in f.readlines()[:size]]

    with open('/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/microbench/unique_hit_key_set/'+non_hit_key_file+".txt", 'r') as f:
        non_hit_key_set = [int(line[:-1]) for line in f.readlines()[:non_hit_size]]

    return key_set, non_hit_key_set
         
        
def generate_build_data(key_set, non_hit_key_set, build_size, build_side_hit_ratio, key_pattern):
    build_data = [0]*build_size
    if key_pattern == 'random':
        build_data =np.concatenate((key_set, non_hit_key_set))
        np.random.shuffle(build_data)
    elif key_pattern == 'sorted':
        build_data = np.concatenate((key_set, non_hit_key_set))
        build_data.sort()
    elif key_pattern == 'clustered' and build_side_hit_ratio < 1:
        rowgroup_ids = np.arange(0, build_size//122880)
        pick_row_group_num = math.ceil(len(key_set)/122880)
        pick_row_group = np.random.choice(rowgroup_ids, pick_row_group_num, replace=False)
        for i in range(pick_row_group_num-1):
            build_data[int(pick_row_group[i]*122880):int((pick_row_group[i]+1)*122880)] = key_set[i*122880:(i+1)*122880]
        left_size = len(key_set) - (pick_row_group_num-1)*122880
        build_data[int(pick_row_group[pick_row_group_num-1]*122880): int((pick_row_group[pick_row_group_num-1])*122880)+left_size] = key_set[(pick_row_group_num-1)*122880:]

        idx=0
        for i in range(build_size):
            if build_data[i]==0:
                build_data[i] = non_hit_key_set[idx%len(non_hit_key_set)]
                idx+=1
    else:
        print(key_pattern, build_side_hit_ratio)
        print('Invalid key pattern')
        
    return build_data

def read_probe_non_hit_key_set(size, key_set_file=''):
    if key_set_file=='':
        key_set_file = '32B'
    key_set_file = 'probe_'+key_set_file+'_non_hit'
    non_hit_key_set = []
    with open('/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/microbench/unique_hit_key_set/'+key_set_file+'.txt', 'r') as f:
        non_hit_key_set = [int(line[:-1]) for line in f.readlines()[:size]]
    return non_hit_key_set

def generate_probe_hit_data(key_set, probe_size, probe_distribution, zipfian_param=2):
    probe_data = []
    if probe_distribution == 'uniform':
        probe_data = np.random.choice(key_set, probe_size, replace=True)
    elif probe_distribution == 'zipfian':
        pick = np.random.zipf(zipfian_param, probe_size)
        np.random.shuffle(key_set)
        probe_data = [key_set[i%len(key_set)] for i in pick]
    else:
        print('Invalid probe distribution')
    return probe_data

def generate_probe_non_hit_data(non_hit_key_set, probe_size):
    return np.random.choice(non_hit_key_set, probe_size, replace=True)

def generate_probe_pattern(probe_hit_data, probe_non_hit_data, key_pattern):
    probe_data = np.concatenate((probe_hit_data, probe_non_hit_data))
    if key_pattern == 'random':
        np.random.shuffle(probe_data)
        return probe_data
    elif key_pattern == 'sorted':
        probe_data.sort()
        return probe_data
    else:
        print('Invalid probe key pattern')
     
def gen_random_string(size, length, fixed_length=True):
    if fixed_length:
        return [''.join(random.choices(string.ascii_uppercase + string.digits, k=length)) for i in range(size)]
    else:
        return [''.join(random.choices(string.ascii_uppercase + string.digits, k=random.randint(length//2, length))) for i in range(size)]   
def generate_payload_data(build_size, payload_size, payload_file, fixed_length):
    payload_data = []
    if payload_file == '':
        match payload_size:
            case 4:
                payload_data = np.random.randint(0, 2**31, build_size)
            case 8:
                payload_data = np.random.randint(0, 2**63, build_size)
            case _:
                payload_data = gen_random_string(build_size, payload_size,fixed_length)
                
    else:
        with open(payload_file, 'r') as f:
            match payload_size:
                case 4:
                    payload_data = [int(line[:-1]) for line in f.readlines()[:build_size]]
                case 8:
                    payload_data = [int64_t(line[:-1]) for line in f.readlines()[:build_size]]
                case _:
                    payload_data = [line[:-1] for line in f.readlines()[:build_size]]
    return payload_data
        
        
#!
#! start generating the data
#!

unique_hit_key_set, non_hit_key_set = generate_unique_hit_key( int(build_size*build_side_hit_ratio), int(build_size- (build_size*build_side_hit_ratio)), key_set_file)

#generate the payload data
#write the build side data to the file
if generate_build_side:
    print(f'Generating {build_file_name} data')
    build_side_key = generate_build_data(unique_hit_key_set, non_hit_key_set, build_size, build_side_hit_ratio, build_key_pattern)

    fixed_length = True
    if payload_size % 10!=0:
        fixed_length = False
    payload_data = generate_payload_data(build_size, payload_size, payload_file,fixed_length)

    build_rowid = np.array(range(build_size))
    build_table = [build_side_key, build_rowid, payload_data]
    
    build_column_types = [pa.int32(), pa.int32()]
    match payload_size:
        case 4:
            build_column_types.append(pa.int32())
        case 8:
            build_column_types.append(pa.int64())
        case _:
            build_column_types.append(pa.string())
    build_arrays = [pa.array(column, type=col_type) for column, col_type in zip(build_table, build_column_types)]
    build_column_names = ['build_key', 'build_side_rowid','payload_0']
    build_pa_table = pa.Table.from_arrays(build_arrays, names=build_column_names)
    pq.write_table(build_pa_table, file_path+build_file_name,row_group_size=122880)
    
#generate the probe data
#write the probe side data to the file
if generate_probe_side:
    print(f'Generating {probe_file_name} data')
    probe_non_hit_key_set = read_probe_non_hit_key_set(probe_size-int(probe_size*sel))
    probe_side_hit_data = generate_probe_hit_data(unique_hit_key_set, int(probe_size*sel), probe_distribution)
    probe_side_non_hit_data = generate_probe_non_hit_data(probe_non_hit_key_set, probe_size-int(probe_size*sel))
    probe_side_key = generate_probe_pattern(probe_side_hit_data, probe_side_non_hit_data, probe_key_pattern)
    
    probe_table = [probe_side_key]
    probe_column_types = [pa.int32()]
    probe_arrays = [pa.array(column, type=col_type) for column, col_type in zip(probe_table, probe_column_types)]
    probe_column_names = ['probe_key']
    probe_pa_table = pa.Table.from_arrays(probe_arrays, names=probe_column_names)
    pq.write_table(probe_pa_table, file_path+probe_file_name,row_group_size=122880)

