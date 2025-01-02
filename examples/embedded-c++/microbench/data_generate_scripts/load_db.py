
import duckdb
import sys

db = int(sys.argv[1])
build_size = int(sys.argv[2])
probe_size = int(sys.argv[3])
sel = float(sys.argv[4])
payload_size = int(sys.argv[5])
probe_distribution = sys.argv[6] # uniform, zipfian, (single hot key)
build_side_hit_ratio = float(sys.argv[7]) # [122880/build_size,1]
build_key_pattern = sys.argv[8] # random, sorted, clustered (only when build_side is not all hit)
probe_key_pattern = sys.argv[9] # random, sorted
key_set_file = ''
if len(sys.argv)>10:
    key_set_file = sys.argv[10] # real-world data file name, if not None, read the key set from the file
payload_file = ''
if len(sys.argv)>11:
    payload_file = sys.argv[11] # real-world data file name, if not None, read the payload set from the file

file_path = '/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/microbench/'
build_file_name = f'build_{build_size}_{int(build_side_hit_ratio*100)}_{build_key_pattern}_{payload_size}'
probe_file_name = f'probe_{build_size}_{probe_size}_{int(sel*10000)}_{probe_key_pattern}_{probe_distribution}_{int(build_side_hit_ratio*100)}'
if key_set_file != '':
    build_file_name += f'_{key_set_file}'
    probe_file_name += f'_{key_set_file}'
if payload_file != '':
    build_file_name += f'_{payload_file}'



data_build_path = file_path + build_file_name + '.parquet'
data_probe_path = file_path + probe_file_name + '.parquet'


if db == 0: #compressed
    con = duckdb.connect(database = "/home/yihao/duckdb/origin/duckdb/examples/embedded-c++/release/micro.db")
    
    con.execute("PRAGMA force_compression='auto';")
    con.execute(f"Drop TABLE IF EXISTS {build_file_name}")
    con.execute(f"CREATE TABLE {build_file_name} AS SELECT * FROM '{data_build_path}'")
    
    
    con.execute("PRAGMA force_compression='auto';")
    con.execute(f"Drop TABLE IF EXISTS {probe_file_name}")
    con.execute(f"CREATE TABLE {probe_file_name} AS SELECT * FROM '{data_probe_path}'")
elif db == 1:
    con = duckdb.connect(database = "/home/yihao/duckdb/origin/duckdb/examples/embedded-c++/release/micro_uncom.db")
    
    con.execute("PRAGMA force_compression='uncompressed';")
    con.execute(f"Drop TABLE IF EXISTS {build_file_name}")
    con.execute(f"CREATE TABLE {build_file_name} AS SELECT * FROM '{data_build_path}'")
    
    con.execute("PRAGMA force_compression='uncompressed';")
    con.execute(f"Drop TABLE IF EXISTS {probe_file_name}")
    con.execute(f"CREATE TABLE {probe_file_name} AS SELECT * FROM '{data_probe_path}'")
    

