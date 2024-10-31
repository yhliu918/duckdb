
import duckdb
import sys

db = int(sys.argv[1])
probe_size = int(sys.argv[2])
build_size = int(sys.argv[3])
selectivity = float(sys.argv[4]) 
unique_key_portion = float(sys.argv[5]) 
heavy_hitter_portion = float(sys.argv[6])
payload_column_num = int(sys.argv[7])
payload_tuple_size = int(sys.argv[8])

heavy_hitter_portion = int(heavy_hitter_portion*100)
unique_key_portion = int(unique_key_portion*100)
selectivity = int(selectivity*100)

data_path = '/home/yihao/data_gen/probe{}_build{}_sel{}_skew{}_{}_payload{}_{}'.format(probe_size, build_size, selectivity, heavy_hitter_portion, unique_key_portion, payload_column_num, payload_tuple_size)


if db == 0: #compressed
    con = duckdb.connect(database = "/home/yihao/duckdb/origin/duckdb/examples/embedded-c++/release/tpch.db")
    
    build_file_path = data_path+'/build.parquet'
    con.execute("PRAGMA force_compression='auto';")
    con.execute(f"Drop TABLE IF EXISTS build_{build_size}_{selectivity}_{heavy_hitter_portion}_{payload_column_num}_{payload_tuple_size}")
    con.execute(f"CREATE TABLE build_{build_size}_{selectivity}_{heavy_hitter_portion}_{payload_column_num}_{payload_tuple_size} AS SELECT * FROM '{build_file_path}'")
    
    
    probe_file_path = data_path+'/probe.parquet'
    con.execute("PRAGMA force_compression='auto';")
    con.execute(f"Drop TABLE IF EXISTS probe_{probe_size}_{selectivity}_{unique_key_portion}_{heavy_hitter_portion}_{payload_column_num}_{payload_tuple_size}")
    con.execute(f"CREATE TABLE probe_{probe_size}_{selectivity}_{unique_key_portion}_{heavy_hitter_portion}_{payload_column_num}_{payload_tuple_size} AS SELECT * FROM '{probe_file_path}'")
elif db == 1:
    con = duckdb.connect(database = "/home/yihao/duckdb/origin/duckdb/examples/embedded-c++/release/tpch_uncom.db")
    
    build_file_path = data_path+'/build.parquet'
    con.execute("PRAGMA force_compression='uncompressed';")
    con.execute(f"Drop TABLE IF EXISTS build_{build_size}_{selectivity}_{heavy_hitter_portion}_{payload_column_num}_{payload_tuple_size}")
    con.execute(f"CREATE TABLE build_{build_size}_{selectivity}_{heavy_hitter_portion}_{payload_column_num}_{payload_tuple_size} AS SELECT * FROM '{build_file_path}'")
    
    probe_file_path = data_path+'/probe.parquet'
    con.execute("PRAGMA force_compression='uncompressed';")
    con.execute(f"Drop TABLE IF EXISTS probe_{probe_size}_{selectivity}_{unique_key_portion}_{heavy_hitter_portion}_{payload_column_num}_{payload_tuple_size}")
    con.execute(f"CREATE TABLE probe_{probe_size}_{selectivity}_{unique_key_portion}_{heavy_hitter_portion}_{payload_column_num}_{payload_tuple_size} AS SELECT * FROM '{probe_file_path}'")
    
    

