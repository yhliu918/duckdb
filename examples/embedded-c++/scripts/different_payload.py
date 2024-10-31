import sys
import os

probe_size = 20000000
build = 10000000
selectivity = [0.02, 0.04, 0.1, 0.2, 0.4] #0.02, 0.04, 0.1, 0.2, 0.4, 0.8

unique_key_portion = [0, 0.1, 0.2, 0.4, 0.8]
heavy_hitter_portion = [0, 0.1, 0.2, 0.4, 0.8]

payload_column_num = [1, 2, 3, 4]
payload_type = [0,1,2,3]
payload_size = [4,8]
fixed_size_string = [10, 20, 100, 200]
payload_choice = [(0,4),(1,8),(2,10),(2,20),(2,100),(3,11),(3,21),(3,101)] 


for sel in selectivity:
    for [type_, size] in payload_choice:
        print(f"build: {build}, sel: {sel}, type: {type_}, size: {size}")
        payload_tuple_size = 0
        with open('payload.config', 'w') as f:
            match type_:
                case 0:
                    f.write(f"{type_} 0")
                    payload_tuple_size+=4
                case 1:
                    f.write(f"{type_} 0")
                    payload_tuple_size+=8
                case 2:
                    f.write(f"{type_} {size}")
                    payload_tuple_size = size
                case 3:
                    f.write(f"{type_} {size}")
                    payload_tuple_size = size
        os.system(f"python3 join_data_gen.py {probe_size} {build} {sel} 0 0")
        for db_mode in [0,1]:
            os.system(f"python3 load_db.py {db_mode} {probe_size} {build} {sel} 0 0 1 {payload_tuple_size}")
        data_path = '/home/yihao/data_gen/probe{}_build{}_sel{}_skew{}_{}_payload{}_{}'.format(probe_size, build, int(sel*100), 0, 0, 1, payload_tuple_size)
        with open(data_path+'/task.sh', 'w') as f:
            for mode in [0,1,2]:
                for thread_num in [1,2,4,8,16]:
                    f.write(f"./micro {thread_num} {mode} {probe_size} {build} {int(sel*100)} 0 0 1 {payload_tuple_size} 0 \n")
                    f.write("sleep 1 \n")
                
                