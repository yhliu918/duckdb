import re
import sys

def parse_log(file_path):
    hash_join_time = 0
    sink_hash_join_time = 0
    total_time = 0
    total_latency = 0
    io_time = 0
    map_building_time = 0
    materialize_time = 0
    pipeline_id = 0
    ff = open(file_path, 'r')
    lines = ff.readlines()
    for l in lines:
        try:
            if 'Pipeline#' in l:
                pipeline_id = int(l.split(' ')[1])
            # if pipeline_id == 0 or pipeline_id == 1:
            #     continue
            if 'HASH_JOIN' in l:
                hash_join_time += float(l.split(' ')[-1])
            if 'Sink operator HASH_JOIN' in l:
                sink_hash_join_time += float(l.split(' ')[-1])
            if ('operator' in l or 'Operator' in l )and 'Unknown' not in l:
                total_time += float(l.split(' ')[-1])
            if 'IO' in l:
                io_time += float(l.split(' ')[-1])
            if 'Map building time:' in l:
                map_building_time += float(l.split(' ')[-1])
            if 'Materialize ' in l:
                materialize_time += float(l.split(' ')[-1])
        except Exception as e:
            print(f"Skipping line due to error: {e}")
            continue
    
    total_latency = float(lines[-1].split(' ')[-1])
    method = lines[-1].split(' ')[1]
    result_size = lines[-1].split(' ')[0]
    thread_num = lines[-1].split(' ')[2]
    mat_strat = lines[-1].split(' ')[3]
    queue_thr = lines[-1].split(' ')[4]
    lat = lines[-1].split(' ')[5]
    str_out = (','.join([method,lat,result_size,thread_num,mat_strat,queue_thr]))
    thread_num = int(thread_num)
            

    return str_out,map_building_time/thread_num, materialize_time/thread_num, sink_hash_join_time/thread_num,total_latency, io_time/thread_num

# Example usage
file_path = 'log'  # Replace with your log file path
str_out,map_building_time, materialize_time, sink_hash_join_time,total_latency, io_time = parse_log(file_path)

print(','.join([str_out,str(round(map_building_time, 2)),str(round(materialize_time, 2)),str(round(sink_hash_join_time, 2)), str(round(total_latency-sink_hash_join_time - io_time-materialize_time-map_building_time, 2)), str(round(io_time, 2)), str(round(total_latency, 2))]))
# ff = open(file_path, 'r')
# lines = ff.readlines()
# print(lines[-1])
# print(str(round(map_building_time, 2)),str(round(materialize_time, 2)),str(round(sink_hash_join_time, 2)), str(round(total_latency-sink_hash_join_time - io_time-materialize_time-map_building_time, 2)), str(round(io_time, 2)), str(round(total_latency, 2)))