probe_size = 20000000
build_size = [2000,20000,200000,1000000,2000000,4000000,10000000,15000000,18000000]

selectivity = [0.0001,0.001,0.01,0.02,0.04,0.1,0.2,0.4,0.8]
payload_size = [4,8,10,11,20,21,40,41,60,61,80,81,100,101]
probe_distribution = ['uniform','zipfian']
probe_distribution = ['uniform']
build_side_hit_ratio = [0.1,0.2,0.4,0.8]
build_key_pattern = ['random','sorted','clustered']
probe_key_pattern = ['random','sorted']
key_pattern_pair = [('random','random'),('clustered','random'),('sorted','sorted'),('clustered','sorted')]
key_pattern_pair = [('random','random')]

for build in build_size:
    for sel in selectivity:
        for pay in payload_size:
            for probe_dist in probe_distribution:
                for key_pattern in key_pattern_pair:
                    build_pat = key_pattern[0]
                    probe_pat = key_pattern[1]
                    if build_pat == 'clustered':
                        for hit_ratio in build_side_hit_ratio:
                            if hit_ratio > 122880/build:
                                for mode in [1]:
                                    for mat_strat in [0,1]:
                                        if mat_strat == 1:
                                            for queue_thr in [10,100, 1000, 2000]:
                                                print('sync; echo 3 > /proc/sys/vm/drop_caches')
                                                print(f'echo {mode} {probe_size} {build} {sel} {pay} {probe_dist} {hit_ratio} {build_pat} {probe_pat} {mat_strat} {queue_thr} >> /home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/payload_hash_table_build_time_0205.txt')
                                                
                                                print(f'echo {mode} {probe_size} {build} {sel} {pay} {probe_dist} {hit_ratio} {build_pat} {probe_pat} {mat_strat} {queue_thr} >> /home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/io_time_0205.txt')
                                    
                                                print(f"./microbench_build_new 1 {mode} {probe_size} {build} {sel} {pay} {probe_dist} {hit_ratio} {build_pat} {probe_pat} {mat_strat} {queue_thr}")
                                                
                                        else:
                                            print('sync; echo 3 > /proc/sys/vm/drop_caches')
                                            print(f'echo {mode} {probe_size} {build} {sel} {pay} {probe_dist} {hit_ratio} {build_pat} {probe_pat} {mat_strat} 0 >> /home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/payload_hash_table_build_time_0205.txt')
                                                
                                            print(f'echo {mode} {probe_size} {build} {sel} {pay} {probe_dist} {hit_ratio} {build_pat} {probe_pat} {mat_strat} 0 >> /home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/io_time_0205.txt')
                                            print(f"./microbench_build_new 1 {mode} {probe_size} {build} {sel} {pay} {probe_dist} {hit_ratio} {build_pat} {probe_pat} {mat_strat} 0")
                                            
             
                    else:
                        for mode in [1]:
                                for mat_strat in [0,1]:
                                    if mat_strat == 1:
                                        for queue_thr in [100, 1000, 2000]:
                                            print('sync; echo 3 > /proc/sys/vm/drop_caches')
                                            print(f'echo {mode} {probe_size} {build} {sel} {pay} {probe_dist} 1 {build_pat} {probe_pat} {mat_strat} {queue_thr} >> /home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/payload_hash_table_build_time_0205.txt')
                                            
                                            print(f'echo {mode} {probe_size} {build} {sel} {pay} {probe_dist} 1 {build_pat} {probe_pat} {mat_strat} {queue_thr} >> /home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/payload_build_time_0205.txt')
                                                
                                            print(f'echo {mode} {probe_size} {build} {sel} {pay} {probe_dist} 1 {build_pat} {probe_pat} {mat_strat} {queue_thr} >> /home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/io_time_0205.txt')
                                            print(f"./microbench_build_new 1 {mode} {probe_size} {build} {sel} {pay} {probe_dist} 1 {build_pat} {probe_pat} {mat_strat} {queue_thr}")
                                            
                                    else:
                                        print('sync; echo 3 > /proc/sys/vm/drop_caches')
                                        print(f'echo {mode} {probe_size} {build} {sel} {pay} {probe_dist} 1 {build_pat} {probe_pat} {mat_strat} 0 >> /home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/payload_hash_table_build_time_0205.txt')
                                                
                                        print(f'echo {mode} {probe_size} {build} {sel} {pay} {probe_dist} 1 {build_pat} {probe_pat} {mat_strat} 0 >> /home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/io_time_0205.txt')
                                        print(f"./microbench_build_new 1 {mode} {probe_size} {build} {sel} {pay} {probe_dist} 1 {build_pat} {probe_pat} {mat_strat} 0")
                                        
                    
                
                