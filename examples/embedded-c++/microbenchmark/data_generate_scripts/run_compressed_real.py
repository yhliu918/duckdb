probe_size = 20000000
build_size = [10000000] #100000,1800000,5000000
selectivity = [0.0001,0.001,0.01,0.02,0.04,0.1,0.2,0.4,0.8]
payload_size = [4,8,0]
probe_distribution = ['uniform']
key_pattern_pair = [('random','random'),('sorted','sorted')]
payload_file = {
    4:['movieid','house_price','hu_freq'],
    8:['books_200M_uint64','genome_200M_uint64'],
    0:['japanese', 'ps_comment','email','uuid','yago','urls' ]
}
for build in build_size:
    for sel in selectivity:
        for probe_dist in probe_distribution:
            for key_pattern in key_pattern_pair:
                build_pat = key_pattern[0]
                probe_pat = key_pattern[1]
                for pay in payload_size:
                    for file in payload_file[pay]:
                        for mat_strat in [0,1,2]:
                            if mat_strat == 1:
                                for queue_thr in [100,1000,2000,20000,50000]:
                                    print('sync; echo 3 > /proc/sys/vm/drop_caches')
                                    print(f"./microbench_build_real 1 2 {probe_size} {build} {sel} {pay} {probe_dist} 1 {build_pat} {probe_pat} {mat_strat} {queue_thr} {file} > log")
                                    print(f"echo 1 {queue_thr} {build} {sel} {pay} {file} >>  breakdown_compress_real_0204.txt")
                                    print("python3 parse_time.py >> breakdown_compress_real_0204.txt")
                            else:
                                print('sync; echo 3 > /proc/sys/vm/drop_caches')
                                print(f"./microbench_build_real 1 2 {probe_size} {build} {sel} {pay} {probe_dist} 1 {build_pat} {probe_pat} {mat_strat} 0 {file}")
                                print(f"echo 1 0 {build} {sel} {pay} {file} >>  breakdown_compress_real_0204.txt")
                                print("python3 parse_time.py >> breakdown_compress_real_0204.txt")
                            
                                    
                        
                        
                    
                