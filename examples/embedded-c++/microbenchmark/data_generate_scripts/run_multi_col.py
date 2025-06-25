probe_size = 20000000
build_size = [1800000,5000000,10000000]
selectivity = [0.0001,0.001,0.01,0.02,0.04,0.1,0.2,0.4,0.8]
payload_size = [4,8,10,11,20,21,40,41]
probe_distribution = ['uniform']
key_pattern_pair = [('random','random'),('sorted','sorted')]
column_numbers = [2,4,6,8,10]

for build in build_size:
    for sel in selectivity:
        for pay in payload_size:
            for probe_dist in probe_distribution:
                for key_pattern in key_pattern_pair:
                    build_pat = key_pattern[0]
                    probe_pat = key_pattern[1]
                    for cols in column_numbers:
                        for mode in [1]:
                                for mat_strat in [0,1,2]:
                                    if mat_strat == 1:
                                        for queue_thr in [10,100, 1000, 2000]:
                                            print('sync; echo 3 > /proc/sys/vm/drop_caches')
                                            print(f"./build_multi_col 1 {mode} {probe_size} {build} {sel} {pay} {probe_dist} 1 {build_pat} {probe_pat} {mat_strat} {queue_thr} {cols}")
                                            
                                    else:
                                        print('sync; echo 3 > /proc/sys/vm/drop_caches')
                                        print(f"./build_multi_col 1 {mode} {probe_size} {build} {sel} {pay} {probe_dist} 1 {build_pat} {probe_pat} {mat_strat} 0 {cols}")
                        
                        
                    
                