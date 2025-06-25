probe_size = 20000000
build_size = [2000,20000,200000,2000000,4000000]
#0.01 ,0.1, 1,10,20
selectivity = [0.0001,0.001,0.01,0.02,0.04,0.1,0.2,0.4,0.8]
payload_size = [4,8,10,11,20,21,40,41,60,61,80,81,100,101]
probe_distribution = ['uniform','zipfian']
build_side_hit_ratio = [0.1,0.2,0.4,0.8]
build_key_pattern = ['random','sorted','clustered']
probe_key_pattern = ['random','sorted']
key_pattern_pair = [('random','random'),('clustered','random'),('sorted','sorted'),('clustered','sorted')]

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
                                print(f"echo build: {build}, sel: {sel}, payload: {pay}, probe_dist: {probe_dist}, key_pattern: {build_pat},{probe_pat} hit_ratio: {hit_ratio}")
                                print(f"python3 generate_data.py {build} {probe_size} {sel} {pay} {probe_dist} {hit_ratio} {build_pat} {probe_pat}")
                                print(f"python3 load_db.py 0 {build} {probe_size} {sel} {pay} {probe_dist} {hit_ratio} {build_pat} {probe_pat}")
                                print(f"python3 load_db.py 1 {build} {probe_size} {sel} {pay} {probe_dist} {hit_ratio} {build_pat} {probe_pat}")
                    else:
                        print(f"echo build: {build}, sel: {sel}, payload: {pay}, probe_dist: {probe_dist}, key_pattern: {build_pat},{probe_pat} ")
                        print(f"python3 generate_data.py {build} {probe_size} {sel} {pay} {probe_dist} 1 {key_pattern[0]} {key_pattern[1]}")
                        print(f"python3 load_db.py 0 {build} {probe_size} {sel} {pay} {probe_dist} 1 {build_pat} {probe_pat}")
                        print(f"python3 load_db.py 1 {build} {probe_size} {sel} {pay} {probe_dist} 1 {build_pat} {probe_pat}")
                        
                    
                