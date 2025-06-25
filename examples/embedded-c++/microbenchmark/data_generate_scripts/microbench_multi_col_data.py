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
                        print(f"echo build: {build}, sel: {sel}, payload: {pay}, probe_dist: {probe_dist}, key_pattern: {build_pat},{probe_pat} column number: {cols}")
                        print(f"python3 generate_data_multicol.py {build} {probe_size} {sel} {pay} {probe_dist} 1 {key_pattern[0]} {key_pattern[1]} {cols}")
                        # print(f"python3 load_db.py 0 {build} {probe_size} {sel} {pay} {probe_dist} 1 {build_pat} {probe_pat}")
                        # print(f"python3 load_db.py 1 {build} {probe_size} {sel} {pay} {probe_dist} 1 {build_pat} {probe_pat}")
                        
                    
                