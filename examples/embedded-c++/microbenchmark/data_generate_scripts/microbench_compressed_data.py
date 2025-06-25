probe_size = 20000000
build_size = [100000,1800000,5000000,10000000]
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
                        print(f"echo build: {build}, sel: {sel}, payload: {pay}, probe_dist: {probe_dist}, key_pattern: {build_pat},{probe_pat} payload file: {file}")
                        print(f"python3 generate_data.py {build} {probe_size} {sel} {pay} {probe_dist} 1 {key_pattern[0]} {key_pattern[1]} {file}")
        