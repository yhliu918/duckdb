import numpy as np
import pandas as pd

file_sizes = [100_000_000, 50_000_000]

output_files = [
    "32B.csv",
    "32B_non_hit.csv"
]


total_keys = sum(file_sizes)
all_keys = np.random.choice(np.arange(0, np.iinfo(np.int32).max, dtype=np.int32), 
                            size=total_keys, replace=False)

start = 0
for size, output_file in zip(file_sizes, output_files):
    end = start + size
    keys = all_keys[start:end]
    
    df = pd.DataFrame(keys, columns=["key"])
    df.to_csv(output_file, index=False, header=False)
    print(f"Generated {output_file} with {size} keys.")
    
    start = end