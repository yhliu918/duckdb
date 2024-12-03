import matplotlib.pyplot as plt
import sys
import os

result_path = sys.argv[1]

thread = [1, 2, 4, 8, 16, 24, 32, 48, 64, 96, 128, 192]

with open(result_path + '/result.txt', 'r') as f:
    results = list(map(lambda s: list(map(float, s.split(' '))), f.read().splitlines()))
print(results)

build_throughput = [1000 / results[i][0] for i in range(len(results))]
probe_throughput = [1000 / results[i][1] for i in range(len(results))]
total_throughput = [1000 / results[i][2] for i in range(len(results))]
memset_throughput = [1000 / (results[i][2] - results[i][1] - results[i][0]) for i in range(len(results))]

plt.plot(thread, build_throughput, label = 'build')
plt.plot(thread, probe_throughput, label = 'probe')
plt.plot(thread, total_throughput, label = 'total')
plt.plot(thread, memset_throughput, label = 'memset')

plt.xlabel('thread')
plt.ylabel('op/s')

plt.legend()
plt.savefig(result_path + '/result.pdf')


plt.cla()

from math import *
build_speedup_log = [log(build_throughput[i] / build_throughput[0]) / log(2) for i in range(len(results))]
probe_speedup_log = [log(probe_throughput[i] / probe_throughput[0]) / log(2) for i in range(len(results))]
total_speedup_log = [log(total_throughput[i] / total_throughput[0]) / log(2) for i in range(len(results))]
thread_log = [log(thread[i]) / log(2) for i in range(len(results))]

plt.plot(thread_log, build_speedup_log, label = 'build')
plt.plot(thread_log, probe_speedup_log, label = 'probe')
plt.plot(thread_log, total_speedup_log, label = 'total')

plt.xlabel('log2 thread')
plt.ylabel('log2 speedup')

plt.legend()
plt.savefig(result_path + '/speedup.pdf')