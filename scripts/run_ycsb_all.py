import subprocess
import os


def run_case(workload, threads):
    label = f'mvcc-{workload}-t{threads}'

    cmd = f'target/release/ycsb --workload workloads/workload_{workload.lower()}_large.json -n {threads}'

    p = subprocess.Popen(cmd.split(' '),
                         stdout=subprocess.PIPE,
                         encoding='utf-8')

    with open(f'results/{label}-stdout.txt', 'w') as f:
        for line in p.stdout.readlines():
            f.write(line)


for workload in ['A', 'B', 'C', 'D']:
    for threads in [4, 8, 10, 20, 30, 40]:
        run_case(workload, threads)
