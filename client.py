import glob
import json
from time import sleep

import requests

ips = [
    'http://localhost:5000',

]

files = [file_location.split('/')[1] for file_location in glob.glob('read_files/*')]
files_per_ip = {}

# Split the files equally among the VMs
for i in range(len(ips)):
    files_per_ip.update(
        {
            ips[i]: {
                'file_names': files[i * (len(files) // len(ips)): (i+1) * (len(files) // len(ips))],
                'read_dir': 'read_files',
                'write_dir': 'write_files'
            }
        }
    )
    if i == len(ips) - 1 and len(files) % len(ips) != 0:
        files_per_ip[ips[i]] += files[(i+1) * (len(files) // len(ips)):]

# Call APIs of all VMs with the data each one needs to handle
for ip, data in files_per_ip.items():
    requests.post(f'{ip}/sorting/stage1', data=data)

# Check whether the first stage has finished by pulling the files
found_all_files = False
results_stage1_files = []
while not found_all_files:
    results_stage1_files = glob.glob('results/*')
    if len(results_stage1_files) == len(ips):
        found_all_files = True
    else:
        print("Sleeping")
        sleep(1)

data_from_stage_1 = {}
data_for_stage_2 = {}
for file in results_stage1_files:
    with open(f'write_files/{file}', 'r') as results_file:
        results = json.load(results_file)
        data_from_stage_1.update(results)

for file, file_data in data_from_stage_1.items():
    for file_partition, positions in file_data.items():
        if not data_for_stage_2.get(file_partition):
            data_for_stage_2.update(
                {
                    file_partition: [positions]
                }
            )
        else:
            data_for_stage_2[file_partition].append(positions)

