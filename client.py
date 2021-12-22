import json
from time import sleep

import boto3
import requests
import uuid
from smart_open import open
from custom_logger import get_logger
# ips = [
#     'http://localhost:5000',
#
# ]
# CODE FOR SPLITTING PAYLOAD AMONG MULTIPLE INSTANCES
# files = [file_location.split('/')[1] for file_location in glob.glob('read_files/*')]
# files_per_ip = {}
#
# # Split the files equally among the VMs
# for i in range(len(ips)):
#     files_per_ip.update(
#         {
#             ips[i]: {
#                 'file_names': files[i * (len(files) // len(ips)): (i+1) * (len(files) // len(ips))],
#                 'read_dir': 'read_files',
#                 'write_dir': 'write_files'
#             }
#         }
#     )
#     if i == len(ips) - 1 and len(files) % len(ips) != 0:
#         files_per_ip[ips[i]] += files[(i+1) * (len(files) // len(ips)):]
#
# # Call APIs of all VMs with the data each one needs to handle
# for ip, data in files_per_ip.items():
#     requests.post(f'{ip}/sorting/stage1', data=data)
#


def run_sorting_experiment(experiment_number, nr_files, file_size, intervals):
    process_uuid = uuid.uuid4()
    results_bucket = 'output-sorting-experiments'
    prefix_results_stage_1 = f'results_stage1/experiment_{experiment_number}_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}/results_stage1_'
    conn = boto3.client('s3')

    logger = get_logger(
        'main_handler',
        'main_handler',
        nr_files,
        file_size,
        intervals
    )

    logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Start stage 1.')
    requests.post(
        'http://localhost:5000/sorting/stage1',
        json={
            "read_dir": "10mb-10files-input",
            "write_dir": "10mb-10files-intermediate",
            "file_names": ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"],
            "config": {
                "file_size": '10MB',
                'nr_files': '10',
                'intervals': '256'
            },
            "experiment_number": experiment_number
        }
    )

    file_found = False
    all_objects = None
    all_obj = None
    files_keys = []

    while not file_found:
        all_obj = conn.list_objects(Bucket=results_bucket, Prefix=prefix_results_stage_1)
        all_objects = all_obj.get('Contents')

        if all_objects is not None and len(all_objects) == 1:
            file_found = True
        else:
            print("Sleeping")
            sleep(3)

    logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finish stage 1.')

    # return

    data_from_stage_1 = {}
    data_for_stage_2 = {}

    for result_stage1 in all_objects:
        if result_stage1['Key'] == f'{prefix_results_stage_1}':
            continue
        with open(f's3://{results_bucket}/{result_stage1["Key"]}', 'r') as file:
            content = json.loads(file.read())
            data_from_stage_1.update(content)
    # Check whether the first stage has finished by pulling the files
    # found_all_files = False
    # results_stage1_files = []
    # while not found_all_files:
    #     results_stage1_files = glob.glob('results/*')
    #     if len(results_stage1_files) == len(ips):
    #         found_all_files = True
    #     else:
    #         print("Sleeping")
    #         sleep(1)

    # for file in results_stage1_files:
    #     with open(file, 'r') as results_file:
    #         results = json.load(results_file)
    #         data_from_stage_1.update(results)

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

    # demo_data_stage2 = {
    # }
    # for i, (key, value) in enumerate(data_for_stage_2.items()):
    #     if i == 2:
    #         break
    #     demo_data_stage2.update({key: value})

    logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Start stage 2.')

    requests.post(
        'http://localhost:5000/sorting/stage2',
        json={
            'partitions': data_for_stage_2,
            "config": {
                "file_size": '10MB',
                'nr_files': '10',
                'intervals': '256'
            },
            "experiment_number": experiment_number
        }
    )

    file_found = False
    all_objects = None
    all_obj = None
    files_keys = []
    prefix_results_stage_2 = f'results_stage2/experiment_{experiment_number}_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}/results_stage2_'

    while not file_found:
        all_obj = conn.list_objects(Bucket=results_bucket, Prefix=prefix_results_stage_2)
        all_objects = all_obj.get('Contents')
        # all_objects = my_bucket.objects.filter(Prefix=prefix_results)
        # The length of the files list must be bigger than 1 because it also lists the prefix folder itself
        if all_objects is not None and len(all_objects) == 1:
            file_found = True
        else:
            sleep(1)
    logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finish stage 2.')

    logger.handlers.pop()
    logger.handlers.pop()
    return


if __name__ == '__main__':
    for i in range(3):
        run_sorting_experiment(i, '10', '10MB', '256')
