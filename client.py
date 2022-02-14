import json
import sys
from time import sleep

import requests
import uuid

from minio import Minio
from custom_logger import get_logger


def run_sorting_experiment(experiment_number, nr_files, file_size, intervals, hosts, minio_ip):
    # Instantiate MinIO client
    minio_client = Minio(
        f"{minio_ip}:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    # Split load evenly on IPs
    # ips = [
    #     'http://localhost:5000',
    #     'http://localhost:5001',
    # ]
    ips = hosts

    files = [str(i) for i in range(int(nr_files))]
    files_per_ip = {}
    for i in range(len(ips)):
        files_per_ip.update(
            {
                ips[i]: files[i * (len(files) // len(ips)): (i+1) * (len(files) // len(ips))]
            }
        )
        if i == len(ips) - 1 and len(files) % len(ips) != 0:
            files_per_ip[ips[i]] += files[(i+1) * (len(files) // len(ips)):]

    process_uuid = uuid.uuid4()
    results_bucket = 'status'
    prefix_results_stage_1 = f'result_stage1_experiment_{experiment_number}_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_'

    logger = get_logger(
        'main_handler',
        'main_handler',
        nr_files,
        file_size,
        intervals
    )

    logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Start stage 1.')

    # Send data to servers
    for ip, data in files_per_ip.items():
        requests.post(
            f'{ip}/sorting/stage1',
            json={
                "file_names": data,
                "config": {
                    "file_size": file_size,
                    'nr_files': nr_files,
                    'intervals': intervals,
                },
                "experiment_number": experiment_number
            }
        )

    # Check if all servers finished STAGE 1
    file_found = False
    object_names = set()
    while not file_found:
        nr_report_files = 0
        all_obj = minio_client.list_objects(bucket_name=results_bucket, prefix=prefix_results_stage_1)
        for obj in all_obj:
            nr_report_files += 1
            object_names.add(obj.object_name)
        if nr_report_files == (len(ips) * experiment_number):
            file_found = True
        else:
            print("Sleeping")
            sleep(3)

    logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finish stage 1.')

    data_from_stage_1 = {}
    data_for_stage_2 = {}

    object_names = list(object_names)

    for result_stage1 in object_names:
        content = json.loads(minio_client.get_object(bucket_name=results_bucket, object_name=result_stage1).data.decode())
        data_from_stage_1.update(content)

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

    listed_data_for_stage_2 = list(data_for_stage_2.items())
    listed_data_for_stage_2_per_ip = {}
    data_for_stage_2_per_ip = {}
    for i, ip in enumerate(ips):
        listed_data_for_stage_2_per_ip.update(
            {
                ip: listed_data_for_stage_2[i * (len(data_for_stage_2) // len(ips)): (i+1) * (len(listed_data_for_stage_2) // len(ips))]
            }
        )
        if i == len(ips) - 1 and len(listed_data_for_stage_2) % len(ips) != 0:
            listed_data_for_stage_2_per_ip[ips[i]] += listed_data_for_stage_2[(i + 1) * (len(listed_data_for_stage_2) // len(ips)):]
    for ip, data in listed_data_for_stage_2_per_ip.items():
        data_for_stage_2_per_ip.update({ip: dict(data)})

    logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Start stage 2.')
    for ip, data in data_for_stage_2_per_ip.items():
        requests.post(
            f'{ip}/sorting/stage2',
            json={
                'partitions': data,
                "config": {
                    "file_size": file_size,
                    'nr_files': nr_files,
                    'intervals': intervals,
                },
                "experiment_number": experiment_number
            }
        )

    file_found = False
    prefix_results_stage_2 = f'results_stage2_experiment_{experiment_number}_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_'
    object_names = set()
    while not file_found:
        nr_report_files = 0
        all_obj = minio_client.list_objects(bucket_name=results_bucket, prefix=prefix_results_stage_2)
        for obj in all_obj:
            nr_report_files += 1
            object_names.add(obj.object_name)
        if nr_report_files == (len(ips) * experiment_number):
            file_found = True
        else:
            print("Sleeping")
            sleep(3)

    logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finish stage 2.')

    logger.handlers.pop()
    logger.handlers.pop()
    return


if __name__ == '__main__':
    for i in range(1, 2):
        run_sorting_experiment(i, '10', '10MB', '256', sys.argv[1], sys.argv[2:])
