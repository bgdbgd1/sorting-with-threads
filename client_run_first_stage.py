import json
import sys
from time import sleep

import requests
import uuid

from minio import Minio
from custom_logger import get_logger


def run_sorting_experiment(experiment_number, nr_files, file_size, intervals, minio_ip, hosts):
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
    prefix_results_stage_1 = f'no_pipelining_results_stage1_experiment_{experiment_number}_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_'

    logger = get_logger(
        'main_handler',
        'main_handler',
        nr_files,
        file_size,
        intervals,
        server_number='main_handler'
    )

    logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Start stage 1.')
    # Send data to servers
    for ip, data in files_per_ip.items():
        requests.post(
            f'{ip}/sorting/no-pipeline/stage1',
            json={
                "file_names": data,
                "config": {
                    "file_size": file_size,
                    'nr_files': nr_files,
                    'intervals': intervals,
                },
                "experiment_number": experiment_number,
                "no_pipeline_threads": 4
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


if __name__ == '__main__':
    print(sys.argv)
    for i in range(1, 2):
        run_sorting_experiment(i, '100', '100MB', '256', sys.argv[1], sys.argv[2:])
