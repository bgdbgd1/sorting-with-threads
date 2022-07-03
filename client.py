import json
import sys
from multiprocessing import Pool
from time import sleep

import requests
import uuid

from minio import Minio
from custom_logger import get_logger
from constants import FILE_NR, FILE_SIZE, CATEGORIES

READING_THREADS_STAGE_1 = 8
DET_CAT_THREADS_STAGE_1 = 8
WRITING_THREADS_STAGE_1 = 8

READING_THREADS_STAGE_2 = 10
SORT_THREADS_STAGE_2 = 8
WRITING_THREADS_STAGE_2 = 6

logger = get_logger(
    'client_handler',
    'client_handler',
    FILE_NR,
    FILE_SIZE,
    CATEGORIES,
    'client_handler',
    'with_pipeline'
)


def call_stage_1_pipeline(ip, data, file_size, nr_files, intervals, experiment_number):
    requests.post(
        f'{ip}/sorting/pipeline/stage1',
        json={
            "file_names": data,
            "config": {
                "file_size": file_size,
                'nr_files': nr_files,
                'intervals': intervals,
            },
            "experiment_number": experiment_number,
            "reading_threads": READING_THREADS_STAGE_1,
            "det_cat_threads": DET_CAT_THREADS_STAGE_1,
            "writing_threads": WRITING_THREADS_STAGE_1
        }
    )


def call_stage_2_pipeline(ip, data, file_size, nr_files, intervals, experiment_number):
    requests.post(
        f'{ip}/sorting/pipeline/stage2',
        json={
            'partitions': data,
            "config": {
                "file_size": file_size,
                'nr_files': nr_files,
                'intervals': intervals,
            },
            "experiment_number": experiment_number,
            "reading_threads": READING_THREADS_STAGE_2,
            "sort_threads": SORT_THREADS_STAGE_2,
            "writing_threads": WRITING_THREADS_STAGE_2
        }
    )


def run_sorting_experiment(experiment_number, nr_files, file_size, intervals, run_local, ips):
    # Instantiate MinIO client
    minio_client = Minio(
        "127.0.0.1:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    process_uuid = uuid.uuid4()
    results_bucket = 'status'
    prefix_results_stage_1 = f'results_stage1_experiment_{experiment_number}_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_'
    if run_local:
        prefix = 'http://192.168.0.'
    else:
        prefix = 'http://10.149.0.'
    # files = [str(i) for i in range(int(nr_files))]
    # files_per_ip = {}

    # for i in range(len(ips)):
    #     ip = f'{prefix}{ips[i]}:5000/'
    #     files_per_ip.update(
    #         {
    #             ip: files[i * (len(files) // len(ips)): (i+1) * (len(files) // len(ips))]
    #         }
    #     )
    #     if i == len(ips) - 1 and len(files) % len(ips) != 0:
    #         files_per_ip[ip] += files[(i+1) * (len(files) // len(ips)):]
    #
    # logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Start stage 1.')
    # print(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Start stage 1.')
    #
    # # # Send data to servers
    # pool_requests = Pool(24)
    # for ip, data in files_per_ip.items():
    #     pool_requests.apply_async(
    #         call_stage_1_pipeline,
    #         args=(
    #             ip,
    #             data,
    #             file_size,
    #             nr_files,
    #             intervals,
    #             experiment_number
    #         )
    #     )
    # pool_requests.close()
    # pool_requests.join()
    # Check if all servers finished STAGE 1
    file_found = False
    object_names = set()
    while not file_found:
        nr_report_files = 0
        all_obj = minio_client.list_objects(bucket_name=results_bucket, prefix=prefix_results_stage_1)
        for obj in all_obj:
            nr_report_files += 1
            object_names.add(obj.object_name)
        if nr_report_files == len(ips):
            file_found = True
        else:
            print("Sleeping")
            sleep(3)

    # logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finish stage 1.')
    # print(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finish stage 1.')
    # return

    data_from_stage_1 = {}
    data_for_stage_2 = {}

    object_names = list(object_names)

    for result_stage1 in object_names:
        content = json.loads(minio_client.get_object(bucket_name=results_bucket, object_name=result_stage1).data.decode())
        data_from_stage_1.update(content)
        # for file_name, file_data in content.items():
        # for file_data in content:
        #     data_from_stage_1.update(file_data)

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
    for i, ip_suffix in enumerate(ips):
        ip = f'{prefix}{ip_suffix}:5000/'

        listed_data_for_stage_2_per_ip.update(
            {
                ip: listed_data_for_stage_2[i * (len(data_for_stage_2) // len(ips)): (i+1) * (len(listed_data_for_stage_2) // len(ips))]
            }
        )
        if i == len(ips) - 1 and len(listed_data_for_stage_2) % len(ips) != 0:
            listed_data_for_stage_2_per_ip[ip] += listed_data_for_stage_2[(i + 1) * (len(listed_data_for_stage_2) // len(ips)):]
    for ip, data in listed_data_for_stage_2_per_ip.items():
        data_for_stage_2_per_ip.update({ip: dict(data)})

    logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Start stage 2.')
    print(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Start stage 2.')
    pool_requests = Pool(24)

    for ip, data in data_for_stage_2_per_ip.items():
        pool_requests.apply_async(
            call_stage_2_pipeline,
            args=(
                ip,
                data,
                file_size,
                nr_files,
                intervals,
                experiment_number
            )
        )
    pool_requests.close()
    pool_requests.join()

    file_found = False
    prefix_results_stage_2 = f'results_stage2_experiment_{experiment_number}_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_'
    object_names = set()
    while not file_found:
        nr_report_files = 0
        all_obj = minio_client.list_objects(bucket_name=results_bucket, prefix=prefix_results_stage_2)
        for obj in all_obj:
            nr_report_files += 1
            object_names.add(obj.object_name)
        if nr_report_files == len(ips):
            file_found = True
        else:
            print("Sleeping")
            sleep(3)

    logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finish stage 2.')
    print(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finish stage 2.')

    # logger.handlers.pop()
    # logger.handlers.pop()
    return


if __name__ == '__main__':
    print(sys.argv)
    for i in range(1, 11):
        run_sorting_experiment(i, '1000', '100MB', '256', False, sys.argv[1:])
