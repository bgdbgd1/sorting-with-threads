import glob
import io
import multiprocessing as mp
import os

import uuid

import numpy as np
from minio import Minio

from custom_logger import get_logger
from constants import SERVER_NUMBER, FILE_NR, FILE_SIZE, CATEGORIES

max_buffers_filled = 100
logger = get_logger(
    'stage_2',
    'stage_2',
    FILE_NR,
    FILE_SIZE,
    CATEGORIES,
    SERVER_NUMBER,
    'with_pipeline'
)


def read_partition(
        partition_name,
        file_name,
        start_index,
        end_index,
        intermediate_bucket,
        minio_ip,
        experiment_number,
):
    process_uuid = uuid.uuid4()
    minio_client = Minio(
        f"{minio_ip}:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started reading partition {partition_name} from file {file_name}.")
    print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started reading partition {partition_name} from file {file_name}.")

    file_content = minio_client.get_object(
        bucket_name=intermediate_bucket,
        object_name=file_name,
        offset=start_index * 100,
        length=(end_index - start_index + 1) * 100
    ).data
    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished reading partition {partition_name} from file {file_name}.")
    print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished reading partition {partition_name} from file {file_name}.")
    if not os.path.isdir(f'stage_2/server_{SERVER_NUMBER}/read/{partition_name}'):
        os.mkdir(f'stage_2/server_{SERVER_NUMBER}/read/{partition_name}')
    with open(f'stage_2/server_{SERVER_NUMBER}/read/{partition_name}/{file_name}', 'wb') as partition_file:
        partition_file.write(file_content)

    if not os.path.isdir(f'stage_2/server_{SERVER_NUMBER}/read_finished/{partition_name}'):
        os.mkdir(f'stage_2/server_{SERVER_NUMBER}/read_finished/{partition_name}')
    with open(f'stage_2/server_{SERVER_NUMBER}/read_finished/{partition_name}/{file_name}', 'wb') as partition_file:
        partition_file.write(file_content)


def sort_category(
        partition_name,
        experiment_number
):
    process_uuid = uuid.uuid4()
    buffer = io.BytesIO()

    partition_files = glob.glob(f'stage_2/server_{SERVER_NUMBER}/read/{partition_name}/*')
    for file in partition_files:
        with open(file, 'rb') as file_content:
            buffer.write(file_content.read())
    print("SORT")
    np_array = np.frombuffer(
        buffer.getbuffer(), dtype=np.dtype([('sorted', 'V1'), ('key', 'V9'), ('value', 'V90')])
    )
    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started sorting category {partition_name}.")
    print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started sorting category {partition_name}.")
    np_array = np.sort(np_array, order='key')
    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished sorting category {partition_name}.")
    print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished sorting category {partition_name}.")
    with open(f'stage_2/server_{SERVER_NUMBER}/sorted/{partition_name}', 'wb') as sorted_file:
        np_array.tofile(sorted_file)
    with open(f'stage_2/server_{SERVER_NUMBER}/sorted_finished/{partition_name}', 'w') as sorted_finished_file:
        sorted_finished_file.write('ok')
    # TODO: Remove read partition directory


def write_category(
        category_name,
        write_bucket,
        minio_ip,
        experiment_number
):
    process_uuid = uuid.uuid4()
    minio_client = Minio(
        f"{minio_ip}:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    with open(f'stage_2/server_{SERVER_NUMBER}/sorted/{category_name}', 'rb') as file:
        file_content = file.read()

    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started writing category {category_name}.")
    print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started writing category {category_name}.")
    minio_client.put_object(
        write_bucket, category_name, io.BytesIO(file_content), length=os.path.getsize(f'stage_2/server_{SERVER_NUMBER}/sorted/{category_name}')
    )
    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished writing category {category_name}.")
    print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished writing category {category_name}.")
    with open(f'stage_2/server_{SERVER_NUMBER}/written/{category_name}', 'w') as file_written:
        file_written.write('ok')
    # TODO: remove sorted file


def execute_stage_2_pipeline(
        partitions,
        minio_ip,
        intermediate_bucket,
        write_bucket,
        status_bucket,
        files_nr,
        files_size,
        categories,
        server_number,
        experiment_number,
        nr_reading_processes,
        nr_sorting_processes,
        nr_writing_processes
):
    process_uuid = uuid.uuid4()
    pool_read = mp.Pool(nr_reading_processes)
    pool_sort = mp.Pool(nr_sorting_processes)
    pool_write = mp.Pool(nr_writing_processes)


    for partition_name, partition_data in partitions.items():
        for file_data in partition_data:
            # read_partition(
            #     partition_name,
            #     file_data['file_name'],
            #     file_data['start_index'],
            #     file_data['end_index'],
            #     intermediate_bucket,
            #     minio_ip,
            #     experiment_number
            # )
            pool_read.apply_async(
                read_partition,
                args=(
                    partition_name,
                    file_data['file_name'],
                    file_data['start_index'],
                    file_data['end_index'],
                    intermediate_bucket,
                    minio_ip,
                    experiment_number
                )
            )

    partitions_read = []
    partitions_sorted = []
    ok = False
    while not ok:
        for partition_name in partitions:
            partition_files = glob.glob(f'stage_2/server_{SERVER_NUMBER}/read_finished/{partition_name}/*')

            if partition_name not in partitions_read and len(partition_files) == len(partitions[partition_name]):
                partitions_read.append(partition_name)
                # sort_category(
                #     partition_name,
                #     experiment_number
                # )
                pool_sort.apply_async(
                    sort_category,
                    args=(
                        partition_name,
                        experiment_number
                    )
                )

            if partition_name not in partitions_read:
                print("PARTITION NOT IN PARTITIONS_READ")
            if len(partition_files) != len(partitions[partition_name]):
                print("LENGTH DOES NOT MATCH")
        partition_files = glob.glob(f'stage_2/server_{SERVER_NUMBER}/sorted_finished/*')
        for file in partition_files:
            name_file = file.split('/')[-1]
            if name_file not in partitions_sorted:
                partitions_sorted.append(name_file)
                pool_write.apply_async(
                    write_category,
                    args=(
                        name_file,
                        write_bucket,
                        minio_ip,
                        experiment_number
                    )
                )
                # write_category(
                #     name_file,
                #     write_bucket,
                #     minio_ip,
                #     experiment_number
                # )
        written_files = glob.glob(f'stage_2/server_{SERVER_NUMBER}/written/*')
        if len(written_files) == len(partitions):
            ok = True

    logger.info("========FINISH STAGE 2===========")
    minio_client = Minio(
        f"{minio_ip}:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    minio_client.put_object(
        status_bucket,
        f'results_stage2_experiment_{experiment_number}_nr_files_{files_nr}_file_size_{files_size}_intervals_{categories}_{process_uuid}.json',
        io.BytesIO(b'done'), length=4)

    read_files = glob.glob(f'stage_2/server_{SERVER_NUMBER}/read/*')
    for file in read_files:
        os.remove(file)
    det_cat_files = glob.glob(f'stage_2/server_{SERVER_NUMBER}/sorted/*')
    for file in det_cat_files:
        os.remove(file)
    read_files = glob.glob(f'stage_2/server_{SERVER_NUMBER}/read_finished/*')
    for file in read_files:
        os.remove(file)
    det_cat_files = glob.glob(f'stage_2/server_{SERVER_NUMBER}/sorted_finished/*')
    for file in det_cat_files:
        os.remove(file)
    written_files = glob.glob(f'stage_2/server_{SERVER_NUMBER}/written/*')
    for file in written_files:
        os.remove(file)
