import glob
import io
import json
import multiprocessing
import multiprocessing as mp
import os

import uuid

import numpy as np
from minio import Minio

from custom_logger import get_logger
from constants import SERVER_NUMBER, FILE_NR, FILE_SIZE, CATEGORIES, PREFIX

logger = get_logger(
    'stage_1',
    'stage_1',
    FILE_NR,
    FILE_SIZE,
    CATEGORIES,
    SERVER_NUMBER,
    'with_pipeline'
)


def read_file(
        file_name,
        minio_ip,
        read_bucket,
        experiment_number,
):
    process_uuid = uuid.uuid4()
    minio_client = Minio(
        f"{minio_ip}:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started reading file {file_name}.")
    print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started reading file {file_name}.")

    file_content = minio_client.get_object(read_bucket, file_name).data
    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished reading file {file_name}.")
    print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished reading file {file_name}.")
    return {
        "file_name": file_name,
        "file_content": file_content,
    }


def determine_categories(
        file_name,
        file_content,
        experiment_number,
):
    process_uuid = uuid.uuid4()
    buf = io.BytesIO()
    buf.write(file_content)
    np_buffer = np.frombuffer(buf.getbuffer(), dtype=np.dtype([('key', 'V2'), ('rest', 'V98')]))
    logger.info(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Started sorting determine categories {file_name}.')
    print(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Started sorting determine categories {file_name}.')
    record_arr = np.sort(np_buffer, order='key')
    logger.info(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished sorting determine categories {file_name}.')
    print(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished sorting determine categories {file_name}.')
    logger.info(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Started determine categories {file_name}.')
    print(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Started determine categories {file_name}.')
    locations = {file_name: {}}
    num_subcats = 1
    first_char = None
    start_index = 0
    current_file_number_per_first_char = 1
    diff = 256 // num_subcats
    lower_margin = 0
    upper_margin = diff
    new_file_name = ''
    nr_elements = 0
    for nr_elements, rec in enumerate(record_arr):
        key_array = bytearray(rec[0])
        if first_char is None:
            first_char = key_array[0]
        new_file_name = f'{first_char}_{current_file_number_per_first_char}'

        if key_array[0] != first_char or (key_array[1] < lower_margin or key_array[1] > upper_margin):

            # TODO: update this to store it per initial file
            locations[file_name][new_file_name] = {
                'start_index': start_index,
                'end_index': nr_elements - 1,
                'file_name': file_name
            }

            if key_array[0] != first_char:
                current_file_number_per_first_char = 1
                start_index = nr_elements
                lower_margin = 0
                upper_margin = diff
                first_char = key_array[0]
            else:
                current_file_number_per_first_char += 1
                lower_margin = upper_margin + 1
                upper_margin = lower_margin + diff
                start_index = nr_elements

    locations[file_name][new_file_name] = {
        'start_index': start_index,
        'end_index': nr_elements,
        'file_name': file_name
    }
    with open(f'{PREFIX}stage_1/server_{SERVER_NUMBER}/sorted_finished/{file_name}', 'w') as write_locations:
        json.dump(locations, write_locations)

    logger.info(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished determine categories {file_name}.')
    print(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished determine categories {file_name}.')
    return {'file_name': file_name, 'file_content': record_arr, 'locations': locations}


def write_file(
        file_name,
        file_content,
        intermediate_bucket,
        minio_ip,
        experiment_number,
):
    minio_client = Minio(
        f"{minio_ip}:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    process_uuid = uuid.uuid4()
    logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Started writing file {file_name}.')
    print(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Started writing file {file_name}.')

    minio_client.put_object(
        intermediate_bucket,
        file_name,
        io.BytesIO(file_content.tobytes()),
        length=file_content.size * 100
    )
    logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished writing file {file_name}.')
    print(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished writing file {file_name}.')


def read_worker(
        queue_read,
        queue_determine_categories,
        minio_ip,
        read_bucket,
        experiment_number
):
    while True:
        item = queue_read.get(block=True)
        if item is None:
            queue_determine_categories.put(None)
            break
        file_content_data = read_file(
            item,
            minio_ip,
            read_bucket,
            experiment_number
        )
        queue_determine_categories.put(file_content_data)


def determine_categories_worker(
        queue_determine_categories,
        queue_locations,
        queue_write,
        experiment_number
):
    while True:
        item = queue_determine_categories.get(block=True)
        if item is None:
            queue_write.put(None)
            break
        det_cat_result = determine_categories(
            item['file_name'],
            item['file_content'],
            experiment_number
        )
        queue_write.put(
            {
                'file_name': det_cat_result['file_name'],
                'file_content': det_cat_result['file_content']
            }
        )
        queue_locations.put(det_cat_result['locations'])


def write_worker(
        queue_write,
        minio_ip,
        intermediate_bucket,
        experiment_number
):
    while True:
        item = queue_write.get(block=True)
        if item is None:
            break
        write_file(
            item['file_name'],
            item['file_content'],
            intermediate_bucket,
            minio_ip,
            experiment_number
        )


def execute_stage_1_pipeline(
        initial_files,
        minio_ip,
        read_bucket,
        intermediate_bucket,
        status_bucket,
        files_nr,
        files_size,
        categories,
        server_number,
        experiment_number,
        nr_reading_processes,
        nr_det_cat_processes,
        nr_write_processes
):
    process_uuid = uuid.uuid4()

    queue_read = multiprocessing.Queue()
    queue_determine_categories = multiprocessing.Queue()
    queue_locations = multiprocessing.Queue()
    queue_write = multiprocessing.Queue()

    pool_read = mp.Pool(
        nr_reading_processes,
        read_worker,
        (queue_read, queue_determine_categories, minio_ip, read_bucket, experiment_number)
    )
    pool_determine_categories = mp.Pool(
        nr_det_cat_processes,
        determine_categories_worker,
        (queue_determine_categories, queue_locations, queue_write, experiment_number)
    )
    pool_write = mp.Pool(
        nr_write_processes,
        write_worker,
        (queue_write, minio_ip, intermediate_bucket, experiment_number)
    )

    for file in initial_files:
        queue_read.put(file)

    for i in range(nr_reading_processes):
        queue_read.put(None)

    queue_read.close()
    queue_read.join_thread()

    queue_determine_categories.close()
    queue_determine_categories.join_thread()

    queue_locations.close()
    queue_locations.join_thread()

    queue_write.close()
    queue_write.join_thread()

    pool_read.close()
    pool_read.join()

    all_locations = {}
    for i in range(len(initial_files)):
        all_locations.update(queue_locations.get())

    utfcontent = json.dumps(all_locations).encode('utf-8')
    minio_client = Minio(
        f"{minio_ip}:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    minio_client.put_object(
        status_bucket,
        f'results_stage1_experiment_{experiment_number}_nr_files_{files_nr}_file_size_{files_size}_intervals_{categories}_{process_uuid}.json',
        io.BytesIO(utfcontent), length=len(utfcontent)
    )
