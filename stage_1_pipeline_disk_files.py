import glob
import io
import json
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
    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started initializing minio client file name {file_name}.")
    minio_client = Minio(
        f"{minio_ip}:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished initializing minio client file name {file_name}.")

    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started reading file {file_name}.")
    print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started reading file {file_name}.")

    file_content = minio_client.get_object(read_bucket, file_name).data
    with open(f'{PREFIX}stage_1/server_{SERVER_NUMBER}/read/{file_name}', 'wb') as file_read:
        file_read.write(file_content)
    with open(f'{PREFIX}stage_1/server_{SERVER_NUMBER}/read_finished/{file_name}', 'w+') as file_read:
        file_read.write('ok')
    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished reading file {file_name}.")
    print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished reading file {file_name}.")


def determine_categories(
        file_name,
        experiment_number,
):
    process_uuid = uuid.uuid4()
    buf = io.BytesIO()
    with open(f'{PREFIX}stage_1/server_{SERVER_NUMBER}/read/{file_name}', 'rb') as file_content:
        buf.write(file_content.read())
    # buf.write(file_info['buffer'])
    np_buffer = np.frombuffer(buf.getbuffer(), dtype=np.dtype([('key', 'V2'), ('rest', 'V98')]))
    logger.info(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Started sorting determine categories {file_name}.')
    print(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Started sorting determine categories {file_name}.')
    record_arr = np.sort(np_buffer, order='key')
    with open(f'{PREFIX}stage_1/server_{SERVER_NUMBER}/sorted/{file_name}', 'w') as det_cat_file:
        record_arr.tofile(det_cat_file)
    # TODO: REMOVE READ FILE
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
    # all_locations.update({file_name: locations})

    logger.info(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished determine categories {file_name}.')
    print(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished determine categories {file_name}.')


def write_file(
        file_name,
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
    with open(f'{PREFIX}stage_1/server_{SERVER_NUMBER}/sorted/{file_name}', 'rb') as file:
        file_content = file.read()
    logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Started writing file {file_name}.')
    print(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Started writing file {file_name}.')

    minio_client.put_object(
        intermediate_bucket,
        file_name,
        io.BytesIO(file_content),
        length=os.path.getsize(f'{PREFIX}stage_1/server_{SERVER_NUMBER}/sorted/{file_name}')
    )
    with open(f'{PREFIX}stage_1/server_{SERVER_NUMBER}/written/{file_name}', 'w') as file_written:
        file_written.write('ok')
    logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished writing file {file_name}.')
    print(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished writing file {file_name}.')
    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; write_file Start updating files_read written {file_name}.")

    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finish Start updating files_read written {file_name}.")


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
    pool_read = mp.Pool(nr_reading_processes)
    pool_determine_categories = mp.Pool(nr_det_cat_processes)
    pool_write = mp.Pool(nr_write_processes)
    for file in initial_files:
        # read_file(file, minio_ip, read_bucket, experiment_number)
        pool_read.apply_async(
            read_file,
            args=(
                file,
                minio_ip,
                read_bucket,
                experiment_number
            )
        )

    ok = False
    finished_read = []
    finished_sorted = []
    while not ok:
        read_files = glob.glob(f'{PREFIX}stage_1/server_{SERVER_NUMBER}/read_finished/*')
        for file_read in read_files:
            name_file = file_read.split('/')[-1]
            if name_file not in finished_read:
                finished_read.append(name_file)
                # determine_categories(name_file, experiment_number)
                pool_determine_categories.apply_async(
                    determine_categories,
                    args=(
                        name_file,
                        experiment_number
                    )
                )
        determined_cat_files = glob.glob(f'{PREFIX}stage_1/server_{SERVER_NUMBER}/sorted_finished/*')
        for file_read in determined_cat_files:
            name_file = file_read.split('/')[-1]
            if name_file not in finished_sorted:
                finished_sorted.append(name_file)
                # write_file(name_file, intermediate_bucket, minio_ip, experiment_number)
                pool_write.apply_async(
                    write_file,
                    args=(
                        name_file,
                        intermediate_bucket,
                        minio_ip,
                        experiment_number
                    )
                )

        written_files = glob.glob(f'{PREFIX}stage_1/server_{SERVER_NUMBER}/written/*')
        if len(initial_files) == len(written_files):
            ok = True

    # pool_read.close()
    # pool_write.close()
    # pool_determine_categories.close()
    all_locations = {}
    determined_cat_files = glob.glob(f'{PREFIX}stage_1/server_{SERVER_NUMBER}/sorted_finished/*')
    try:
        for file_read in determined_cat_files:
            with open(file_read, 'r') as locations_content:
                locations = json.load(locations_content)
                all_locations.update(locations)
    except Exception as exc:
        print(f"FILEREAD!!!!!!!!!!!!!!!!!: {file_read}")
        raise
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

    # CLEANUP
    read_files = glob.glob(f'{PREFIX}stage_1/server_{SERVER_NUMBER}/read/*')
    for file in read_files:
        os.remove(file)
    det_cat_files = glob.glob(f'{PREFIX}stage_1/server_{SERVER_NUMBER}/sorted/*')
    for file in det_cat_files:
        os.remove(file)
    read_files = glob.glob(f'{PREFIX}stage_1/server_{SERVER_NUMBER}/read_finished/*')
    for file in read_files:
        os.remove(file)
    det_cat_files = glob.glob(f'{PREFIX}stage_1/server_{SERVER_NUMBER}/sorted_finished/*')
    for file in det_cat_files:
        os.remove(file)
    written_files = glob.glob(f'{PREFIX}stage_1/server_{SERVER_NUMBER}/written/*')
    for file in written_files:
        os.remove(file)
