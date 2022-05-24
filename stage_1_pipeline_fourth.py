import io
import json
import multiprocessing as mp

import uuid

import numpy as np
from minio import Minio

from custom_logger import get_logger
from constants import SERVER_NUMBER, FILE_NR, FILE_SIZE, CATEGORIES
max_buffers_filled = 100

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
        files_read,
        buffers_filled,
        minio_ip,
        read_bucket,
        experiment_number,
        files_read_lock,
        files_read_counter,
        scheduled_files_statuses
):
    try:
        with files_read_lock:
            if files_read.get(file_name):
                print("FOUND in READ. RETURNING")
                return
        minio_client = Minio(
            f"{minio_ip}:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )
        process_uuid = uuid.uuid4()
        with files_read_lock:
            files_read.update({file_name: {'buffer': None, 'status': 'IN_READ'}})
        buffers_filled.value += 1

        logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started reading file {file_name}.")
        print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started reading file {file_name}.")

        file_content = minio_client.get_object(read_bucket, file_name).data
        buf = io.BytesIO()
        buf.write(file_content)
        with files_read_lock:
            files_read.update({file_name: {'buffer': file_content, 'status': 'READ', 'length': len(buf.getbuffer())}})
            files_read_counter.value += 1
        logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished reading file {file_name}.")
        print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished reading file {file_name}.")
    except:
        scheduled_files_statuses.update({file_name: 'NOT_SCHEDULED'})


def determine_categories(
        file_name,
        files_read,
        all_locations,
        experiment_number,
        files_read_lock
):
    process_uuid = uuid.uuid4()
    with files_read_lock:
        file_info = files_read.get(file_name)
    if file_info['status'] != 'READ':
        print("Return from DETERMINE CATEGORIES")
        return
    with files_read_lock:
        files_read.update(
            {
                file_name: {
                    'buffer': files_read[file_name]['buffer'],
                    'status': 'DETERMINING_CATEGORIES',
                    'length': files_read[file_name]['length']
                }
            }
        )
    logger.info(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Started sorting determine categories {file_name}.')
    print(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Started sorting determine categories {file_name}.')
    buf = io.BytesIO()
    buf.write(file_info['buffer'])
    np_buffer = np.frombuffer(buf.getbuffer(), dtype=np.dtype([('key', 'V2'), ('rest', 'V98')]))
    record_arr = np.sort(np_buffer, order='key')
    logger.info(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished sorting determine categories {file_name}.')
    print(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished sorting determine categories {file_name}.')
    with files_read_lock:
        files_read.update(
            {
                file_name: {
                    'buffer': record_arr,
                    'status': 'DETERMINED_CATEGORIES'
                }
            }
        )
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
    all_locations.update({file_name: locations})
    logger.info(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished determine categories {file_name}.')
    print(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished determine categories {file_name}.')


def write_file(
        file_name,
        files_read,
        buffers_filled,
        written_files,
        intermediate_bucket,
        minio_ip,
        experiment_number,
        files_read_lock
):
    minio_client = Minio(
        f"{minio_ip}:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    process_uuid = uuid.uuid4()
    file_info = files_read.get(file_name)

    if file_info['status'] != 'DETERMINED_CATEGORIES':
        print("Return from WRITE")
        return
    with files_read_lock:
        files_read.update(
            {
                file_name: {
                    'buffer': files_read[file_name]['buffer'],
                    'status': 'WRITING'
                }
            }
        )
    logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Started writing file {file_name}.')
    print(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Started writing file {file_name}.')
    minio_client.put_object(
        intermediate_bucket,
        file_name,
        io.BytesIO(file_info['buffer'].tobytes()),
        length=file_info['buffer'].size * 100
    )
    logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished writing file {file_name}.')
    print(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished writing file {file_name}.')
    with files_read_lock:
        files_read.update(
            {
                file_name: {
                    'buffer': None,
                    'status': 'WRITTEN'
                }
            }
        )
    written_files.value += 1
    buffers_filled.value -= 1


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
    with mp.Manager() as manager:
        files_read = manager.dict()
        scheduled_files_statuses = manager.dict()
        for file_name in initial_files:
            scheduled_files_statuses.update({file_name: 'NOT_SCHEDULED'})
        files_read_lock = manager.Lock()
        files_read_counter = manager.Value('files_read_counter', 0)
        all_locations = manager.dict()
        buffers_filled = manager.Value('buffers_filled', 0)
        written_files = manager.Value('written_files', 0)
        while written_files.value < len(initial_files):
            for file in initial_files:
                with files_read_lock:
                    file_data = files_read.get(file)
                if (
                        not file_data and files_read_counter.value < len(initial_files) and scheduled_files_statuses[file_data] == 'NOT_SCHEDULED'
                        # buffers_filled.value < max_buffers_filled
                ):
                    pool_read.apply_async(
                        read_file,
                        args=(
                            file,
                            files_read,
                            buffers_filled,
                            minio_ip,
                            read_bucket,
                            experiment_number,
                            files_read_lock,
                            files_read_counter,
                            scheduled_files_statuses
                        )
                    )
                    scheduled_files_statuses[file_data] = 'SCHEDULED'
                elif (
                        file_data and
                        file_data['status'] == 'READ'
                ):
                    pool_determine_categories.apply_async(
                        determine_categories,
                        args=(
                            file,
                            files_read,
                            all_locations,
                            experiment_number,
                            files_read_lock
                        )
                    )
                elif (
                        file_data and
                        file_data['status'] == 'DETERMINED_CATEGORIES'
                ):
                    pool_write.apply_async(
                        write_file,
                        args=(
                            file,
                            files_read,
                            buffers_filled,
                            written_files,
                            intermediate_bucket,
                            minio_ip,
                            experiment_number,
                            files_read_lock
                        )
                    )

        utfcontent = json.dumps(all_locations.values()).encode('utf-8')
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
