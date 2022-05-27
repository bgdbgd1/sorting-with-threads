import io
import json
import multiprocessing as mp

import uuid

import numpy as np
from minio import Minio

from custom_logger import get_logger
from constants import SERVER_NUMBER, FILE_NR, FILE_SIZE, CATEGORIES

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
        files_read_counter,
        scheduled_files_statuses,
        read_buffers
):
    try:
        if files_read.get(file_name):
            print("FOUND in READ. RETURNING")
            return
        process_uuid = uuid.uuid4()
        logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started initializing minio client file name {file_name}.")
        minio_client = Minio(
            f"{minio_ip}:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )
        logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished initializing minio client file name {file_name}.")

        files_read.update({file_name: {'buffer': None, 'status': 'IN_READ'}})
        buffers_filled.value += 1

        logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started reading file {file_name}.")
        print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started reading file {file_name}.")

        file_content = minio_client.get_object(read_bucket, file_name).data
        logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished reading file {file_name}.")
        print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished reading file {file_name}.")
        buf = io.BytesIO()
        buf.write(file_content)
        logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; read_file Start updating files_read {file_name}.")
        files_read.update({file_name: {'buffer': file_content, 'status': 'READ', 'length': len(buf.getbuffer())}})
        logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; read_file Finish updating files_read {file_name}.")
        files_read_counter.value += 1
    except:
        scheduled_files_statuses.update({file_name: 'NOT_SCHEDULED'})
        files_read.pop(file_name)
        # read_buffers.value -= 1
        print("============================ EXCEPTION READ ============================")


def determine_categories(
        file_name,
        files_read,
        all_locations,
        experiment_number,
        scheduled_files_statuses,
        det_buffers
):
    try:
        process_uuid = uuid.uuid4()
        file_info = files_read.get(file_name)
        if file_info['status'] != 'READ':
            print("Return from DETERMINE CATEGORIES")
            return
        logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; det_cat Start updating files_read determining_categories {file_name}.")
        files_read.update(
            {
                file_name: {
                    'buffer': files_read[file_name]['buffer'],
                    'status': 'DETERMINING_CATEGORIES',
                    'length': files_read[file_name]['length']
                }
            }
        )
        logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; det_cat Finish updating files_read determining_categories {file_name}.")
        buf = io.BytesIO()
        buf.write(file_info['buffer'])
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
        all_locations.update({file_name: locations})
        logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; det_cat Start updating files_read determined_categories {file_name}.")
        files_read.update(
            {
                file_name: {
                    'buffer': record_arr,
                    'status': 'DETERMINED_CATEGORIES'
                }
            }
        )
        logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; det_cat Finish updating files_read determined_categories {file_name}.")

        logger.info(
            f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished determine categories {file_name}.')
        print(
            f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished determine categories {file_name}.')
    except Exception:
        scheduled_files_statuses.update({file_name: 'SCHEDULED_READ'})
        # det_buffers.value -= 1
        print("============================ EXCEPTION DETERMINE CATEGORY ============================")


def write_file(
        file_name,
        files_read,
        buffers_filled,
        written_files,
        intermediate_bucket,
        minio_ip,
        experiment_number,
        scheduled_files_statuses,
        read_buffers,
        det_buffers,
):
    try:
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
        logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; write_file Start updating files_read writing {file_name}.")
        files_read.update(
            {
                file_name: {
                    'buffer': files_read[file_name]['buffer'],
                    'status': 'WRITING'
                }
            }
        )
        logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; write_file Finish updating files_read writing {file_name}.")

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
        logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; write_file Start updating files_read written {file_name}.")

        files_read.update(
            {
                file_name: {
                    'buffer': None,
                    'status': 'WRITTEN'
                }
            }
        )
        logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finish Start updating files_read written {file_name}.")

        written_files.value += 1
        buffers_filled.value -= 1
        # read_buffers.value -= 1
        # det_buffers.value -= 1
    except Exception:
        scheduled_files_statuses.update({file_name: 'SCHEDULED_DET_CAT'})
        print("============================ EXCEPTION WRITE ============================")
        # scheduled_files_statuses[file_name] =


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
        files_read_counter = manager.Value('i', 0)
        all_locations = manager.dict()
        buffers_filled = manager.Value('i', 0)
        written_files = manager.Value('i', 0)
        max_read_buffers_filled = 15
        max_det_cat_buffers_filled = 15
        read_buffers = manager.Value('i', 0)
        det_buffers = manager.Value('i', 0)

        while written_files.value < len(initial_files):
            for file in initial_files:
                file_data = files_read.get(file)
                if (
                        not file_data and
                        files_read_counter.value < len(initial_files) and
                        scheduled_files_statuses[file] == 'NOT_SCHEDULED'
                        # and read_buffers.value < max_read_buffers_filled
                ):
                    scheduled_files_statuses[file] = 'SCHEDULED_READ'
                    # read_buffers.value += 1
                    pool_read.apply_async(
                        read_file,
                        args=(
                            file,
                            files_read,
                            buffers_filled,
                            minio_ip,
                            read_bucket,
                            experiment_number,
                            files_read_counter,
                            scheduled_files_statuses,
                            read_buffers
                        )
                    )
                if (
                        file_data and
                        file_data['status'] == 'READ' and
                        scheduled_files_statuses[file] == 'SCHEDULED_READ'
                        # and det_buffers.value < max_det_cat_buffers_filled
                ):
                    scheduled_files_statuses[file] = 'SCHEDULED_DET_CAT'
                    # det_buffers.value += 1
                    pool_determine_categories.apply_async(
                        determine_categories,
                        args=(
                            file,
                            files_read,
                            all_locations,
                            experiment_number,
                            scheduled_files_statuses,
                            det_buffers
                        )
                    )
                if (
                        file_data and
                        file_data['status'] == 'DETERMINED_CATEGORIES' and
                        scheduled_files_statuses[file] == 'SCHEDULED_DET_CAT'
                ):
                    scheduled_files_statuses[file] = 'SCHEDULED_WRITE'
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
                            scheduled_files_statuses,
                            read_buffers,
                            det_buffers
                        )
                    )
            for key, val in scheduled_files_statuses.items():
                print(f'==============SCHEDULED Key: {key}  Value: {val} ==============')
            for key, val in files_read.items():
                print(f'==============FILES READ Key: {key}  Value: {val} ==============')
            print(f'written files: {written_files.value}')
        pool_read.close()
        pool_write.close()
        pool_determine_categories.close()
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
