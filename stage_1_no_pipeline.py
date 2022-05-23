import uuid
import multiprocessing as mp
import io
import json
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
    'no_pipeline'
)


def execute_all_methods(
        file_name,
        files_read,
        all_locations,
        minio_ip,
        read_bucket,
        intermediate_bucket,
        experiment_number,
        files_read_lock,
        written_files
):
    with files_read_lock:
        if files_read.get(file_name):
            return
    minio_client = Minio(
        f"{minio_ip}:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    process_uuid = uuid.uuid4()
    with files_read_lock:
        files_read.update({file_name: {'buffer': None, 'status': 'IN_READ', 'lock': None}})

    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started reading file {file_name}.")

    file_content = minio_client.get_object(read_bucket, file_name).data
    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished reading file {file_name}.")
    buf = io.BytesIO()
    buf.write(file_content)
    np_buffer = np.frombuffer(buf.getbuffer(), dtype=np.dtype([('key', 'V2'), ('rest', 'V98')]))
    logger.info(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Started sorting determine categories {file_name}.')

    record_arr = np.sort(np_buffer, order='key')

    logger.info(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished sorting determine categories {file_name}.')
    logger.info(
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

    ##################### WRITING FILE #####################
    logger.info(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Started writing file {file_name}.')

    minio_client.put_object(
        intermediate_bucket,
        file_name,
        io.BytesIO(record_arr.tobytes()),
        length=record_arr.size * 100
    )
    with files_read_lock:
        written_files.value += 1
    logger.info(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished writing file {file_name}.')
    # logger.handlers.pop()
    # logger.handlers.pop()


def execute_stage_1_no_pipeline(
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
        nr_processes,
):
    process_uuid = uuid.uuid4()
    pool = mp.Pool(nr_processes)
    with mp.Manager() as manager:
        files_read = manager.dict()
        files_read_lock = manager.Lock()
        all_locations = manager.dict()
        written_files = manager.Value('written_files', 0)
        # while written_files.value < len(initial_files):
        for file in initial_files:
            with files_read_lock:
                file_data = files_read.get(file)
            if not file_data:
                pool.apply_async(
                    execute_all_methods,
                    args=(
                        file,
                        files_read,
                        all_locations,
                        minio_ip,
                        read_bucket,
                        intermediate_bucket,
                        experiment_number,
                        files_read_lock,
                        written_files
                    )
                )
                # execute_all_methods(
                #     file,
                #     files_read,
                #     all_locations,
                #     minio_ip,
                #     read_bucket,
                #     intermediate_bucket,
                #     experiment_number,
                # )
        pool.close()
        pool.join()
        minio_client = Minio(
            f"{minio_ip}:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )

        utfcontent = json.dumps(all_locations.values()).encode('utf-8')
        minio_client.put_object(
            status_bucket,
            f'no_pipelining_results_stage1_experiment_{experiment_number}_nr_files_{files_nr}_file_size_{files_size}_intervals_{categories}_{process_uuid}.json',
            io.BytesIO(utfcontent), length=len(utfcontent)
        )
