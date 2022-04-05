import io
import sys
from multiprocessing import Pool, TimeoutError, Process
import time
import os
from threading import Lock
import numpy as np
import uuid
from minio import Minio
from custom_logger import get_logger

minio_ip = "10.149.0.31"
# minio_ip = '127.0.0.1'
# minio_client = Minio(
#         f"{minio_ip}:9000",
#         access_key="minioadmin",
#         secret_key="minioadmin",
#         secure=False
#     )

lock_logger = Lock()
logger = get_logger(
    'threads_test',
    'threads_test',
    '100',
    '100MB',
    '256',
    server_number=1
)

# def f(x):
#     print(x*x)


def collect_result(result):
    global results
    results.append(result)


def write_log_message(message):
    with lock_logger:
        logger.info(message)


def download_file(filename):
    minio_client = Minio(
        f"{minio_ip}:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    file_content = minio_client.get_object('read', str(filename)).data
    buf = io.BytesIO()
    buf.write(file_content)


def download_and_upload(filename):
    process_uuid = uuid.uuid4()
    experiment_number = 1
    minio_client = Minio(
        f"{minio_ip}:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    ################### READ INITIAL FILE ###################

    write_log_message(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Started reading file {filename}.')
    file_content = minio_client.get_object('read', str(filename)).data
    write_log_message(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished reading file {filename}.')
    buf = io.BytesIO()
    buf.write(file_content)
    np_buffer = np.frombuffer(buf.getbuffer(), dtype=np.dtype([('key', 'V2'), ('rest', 'V98')]))

    # write_log_message(
    #     f'experiment_number:{experiment_number}; uuid:{process_uuid}; Started reading file {filename + 1}.')
    # file_content_2 = minio_client.get_object('read', str(filename + 1)).data
    # write_log_message(
    #     f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished reading file {filename + 1}.')
    # buf_2 = io.BytesIO()
    # buf_2.write(file_content_2)
    # np_buffer_2 = np.frombuffer(buf_2.getbuffer(), dtype=np.dtype([('key', 'V2'), ('rest', 'V98')]))
    ################### SORT DETERMINE CATEGORIES ###################

    write_log_message(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Started sorting determine categories file {filename}.')

    record_arr = np.sort(np_buffer, order='key')

    write_log_message(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished sorting determine categories file {filename}.')

    # write_log_message(
    #     f'experiment_number:{experiment_number}; uuid:{process_uuid}; Started sorting determine categories file {filename + 1}.')
    #
    # record_arr_2 = np.sort(np_buffer_2, order='key')
    #
    # write_log_message(
    #     f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished sorting determine categories file {filename + 1}.')
    ##################### WRITING FILE #####################

    write_log_message(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Started writing file {filename}.')
    minio_client.put_object(
        'intermediate',
        str(filename),
        io.BytesIO(record_arr.tobytes()),
        length=record_arr.size * 100
    )
    write_log_message(
        f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished writing file {filename}.')

    # write_log_message(
    #     f'experiment_number:{experiment_number}; uuid:{process_uuid}; Started writing file {filename + 1}.')
    # minio_client.put_object(
    #     'intermediate',
    #     str(filename + 1),
    #     io.BytesIO(record_arr_2.tobytes()),
    #     length=record_arr_2.size * 100
    # )
    # write_log_message(
    #     f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finished writing file {filename + 1}.')


if __name__ == '__main__':
    # minio_ip = sys.argv[1]
    # start 4 worker processes
    pool = Pool(processes=4)
    for i in range(0, 100):
        pool.apply_async(download_and_upload, args=(i,))
    pool.close()
    pool.join()
    # with Pool(processes=8) as pool:
    #     pool.map_async(download_and_upload, range(100))
    #     pool.close()
    #     pool.join()

