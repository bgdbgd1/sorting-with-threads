import uuid
import multiprocessing as mp
import io
import numpy as np
from minio import Minio
from custom_logger import get_logger
from constants import FILE_NR, FILE_SIZE, CATEGORIES, SERVER_NUMBER

logger = get_logger(
    'stage_2',
    'stage_2',
    FILE_NR,
    FILE_SIZE,
    CATEGORIES,
    SERVER_NUMBER,
    'no_pipeline'
)


def execute_all_methods(
        partition_name,
        partition_data,
        minio_ip,
        intermediate_bucket,
        final_bucket,
        experiment_number,
):

    minio_client = Minio(
        f"{minio_ip}:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    process_uuid = uuid.uuid4()
    buffer = io.BytesIO()

    ################### READ PARTITIONS ###################
    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started reading category {partition_name}.")
    print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started reading category {partition_name}.")

    for data in partition_data:
        logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started reading partition file {data['file_name']} for category {partition_name}.")
        print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started reading partition file {data['file_name']} for category {partition_name}.")

        file_content = minio_client.get_object(
            bucket_name=intermediate_bucket,
            object_name=data['file_name'],
            offset=data['start_index'] * 100,
            length=(data['end_index'] - data['start_index'] + 1) * 100
        ).data
        logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished reading partition file {data['file_name']} for category {partition_name}.")
        print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished reading partition file {data['file_name']} for category {partition_name}.")

        buffer.write(file_content)
    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished reading category {partition_name}.")
    print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished reading category {partition_name}.")

    ################### SORT CATEGORY #################
    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started sorting category {partition_name}.")
    print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started sorting category {partition_name}.")

    np_array = np.frombuffer(
        buffer.getbuffer(), dtype=np.dtype([('sorted', 'V1'), ('key', 'V9'), ('value', 'V90')])
    )
    np_array = np.sort(np_array, order='key')

    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished sorting category {partition_name}.")
    print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished sorting category {partition_name}.")

    ################### WRITE CATEGORY ###################
    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started writing category {partition_name}.")
    print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started writing category {partition_name}.")

    minio_client.put_object(final_bucket, partition_name, io.BytesIO(np_array.tobytes()), length=np_array.size * 100)

    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished writing category {partition_name}.")
    print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished writing category {partition_name}.")
    # logger.handlers.pop()
    # logger.handlers.pop()


def execute_stage_2_no_pipeline(
        partitions,
        minio_ip,
        intermediate_bucket,
        final_bucket,
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
    for partition_name, partition_data in partitions.items():
        pool.apply_async(
            execute_all_methods,
            args=(
                partition_name,
                partition_data,
                minio_ip,
                intermediate_bucket,
                final_bucket,
                experiment_number,
            )
        )
        # execute_all_methods(
        #     partition_name,
        #     partition_data,
        #     minio_ip,
        #     intermediate_bucket,
        #     final_bucket,
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
    minio_client.put_object(
        status_bucket,
        f'no_pipelining_results_stage2_experiment_{experiment_number}_nr_files_{files_nr}_file_size_{files_size}_intervals_{categories}_{process_uuid}.json',
        io.BytesIO(b'done'), length=4)
