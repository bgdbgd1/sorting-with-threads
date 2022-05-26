import io
import multiprocessing as mp

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
        files_in_read,
        files_read,
        files_in_read_lock,
        files_read_lock,
        partition_name,
        file_name,
        start_index,
        end_index,
        intermediate_bucket,
        minio_ip,
        experiment_number,
        scheduled_files_statuses
):
    try:
        with files_in_read_lock:
            partition_data_in_read = files_in_read.get(partition_name)
            if partition_data_in_read and file_name in partition_data_in_read:
                print("RETURNING from READ")
                return
            elif partition_data_in_read and file_name not in partition_data_in_read:
                new_in_read = files_in_read[partition_name]
                new_in_read.append(file_name)
                files_in_read.update({partition_name: new_in_read})
            elif not partition_data_in_read:
                files_in_read.update({partition_name: [file_name]})

        # print(f'Reading partition {partition_name} from file {file_name}')

        process_uuid = uuid.uuid4()
        logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started reading partition {partition_name} from file {file_name}.")
        print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started reading partition {partition_name} from file {file_name}.")

        minio_client = Minio(
            f"{minio_ip}:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )
        file_content = minio_client.get_object(
            bucket_name=intermediate_bucket,
            object_name=file_name,
            offset=start_index * 100,
            length=(end_index - start_index + 1) * 100
        ).data
        logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished reading partition {partition_name} from file {file_name}.")
        print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished reading partition {partition_name} from file {file_name}.")

        with files_read_lock:
            if not files_read.get(partition_name):
                files_read.update(
                    {
                        partition_name: {
                            file_name: {
                                'buffer': file_content
                            }
                        }
                    }
                )
            else:
                new_files_read = files_read[partition_name]
                new_files_read.update(
                    {
                        file_name: {
                            'buffer': file_content
                        }
                    }
                )
                files_read.update({partition_name: new_files_read})
    except Exception:
        scheduled_files_statuses[partition_name][file_name] = 'NOT_SCHEDULED'

def sort_category(
        files_read,
        files_sorted,
        partition_name,
        partitions_length,
        experiment_number
):
    if (
            len(files_read[partition_name]) != partitions_length or
            files_sorted.get(partition_name, None) is not None
    ):
        print("RETURNING from SORT CATEGORY")
    process_uuid = uuid.uuid4()
    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started sorting partition.")
    print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started sorting category {partition_name}.")

    files_sorted.update({partition_name: 'Nothing'})
    buffer = io.BytesIO()
    file = files_read.get(partition_name)

    for file_name, file_data in file.items():
        buffer.write(file_data['buffer'])
    print("SORT")

    np_array = np.frombuffer(
        buffer.getbuffer(), dtype=np.dtype([('sorted', 'V1'), ('key', 'V9'), ('value', 'V90')])
    )
    np_array = np.sort(np_array, order='key')
    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished sorting category {partition_name}.")
    print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished sorting category {partition_name}.")
    files_sorted.update({partition_name: np_array})
    # print("FINISHED SORTING")


def write_category(
        files_written,
        files_sorted,
        category_name,
        write_bucket,
        minio_ip,
        experiment_number
):
    if files_written.get(category_name):
        print("RETURN from WRITE")
        return
    process_uuid = uuid.uuid4()
    minio_client = Minio(
        f"{minio_ip}:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started writing category {category_name}.")
    print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Started writing category {category_name}.")
    minio_client.put_object(write_bucket, category_name,
                            io.BytesIO(files_sorted[category_name].tobytes()),
                            length=files_sorted[category_name].size * 100)
    logger.info(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished writing category {category_name}.")
    print(f"experiment_number:{experiment_number}; uuid:{process_uuid}; Finished writing category {category_name}.")
    try:
        files_written.update({category_name: 1})
        # files_written[position] = category_int
    except Exception as exc:
        print(f'category_name: {category_name}')
        raise exc
    files_sorted.update({category_name: None})


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

    partitions_names = [partition_name for partition_name, partition_data in partitions.items()]
    with mp.Manager() as manager:
        files_in_read = manager.dict()
        scheduled_files_statuses = manager.dict()
        for partition_name, partition_data in partitions.items():
            scheduled_files_statuses[partition_name] = {file_data['file_name']: 'NOT_SCHEDULED' for file_data in partition_data }
        files_read = manager.dict()
        files_sorted = manager.dict()
        files_in_read_lock = manager.Lock()
        files_read_lock = manager.Lock()
        buffers_filled = manager.Value('buffers_filled', 0)
        files_written = manager.dict()
        while len(files_written) < len(partitions_names):
            for partition_name, partition_data in partitions.items():
                if (
                        files_read.get(partition_name) is not None and
                        len(files_read[partition_name]) == len(partitions[partition_name]) and
                        files_sorted.get(partition_name) is None
                ):
                    pool_sort.apply_async(
                        sort_category,
                        args=(
                            files_read,
                            files_sorted,
                            partition_name,
                            len(partitions[partition_name]),
                            experiment_number
                        )
                    )
                elif (
                        files_sorted.get(partition_name) is not None and
                        files_written.get(partition_name) is None
                ):
                    pool_write.apply_async(
                        write_category,
                        args=(
                            files_written,
                            files_sorted,
                            partition_name,
                            write_bucket,
                            minio_ip,
                            experiment_number
                        )
                    )
                elif files_written.get(partition_name) is not None:
                    continue
                else:
                    partition_data_in_read = None
                    with files_in_read_lock:
                        partition_data_in_read = files_in_read.get(partition_name)
                    if not partition_data_in_read:
                        files_in_read.update({partition_name: []})
                    for file_data in partition_data:
                        is_ok_to_read = False
                        with files_in_read_lock:
                            is_ok_to_read = (file_data['file_name'] not in files_in_read[partition_name])
                        if is_ok_to_read and scheduled_files_statuses[partition_name][file_data['file_name']] == 'NOT_SCHEDULED':
                            scheduled_files_statuses[partition_name][file_data['file_name']] = 'SCHEDULED'
                            files_in_read[partition_name].append(file_data['file_name'])
                            pool_read.apply_async(
                                read_partition,
                                args=(
                                    files_in_read,
                                    files_read,
                                    files_in_read_lock,
                                    files_read_lock,
                                    partition_name,
                                    file_data['file_name'],
                                    file_data['start_index'],
                                    file_data['end_index'],
                                    intermediate_bucket,
                                    minio_ip,
                                    experiment_number,
                                    scheduled_files_statuses
                                )
                            )
            # for part_name in partitions_names:
            #     if (
            #             files_read.get(part_name) is not None and
            #             len(files_read[part_name]) == len(partitions[part_name]) and
            #             files_sorted.get(part_name) is None
            #     ):
            #         pool_sort.apply_async(
            #             sort_category,
            #             args=(
            #                 files_read,
            #                 files_sorted,
            #                 part_name,
            #                 len(partitions[part_name]),
            #                 experiment_number
            #             )
            #         )
            #
            #     if files_sorted.get(part_name) is not None and files_written.get(part_name) is None:
            #         pool_write.apply_async(
            #             write_category,
            #             args=(
            #                 files_written,
            #                 files_sorted,
            #                 part_name,
            #                 write_bucket,
            #                 minio_ip,
            #                 experiment_number
            #             )
            #         )
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
