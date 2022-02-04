import io
import json
import logging
from concurrent.futures.thread import ThreadPoolExecutor
from enum import Enum
from threading import Lock

import boto3
import numpy as np
from minio import Minio

from S3File import S3File
from smart_open import open
import uuid
from custom_logger import get_logger


class SortingHandlerStage2:
    def __init__(self, read_bucket, intermediate_bucket, write_bucket, read_dir, write_dir, partitions, experiment_number, config, **kwargs):
        self.files_in_read = {}
        self.files_read = {}

        self.read_files = 0
        self.sorted_files = 0
        self.is_result_written = False

        self.max_read = 2
        self.max_sort = 1
        self.max_write = 1

        self.current_read = 0
        self.current_sort = 0
        self.current_write = 0

        self.buffers_filled = 0
        self.max_buffers_filled = 1

        self.reading_threads = ThreadPoolExecutor(max_workers=2)
        self.sort_threads = ThreadPoolExecutor(max_workers=1)
        self.writing_threads = ThreadPoolExecutor(max_workers=1)

        self.lock_current_read = Lock()
        self.lock_current_sort = Lock()
        self.lock_current_write = Lock()
        self.lock_buffers_filled = Lock()

        self.uuid = uuid.uuid4()

        self.read_bucket = read_bucket
        self.intermediate_bucket = intermediate_bucket
        self.write_bucket = write_bucket
        self.read_dir = read_dir
        self.write_dir = write_dir
        self.partitions = partitions
        self.files_in_read = {}
        self.files_read = {}
        self.files_in_sorting = {}
        self.files_sorted = {}
        self.files_written = []
        self.partitions_names = [partition_name for partition_name, partition_data in partitions.items()]
        self.experiment_number = experiment_number
        self.config = config
        self.logger = get_logger(
            'stage_2',
            'stage_2',
            config['nr_files'],
            config['file_size'],
            config['intervals']
        )
        self.minio_client = Minio(
            "127.0.0.1:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )

    def read_partition(self, partition_name, file_name, start_index, end_index):
        partition_data_in_read = self.files_in_read.get(partition_name)
        if (
                partition_data_in_read and file_name in partition_data_in_read
        ) or (
            not partition_data_in_read and self.buffers_filled >= self.max_buffers_filled
        ):
            return
        elif partition_data_in_read and file_name not in partition_data_in_read:
            self.files_in_read[partition_name].append(file_name)
        elif not partition_data_in_read and self.buffers_filled < self.max_buffers_filled:
            self.files_in_read.update({partition_name: [file_name]})
            with self.lock_buffers_filled:
                self.buffers_filled += 1

        print(f'Reading partition {partition_name} from file {file_name}')

        with self.lock_current_read:
            self.current_read += 1

        # s3 = boto3.resource("s3")
        process_uuid = uuid.uuid4()
        self.logger.info(f"experiment_number:{self.experiment_number}; uuid:{process_uuid}; Started reading partition.")
        file_content = self.minio_client.get_object(
            bucket_name=self.intermediate_bucket,
            object_name=file_name,
            offset=start_index * 100,
            length=(end_index - start_index + 1) * 100
        ).data
        # with open(f'{self.read_dir}/{file_name}', 'rb') as read_file:
        #     read_file.seek(start_index * 100, 1)
        #     file_content = read_file.read((end_index - start_index + 1) * 100)
        #     read_file.seek(0, 0)
        # s3_object = s3.Object(bucket_name=self.read_bucket, key=f'{self.read_dir}/{file_name}')
        # s3file = S3File(s3_object, position=start_index * 100)
        # file_content = s3file.read(size=(end_index + 1) * 100 - start_index * 100)

        self.logger.info(f"experiment_number:{self.experiment_number}; uuid:{process_uuid}; Finished reading partition.")
        if not self.files_read.get(partition_name):
            self.files_read.update(
                {
                    partition_name: {
                        file_name: {
                            'buffer': file_content
                        }
                    }
                }
            )
        else:
            self.files_read[partition_name].update({file_name: {'buffer': file_content}})

        self.read_files += 1
        with self.lock_current_read:
            self.current_read -= 1

    def sort_partition(self, partition_name):
        if (
            len(self.files_read[partition_name]) != len(self.partitions[partition_name]) or
            self.files_sorted.get(partition_name, None) is not None
        ):
            print(f"fast returned {partition_name}")
            return
        with self.lock_current_sort:
            self.current_sort += 1
        try:
            self.files_sorted.update({partition_name: 'Nothing'})
            buffer = io.BytesIO()
            file = self.files_read.get(partition_name)
            for file_name, file_data in file.items():
                buffer.write(file_data['buffer'])

            print("SORT")

            process_uuid = uuid.uuid4()
            self.logger.info(f"experiment_number:{self.experiment_number}; uuid:{process_uuid}; Started sorting partition.")
            np_array = np.frombuffer(
                buffer.getbuffer(), dtype=np.dtype([('sorted', 'V1'), ('key', 'V9'), ('value', 'V90')])
            )
            np_array = np.sort(np_array, order='key')
            self.logger.info(f"experiment_number:{self.experiment_number}; uuid:{process_uuid}; Finished sorting partition.")
            self.files_sorted.update({partition_name: np_array})

            print("FINISHED SORTING")
        except:
            with self.lock_current_sort:
                self.current_sort -= 1
                self.files_sorted.pop(partition_name)
            raise
        with self.lock_current_sort:
            self.current_sort -= 1
        self.sorted_files += 1

    def write_sorted_file(self, partition_name):
        if partition_name in self.files_written:
            return
        print("Write")
        with self.lock_current_write:
            self.current_write += 1
        process_uuid = uuid.uuid4()
        self.logger.info(f"experiment_number:{self.experiment_number}; uuid:{process_uuid}; Started writing partition.")
        try:
            self.minio_client.put_object(self.write_bucket, partition_name, io.BytesIO(self.files_sorted[partition_name].tobytes()), length=self.files_sorted[partition_name].size * 100)
            # with open(f'{self.write_dir}/{partition_name}', 'wb') as file:
            # with open(f's3://{self.write_bucket}/{self.write_dir}/{partition_name}', 'wb') as file:
            #     file.write(memoryview(self.files_sorted[partition_name]))
        except:
            with self.lock_current_write:
                self.current_write -= 1
            return
        self.logger.info(f"experiment_number:{self.experiment_number}; uuid:{process_uuid}; Finished writing partition.")

        print("FINISH WRITING")
        self.files_written.append(partition_name)
        with self.lock_current_write:
            self.current_write -= 1
        self.buffers_filled -= 1

    def execute_stage2(self):
        is_everything_done = False
        is_everything_read = False
        is_everything_sorted = False
        is_everything_written = False
        while not is_everything_done:
            if not is_everything_read:
                for partition_name, partition_data in self.partitions.items():
                    for file_data in partition_data:
                        if (
                                self.current_read < self.max_read and
                                (
                                        (
                                                self.buffers_filled < self.max_buffers_filled and
                                                not self.files_in_read.get(partition_name)
                                        ) or
                                        (
                                                self.files_in_read.get(partition_name) and
                                                not file_data['file_name'] in self.files_in_read[partition_name]
                                        )
                                )
                        ):
                            # self.read_partition(
                            #     partition_name,
                            #     file_data['file_name'],
                            #     file_data['start_index'],
                            #     file_data['end_index']
                            # )
                            # print("In Reading")
                            self.reading_threads.submit(
                                self.read_partition,
                                partition_name,
                                file_data['file_name'],
                                file_data['start_index'],
                                file_data['end_index']
                            )
            for part_name in self.partitions_names:
                if (
                        not is_everything_sorted and
                        self.files_read.get(part_name, None) is not None and
                        len(self.files_read[part_name]) == len(self.partitions[part_name]) and
                        self.files_sorted.get(part_name, None) is None and
                        self.current_sort < self.max_sort
                ):
                    # self.sort_partition(part_name)
                    print(f"Try sort {part_name}")
                    self.sort_threads.submit(self.sort_partition, part_name)
                if (
                        not is_everything_written and
                        self.files_sorted.get(part_name) is not None and
                        self.current_write < self.max_write
                ):
                    # self.write_sorted_file(part_name)
                    self.writing_threads.submit(self.write_sorted_file, part_name)

            if not is_everything_read:
                truth_list = [False for i in range(len(self.partitions_names))]
                for i, part_name in enumerate(self.partitions_names):
                    if self.files_read.get(part_name) and len(self.files_read[part_name]) == len(
                            self.partitions[part_name]):
                        truth_list[i] = True
                    else:
                        break
                if all(truth_list):
                    is_everything_read = True

            if not is_everything_sorted and len(self.files_sorted) == len(self.partitions_names):
                is_everything_sorted = True

            if not is_everything_written and len(self.files_written) == len(self.partitions_names):
                is_everything_written = True

            if is_everything_read and is_everything_sorted and is_everything_written:
                is_everything_done = True

        print("========FINISH STAGE 2===========")
        # with open(f's3://{self.write_bucket}/results_stage2/experiment_{self.experiment_number}_nr_files_{self.config["nr_files"]}_file_size_{self.config["file_size"]}_intervals_{self.config["intervals"]}/results_stage2_{self.uuid}.json', 'w') as results_file:
        #     json.dump({str(self.uuid): "DONE"}, results_file)
        self.logger.handlers.pop()
        self.logger.handlers.pop()

        print("DONE")


class FileStatusStage2(Enum):
    NOT_READ = 'NOT_READ'
    READ = 'READ'
    SORTED = 'SORTED'
    WRITING = 'WRITING'
    WRITTEN = 'WRITTEN'
