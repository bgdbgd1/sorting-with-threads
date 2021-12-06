import io
import json
from concurrent.futures.thread import ThreadPoolExecutor
from enum import Enum
from time import sleep

import boto3
import numpy as np
from S3File import S3File
from smart_open import open


class SortingHandlerStage2:
    read_bucket = None
    write_bucket = None
    read_dir = None
    write_dir = None

    initial_files = []
    files_in_read = {}
    files_read = {}

    read_files = 0
    sorted_files = 0
    is_result_written = False

    max_read = 3
    max_sort = 1
    max_write = 1

    current_read = 0
    current_sort = 0
    current_write = 0

    buffers_filled = 0
    max_buffers_filled = 1

    reading_threads = ThreadPoolExecutor(max_workers=2)
    sort_threads = ThreadPoolExecutor(max_workers=1)
    writing_threads = ThreadPoolExecutor(max_workers=1)

    def __init__(self, read_bucket, write_bucket, read_dir, write_dir, partitions, **kwargs):
        self.read_bucket = read_bucket
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

    def read_partition(self, partition_name, file_name, start_index, end_index):
        partition_data_in_read = self.files_in_read.get(partition_name)
        if partition_data_in_read and file_name in partition_data_in_read:
            return
        elif partition_data_in_read and not file_name in partition_data_in_read:
            self.files_in_read[partition_name].append(file_name)
        elif not partition_data_in_read and self.buffers_filled < self.max_buffers_filled:
            self.files_in_read.update({partition_name: []})
            self.buffers_filled += 1
        print(f'Reading partition {partition_name} from file {file_name}')
        self.current_read += 1
        s3 = boto3.resource("s3")
        s3_object = s3.Object(bucket_name=self.read_bucket, key=f'{self.read_dir}/{file_name}')
        s3file = S3File(s3_object, position=start_index * 100)
        file_content = s3file.read(size=(end_index + 1) * 100 - start_index * 100)
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
        self.current_read -= 1

    def sort_partition(self, partition_name):
        self.current_sort += 1
        try:
            buffer = io.BytesIO()
            file = self.files_read.get(partition_name)
            for file_name, file_data in file.items():
                buffer.write(file_data['buffer'])

            print("SORT")
            np_array = np.frombuffer(
                buffer.getbuffer(), dtype=np.dtype([('sorted', 'V1'), ('key', 'V9'), ('value', 'V90')])
            )
            np_array = np.sort(np_array, order='key')
            self.files_sorted.update({partition_name: np_array})
            print("FINISHED SORTING")
        except:
            self.current_sort -= 1
            raise
        self.current_sort -= 1
        self.sorted_files += 1

    def write_sorted_file(self, partition_name):
        if partition_name in self.files_written:
            return
        print("Write")
        self.current_write += 1
        with open(f's3://{self.write_bucket}/{self.write_dir}/{partition_name}', 'wb') as file:
            file.write(memoryview(self.files_sorted[partition_name]))
        print("FINISH WRITING")
        self.files_written.append(partition_name)
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
                        if self.current_read < self.max_read and self.buffers_filled <= self.max_buffers_filled:
                            # self.read_partition(
                            #     partition_name,
                            #     file_data['file_name'],
                            #     file_data['start_index'],
                            #     file_data['end_index']
                            # )
                            print("In Reading")
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
                        self.files_read.get(part_name) and
                        len(self.files_read[part_name]) == len(self.partitions[part_name]) and
                        not self.files_sorted.get(part_name) and
                        self.current_sort < self.max_sort
                ):
                    # self.sort_partition(part_name)
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
                    if self.files_read.get(part_name) and len(self.files_read[part_name]) == len(self.partitions[part_name]):
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

        print("DONE")
        # with open(f'results/finished_{next(iter(self.partition_data))}') as res_file:
        #     json.dump({"result": "OK"}, res_file)


class FileStatusStage2(Enum):
    NOT_READ = 'NOT_READ'
    READ = 'READ'
    SORTED = 'SORTED'
    WRITING = 'WRITING'
    WRITTEN = 'WRITTEN'
