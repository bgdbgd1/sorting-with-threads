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
    written_files = 0
    is_result_written = False
    sorted_buffer = None

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

    def __init__(self, read_bucket, write_bucket, read_dir, write_dir, partition_name, partition_data, **kwargs):
        self.read_bucket = read_bucket
        self.write_bucket = write_bucket
        self.read_dir = read_dir
        self.write_dir = write_dir
        self.partition_name = partition_name
        self.partition_data = partition_data
        self.files_in_read = {partition_name: []}
        self.files_read = {partition_name: {}}

    def read_partition_v2(self, partition_name, file_name, start_index, end_index):
        partition_data_in_read = self.files_in_read.get(partition_name)
        if file_name in partition_data_in_read:
            return
        else:
            self.files_in_read[partition_name].append(file_name)
        print(f'Reading partition {partition_name} from file {file_name}')
        self.current_read += 1
        self.buffers_filled += 1
        s3 = boto3.resource("s3")
        s3_object = s3.Object(bucket_name=self.read_bucket, key=f'{self.read_dir}/{file_name}')
        s3file = S3File(s3_object, position=start_index * 100)
        file_content = s3file.read(size=(end_index + 1) * 100 - start_index * 100)
        self.files_read[partition_name].update({file_name: {'buffer': file_content}})

        self.read_files += 1
        self.current_read -= 1

    def sort_partition_v2(self):
        buffer = io.BytesIO()
        file = self.files_read.get(self.partition_name)
        for file_name, file_data in file.items():
            buffer.write(file_data['buffer'])

        print("SORT")
        self.current_sort += 1
        np_array = np.frombuffer(
            buffer.getbuffer(), dtype=np.dtype([('sorted', 'V1'), ('key', 'V9'), ('value', 'V90')])
        )
        np_array = np.sort(np_array, order='key')
        self.sorted_buffer = np_array
        self.current_sort -= 1
        self.sorted_files += 1

    def write_sorted_file_v2(self):
        if self.sorted_buffer is None:
            return
        print("Write")
        self.current_write += 1
        with open(f's3://{self.write_bucket}/{self.write_dir}/{self.partition_name}', 'wb') as file:
            file.write(memoryview(self.sorted_buffer))
        self.written_files += 1
        self.current_write -= 1
        self.buffers_filled -= 1
        self.is_result_written = True

    def execute_stage2(self):
        while not self.is_result_written:
            while len(self.files_read[self.partition_name]) < len(self.partition_data):
                for file_data in self.partition_data:
                    if self.current_read < self.max_read:
                        self.reading_threads.submit(
                            self.read_partition_v2,
                            self.partition_name,
                            file_data['file_name'],
                            file_data['start_index'],
                            file_data['end_index']
                        )
                        # self.read_partition_v2(
                        #     self.partition_name,
                        #     file_data['file_name'],
                        #     file_data['start_index'],
                        #     file_data['end_index']
                        # )
                    else:
                        print("SLEEP")
                        sleep(1)

            self.sort_partition_v2()
            self.write_sorted_file_v2()

        print("DONE")
        # with open(f'results/finished_{next(iter(self.partition_data))}') as res_file:
        #     json.dump({"result": "OK"}, res_file)


class FileStatusStage2(Enum):
    NOT_READ = 'NOT_READ'
    READ = 'READ'
    SORTED = 'SORTED'
    WRITING = 'WRITING'
    WRITTEN = 'WRITTEN'
