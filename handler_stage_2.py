import io
import json
from concurrent.futures.thread import ThreadPoolExecutor
from enum import Enum

import boto3
import numpy as np
from S3File import S3File


class SortingHandlerStage2:
    read_bucket = None
    write_bucket = None
    read_dir = None
    write_dir = None

    initial_files = []
    files_read = {}

    read_files = 0
    sorted_files = 0
    written_files = 0

    max_read = 2
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

    locations = {}

    def __init__(self, read_bucket, write_bucket, read_dir, write_dir, partition_data, **kwargs):
        self.read_bucket = read_bucket
        self.write_bucket = write_bucket
        self.read_dir = read_dir
        self.write_dir = write_dir
        self.partition_data = partition_data

    def raed_partition_v2(self, partition_name, file_name, start_index, end_index):
        partition_data = self.files_read.get(partition_name)
        if partition_data and partition_data.get(file_name):
            return
        elif partition_data and not partition_data.get(file_name):
            self.files_read[partition_name].update(
                {
                    file_name: {
                        'buffer': None,
                        'status': FileStatusStage2.NOT_READ
                    }
                }
            )
        elif not partition_data:
            self.files_read.update(
                {
                    partition_name: {
                        file_name: {
                            'buffer': None,
                            'status': FileStatusStage2.NOT_READ
                        }
                    }
                }
            )

        s3 = boto3.resource("s3")
        self.current_read += 1
        self.buffers_filled += 1
        buf = io.BytesIO()
        s3_object = s3.Object(bucket_name=self.read_bucket, key=f'{self.read_dir}/{file_name}')
        s3file = S3File(s3_object, position=start_index * 100)
        file_content = s3file.read(size=(end_index + 1) * 100 - start_index * 100)
        buf.write(file_content)
        self.files_read[partition_name][file_name]['buffer'] = buf.getbuffer()
        self.files_read[partition_name][file_name]['status'] = FileStatusStage2.READ
        self.read_files += 1
        self.current_read -= 1

    def read_partition(self, partition_name):
        if self.files_read.get(partition_name):
            return
        print(f"Read partition {partition_name}")
        s3 = boto3.resource("s3")

        self.current_read += 1
        self.buffers_filled += 1

        buf = io.BytesIO()

        for file_data in self.partition_data[partition_name]:
            s3_object = s3.Object(bucket_name=self.read_bucket, key=f'{self.read_dir}/{file_data["file_name"]}')
            s3file = S3File(s3_object, position=file_data['start_index'] * 100)
            file_content = s3file.read(
                size=(file_data['end_index'] + 1) * 100 - file_data['start_index'] * 100)
            buf.write(file_content)

        self.files_read.update(
            {
                partition_name: {'buffer': buf.getbuffer(), 'status': FileStatusStage2.READ}
            }
        )
        self.read_files += 1
        self.current_read -= 1

    def sort_partition(self, partition_name):
        if not self.files_read.get(partition_name):
            return
        print(f"Sort partition {partition_name}")
        self.current_sort += 1
        np_array = np.frombuffer(
            self.files_read[partition_name]['buffer'], dtype=np.dtype([('sorted', 'V1'), ('key', 'V9'), ('value', 'V90')])
        )
        np_array = np.sort(np_array, order='key')
        self.files_read[partition_name]['buffer'] = np_array
        self.files_read[partition_name]['status'] = FileStatusStage2.SORTED

        self.current_sort -= 1
        self.sorted_files += 1

    def write_sorted_file(self, partition_name):

        partition = self.files_read.get(partition_name)
        if partition and partition['status'] != FileStatusStage2.SORTED:
            return
        print(f"Write partition {partition_name}")

        self.current_write += 1
        partition['status'] = FileStatusStage2.WRITING
        with open(f's3://{self.write_bucket}/{self.write_dir}/{partition_name}', 'wb') as file:
            file.write(memoryview(partition['buffer']))
        partition['status'] = FileStatusStage2.WRITTEN
        self.written_files += 1
        self.current_write -= 1
        self.buffers_filled -= 1

    def execute_stage2(self):
        while self.written_files < len(self.partition_data):
            for partition_name in self.partition_data:
                partition_data = self.files_read.get(partition_name)
                if (
                        not partition_data and
                        self.current_read < self.max_read and
                        self.buffers_filled < self.max_buffers_filled
                ):
                    self.read_partition(partition_name)
                    # self.reading_threads.submit(self.read_partition, partition_name)
                elif (
                    partition_data and
                    partition_data['status'] == FileStatusStage2.READ and
                    self.current_sort < self.max_sort
                ):
                    self.sort_partition(partition_name)
                    # self.sort_threads.submit(self.sort_partition, partition_name)
                elif (
                    partition_data and
                    partition_data['status'] == FileStatusStage2.SORTED and
                    self.current_write < self.max_write
                ):
                    self.write_sorted_file(partition_name)
                    # self.writing_threads.submit(self.write_sorted_file, partition_name)

        with open(f'results/finished_{next(iter(self.partition_data))}') as res_file:
            json.dump({"result": "OK"}, res_file)

class FileStatusStage2(Enum):
    NOT_READ = 'NOT_READ'
    READ = 'READ'
    SORTED = 'SORTED'
    WRITING = 'WRITING'
    WRITTEN = 'WRITTEN'
