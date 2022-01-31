import io
import json
from concurrent.futures.thread import ThreadPoolExecutor
from enum import Enum
from threading import Lock

import uuid
# from smart_open import open

import numpy as np
from minio import Minio

from custom_logger import get_logger


class SortingHandlerStage1:
    def __init__(self, read_bucket, intermediate_bucket, write_bucket, read_dir, write_dir, initial_files, experiment_number, config, **kwargs):
        self.files_read = {}

        self.read_files = 0
        self.determined_categories_files = 0
        self.written_files = 0

        self.max_read = 2
        self.max_determine_categories = 1
        self.max_write = 1

        self.current_read = 0
        self.current_determine_categories = 0
        self.current_write = 0

        self.buffers_filled = 0
        self.max_buffers_filled = 2

        self.reading_threads = ThreadPoolExecutor(max_workers=2)
        self.determine_categories_threads = ThreadPoolExecutor(max_workers=1)
        self.writing_threads = ThreadPoolExecutor(max_workers=1)

        self.lock_current_read = Lock()
        self.lock_current_determine_categories = Lock()
        self.lock_current_write = Lock()
        self.lock_buffers_filled = Lock()
        self.locations = {}
        self.uuid = uuid.uuid4()

        self.read_bucket = read_bucket
        self.intermediate_bucket = intermediate_bucket
        self.write_bucket = write_bucket
        self.read_dir = read_dir
        self.write_dir = write_dir
        self.initial_files = initial_files
        self.experiment_number = experiment_number
        self.config = config
        self.logger = get_logger(
            'stage_1',
            'stage_1',
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

    def read_file(self, file_name):
        process_uuid = uuid.uuid4()
        if self.files_read.get(file_name) or self.buffers_filled >= self.max_buffers_filled:
            return
        self.files_read.update({file_name: {'buffer': None, 'status': FileStatusStage1.IN_READ, 'lock': None}})

        with self.lock_current_read:
            self.current_read += 1

        with self.lock_buffers_filled:
            self.buffers_filled += 1

        self.logger.info(f"experiment_number:{self.experiment_number}; uuid:{process_uuid}; Started reading file.")
        file_content = self.minio_client.get_object(self.read_bucket, file_name).data
        # with open(f'{self.read_dir}/{file_name}', 'rb') as file:
        # with open(f's3://{self.read_bucket}/{self.read_dir}/{file_name}', 'rb') as file:
        #     file_content = file.read()
        buf = io.BytesIO()
        buf.write(file_content)
        self.files_read[file_name] = {'buffer': buf.getbuffer(), 'status': FileStatusStage1.READ, 'lock': Lock()}
        self.read_files += 1
        self.logger.info(f"experiment_number:{self.experiment_number}; uuid:{process_uuid}; Finished reading file.")

        with self.lock_current_read:
            self.current_read -= 1

    def determine_categories(self, file_name):
        process_uuid = uuid.uuid4()
        file_info = self.files_read.get(file_name)
        if file_info['status'] != FileStatusStage1.READ:
            return

        with self.lock_current_determine_categories:
            self.current_determine_categories += 1

        file_info['status'] = FileStatusStage1.DETERMINING_CATEGORIES

        self.logger.info(f'experiment_number:{self.experiment_number}; uuid:{process_uuid}; Started sorting determine categories.')
        np_buffer = np.frombuffer(file_info['buffer'], dtype=np.dtype([('key', 'V2'), ('rest', 'V98')]))
        record_arr = np.sort(np_buffer, order='key')
        self.logger.info(f'experiment_number:{self.experiment_number}; uuid:{process_uuid}; Finished sorting determine categories.')
        file_info['buffer'] = record_arr
        file_info['status'] = FileStatusStage1.DETERMINED_CATEGORIES
        self.logger.info(f'experiment_number:{self.experiment_number}; uuid:{process_uuid}; Started determine categories.')

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
        self.locations.update(locations)
        self.logger.info(f'experiment_number:{self.experiment_number}; uuid:{process_uuid}; Finished determine categories {file_name}.')

        self.determined_categories_files += 1

        with self.lock_current_determine_categories:
            self.current_determine_categories -= 1

    def write_file(self, file_name):
        process_uuid = uuid.uuid4()
        file_info = self.files_read.get(file_name)
        if file_info['status'] != FileStatusStage1.DETERMINED_CATEGORIES:
            return

        with self.lock_current_write:
            self.current_write += 1

        file_info['status'] = FileStatusStage1.WRITING
        self.logger.info(f'experiment_number:{self.experiment_number}; uuid:{process_uuid}; Started writing file {file_name}.')
        self.minio_client.put_object(self.intermediate_bucket, file_name, io.BytesIO(file_info['buffer'].tobytes()), length=file_info['buffer'].size * 100)
        # with open(f'{self.write_dir}/{file_name}', 'wb') as file:
        # with open(f's3://{self.write_bucket}/{self.write_dir}/{file_name}', 'wb') as file:
        #     file.write(memoryview(file_info['buffer']))
        self.logger.info(f'experiment_number:{self.experiment_number}; uuid:{process_uuid}; Finished writing file {file_name}.')

        file_info['buffer'] = None
        file_info['status'] = FileStatusStage1.WRITTEN
        self.written_files += 1

        with self.lock_current_write:
            self.current_write -= 1

        with self.lock_buffers_filled:
            self.buffers_filled -= 1

    def execute_stage1(self):
        while self.written_files < len(self.initial_files):
            for file in self.initial_files:
                file_data = self.files_read.get(file)
                if (
                        not file_data and
                        self.current_read < self.max_read and
                        self.buffers_filled < self.max_buffers_filled
                ):
                    # self.read_file(file)
                    self.reading_threads.submit(self.read_file, file)
                elif (
                        file_data and
                        file_data['status'] == FileStatusStage1.READ and
                        self.current_determine_categories < self.max_determine_categories
                ):
                    # self.determine_categories(file)
                    self.determine_categories_threads.submit(self.determine_categories, file)
                elif (
                        file_data and
                        file_data['status'] == FileStatusStage1.DETERMINED_CATEGORIES and
                        self.current_write < self.max_write
                ):
                    # self.write_file(file)
                    self.writing_threads.submit(self.write_file, file)

        print("WRITING_RESULTS_FILE STAGE 1")
        # with open(f's3://{self.write_bucket}/results_stage1/experiment_{self.experiment_number}_nr_files_{self.config["nr_files"]}_file_size_{self.config["file_size"]}_intervals_{self.config["intervals"]}/results_stage1_{self.uuid}.json', 'w') as locations_file:
        #     json.dump(self.locations, locations_file)
        self.logger.handlers.pop()
        self.logger.handlers.pop()
        print("DONE")
        return self.locations


class FileStatusStage1(Enum):
    IN_READ = 'IN_READ'
    READ = 'READ'
    DETERMINING_CATEGORIES = 'DETERMINING_CATEGORIES'
    DETERMINED_CATEGORIES = 'DETERMINED_CATEGORIES'
    WRITING = 'WRITING'
    WRITTEN = 'WRITTEN'
