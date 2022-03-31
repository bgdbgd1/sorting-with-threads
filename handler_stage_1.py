import io
import json
from concurrent.futures.thread import ThreadPoolExecutor
import multiprocessing as mp
from enum import Enum
from threading import Lock

import uuid
# from smart_open import open

import numpy as np
from minio import Minio

from custom_logger import get_logger


class SortingHandlerStage1:
    def __init__(self, read_bucket, intermediate_bucket, write_bucket, status_bucket, initial_files, experiment_number, config, minio_ip, server_number, read_dir=None, write_dir=None, reading_threads=None, det_cat_threads=None, writing_threads=None, no_pipeline_threads=None, **kwargs):
        self.files_read = {}

        self.read_files = 0
        self.determined_categories_files = 0
        self.written_files = 0

        self.max_read = 100
        self.max_determine_categories = 100
        self.max_write = 100

        self.current_read = 0
        self.current_determine_categories = 0
        self.current_write = 0

        self.buffers_filled = 0
        self.max_buffers_filled = 100

        self.reading_threads = ThreadPoolExecutor(max_workers=reading_threads)
        self.determine_categories_threads = ThreadPoolExecutor(max_workers=det_cat_threads)
        self.writing_threads = ThreadPoolExecutor(max_workers=writing_threads)

        # self.no_pipelining_threads = ThreadPoolExecutor(max_workers=no_pipeline_threads)
        # self.no_pipelining_threads = mp.Pool(mp.cpu_count())
        self.no_pipelining_threads = mp.Pool(no_pipeline_threads)
        self.lock_current_read = Lock()
        self.lock_current_determine_categories = Lock()
        self.lock_current_write = Lock()
        self.lock_buffers_filled = Lock()
        self.lock_write_locations = Lock()
        self.lock_written_files = Lock()
        self.lock_logger = Lock()
        self.locations = {}
        self.locations_2 = {}
        self.uuid = uuid.uuid4()

        self.read_bucket = read_bucket
        self.intermediate_bucket = intermediate_bucket
        self.write_bucket = write_bucket
        self.status_bucket = status_bucket
        self.read_dir = read_dir
        self.write_dir = write_dir
        self.initial_files = initial_files
        for file in initial_files:
            self.locations_2[file] = {}

        self.experiment_number = experiment_number
        self.config = config
        self.logger = get_logger(
            'stage_1',
            'stage_1',
            config['nr_files'],
            config['file_size'],
            config['intervals'],
            server_number=server_number
        )
        self.minio_client = Minio(
            f"{minio_ip}:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )

    def write_log_message(self, message):
        with self.lock_logger:
            self.logger.info(message)

    def read_file(self, file_name):
        process_uuid = uuid.uuid4()
        if self.files_read.get(file_name) or self.buffers_filled >= self.max_buffers_filled:
            return
        self.files_read.update({file_name: {'buffer': None, 'status': FileStatusStage1.IN_READ, 'lock': None}})

        with self.lock_current_read:
            self.current_read += 1

        with self.lock_buffers_filled:
            self.buffers_filled += 1

        self.write_log_message(f"experiment_number:{self.experiment_number}; uuid:{process_uuid}; Started reading file.")

        file_content = self.minio_client.get_object(self.read_bucket, file_name).data
        buf = io.BytesIO()
        buf.write(file_content)
        self.files_read[file_name] = {'buffer': buf.getbuffer(), 'status': FileStatusStage1.READ, 'lock': Lock()}
        self.read_files += 1

        self.write_log_message(f"experiment_number:{self.experiment_number}; uuid:{process_uuid}; Finished reading file.")

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

        self.write_log_message(f'experiment_number:{self.experiment_number}; uuid:{process_uuid}; Started sorting determine categories.')

        np_buffer = np.frombuffer(file_info['buffer'], dtype=np.dtype([('key', 'V2'), ('rest', 'V98')]))
        record_arr = np.sort(np_buffer, order='key')

        self.write_log_message(f'experiment_number:{self.experiment_number}; uuid:{process_uuid}; Finished sorting determine categories.')
        file_info['buffer'] = record_arr
        file_info['status'] = FileStatusStage1.DETERMINED_CATEGORIES

        self.write_log_message(
            f'experiment_number:{self.experiment_number}; uuid:{process_uuid}; Started determine categories.')

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
        with self.lock_write_locations:
            self.locations.update(locations)
            self.locations_2[file_name].update(
                {
                    new_file_name: {
                        'start_index': start_index,
                        'end_index': nr_elements,
                        'file_name': file_name
                    }
                }
            )
            self.write_log_message(
                f'experiment_number:{self.experiment_number}; uuid:{process_uuid}; Finished determine categories {file_name}.')

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
        self.write_log_message(
            f'experiment_number:{self.experiment_number}; uuid:{process_uuid}; Started writing file {file_name}.')

        self.minio_client.put_object(self.intermediate_bucket, file_name, io.BytesIO(file_info['buffer'].tobytes()), length=file_info['buffer'].size * 100)

        self.write_log_message(
            f'experiment_number:{self.experiment_number}; uuid:{process_uuid}; Finished writing file {file_name}.')

        file_info['buffer'] = None
        file_info['status'] = FileStatusStage1.WRITTEN
        with self.lock_written_files:
            self.written_files += 1

        with self.lock_current_write:
            self.current_write -= 1

        with self.lock_buffers_filled:
            self.buffers_filled -= 1

    def execute_all_methods(self, file_name):
        process_uuid = uuid.uuid4()
        if self.files_read.get(file_name) or self.buffers_filled >= self.max_buffers_filled:
            return
        self.files_read.update({file_name: {'buffer': None, 'status': FileStatusStage1.IN_READ, 'lock': None}})

        with self.lock_current_read:
            self.current_read += 1

        with self.lock_buffers_filled:
            self.buffers_filled += 1

        ################### READ INITIAL FILE ###################
        self.write_log_message(f"experiment_number:{self.experiment_number}; uuid:{process_uuid}; Started reading file.")

        file_content = self.minio_client.get_object(self.read_bucket, file_name).data

        self.write_log_message(f"experiment_number:{self.experiment_number}; uuid:{process_uuid}; Finished reading file.")

        buf = io.BytesIO()
        buf.write(file_content)
        np_buffer = np.frombuffer(buf.getbuffer(), dtype=np.dtype([('key', 'V2'), ('rest', 'V98')]))

        ################### SORT DETERMINE CATEGORIES ###################
        self.write_log_message(
            f'experiment_number:{self.experiment_number}; uuid:{process_uuid}; Started sorting determine categories.')

        record_arr = np.sort(np_buffer, order='key')

        self.write_log_message(
            f'experiment_number:{self.experiment_number}; uuid:{process_uuid}; Finished sorting determine categories.')

        ################### DETERMINE CATEGORIES ###################
        self.write_log_message(
            f'experiment_number:{self.experiment_number}; uuid:{process_uuid}; Started determine categories.')

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
        with self.lock_write_locations:
            self.locations.update(locations)
            self.locations_2[file_name].update(
                {
                    new_file_name: {
                        'start_index': start_index,
                        'end_index': nr_elements,
                        'file_name': file_name
                    }
                }
            )
        self.write_log_message(
            f'experiment_number:{self.experiment_number}; uuid:{process_uuid}; Finished determine categories {file_name}.')

        ##################### WRITING FILE #####################
        self.write_log_message(
            f'experiment_number:{self.experiment_number}; uuid:{process_uuid}; Started writing file {file_name}.')

        self.minio_client.put_object(
            self.intermediate_bucket,
            file_name,
            io.BytesIO(record_arr.tobytes()),
            length=record_arr.size * 100
        )

        self.write_log_message(
            f'experiment_number:{self.experiment_number}; uuid:{process_uuid}; Finished writing file {file_name}.')

        with self.lock_written_files:
            self.written_files += 1
        with self.lock_current_read:
            self.current_read -= 1
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
        utfcontent = json.dumps(self.locations).encode('utf-8')
        self.minio_client.put_object(
            self.status_bucket,
            f'results_stage1_experiment_{self.experiment_number}_nr_files_{self.config["nr_files"]}_file_size_{self.config["file_size"]}_intervals_{self.config["intervals"]}_{self.uuid}.json',
            io.BytesIO(utfcontent), length=len(utfcontent)
        )

        self.logger.handlers.pop()
        self.logger.handlers.pop()
        print("DONE")
        return self.locations

    def execute_stage1_without_pipelining(self):
        while self.written_files < len(self.initial_files):
            for file in self.initial_files:
                file_data = self.files_read.get(file)
                if (
                        not file_data and
                        self.current_read < self.max_read and
                        self.buffers_filled < self.max_buffers_filled
                ):
                    p = mp.Process(target=self.execute_all_methods, args=(file))
                    p.start()
                    # self.no_pipelining_threads.apply(self.execute_all_methods, self.initial_files)
                    # self.no_pipelining_threads.submit(self.execute_all_methods, file)

        print("WRITING_RESULTS_FILE STAGE 1")
        utfcontent = json.dumps(self.locations).encode('utf-8')
        self.minio_client.put_object(
            self.status_bucket,
            f'no_pipelining_results_stage1_experiment_{self.experiment_number}_nr_files_{self.config["nr_files"]}_file_size_{self.config["file_size"]}_intervals_{self.config["intervals"]}_{self.uuid}.json',
            io.BytesIO(utfcontent), length=len(utfcontent)
        )

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
