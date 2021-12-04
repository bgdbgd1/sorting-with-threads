import io
import json
from concurrent.futures.thread import ThreadPoolExecutor
from enum import Enum

import numpy as np


class SortingHandlerStage1:
    read_dir = None
    write_dir = None

    initial_files = []
    files_read = {}

    read_files = 0
    determined_categories_files = 0
    written_files = 0

    max_read = 2
    max_determine_categories = 1
    max_write = 1

    current_read = 0
    current_determine_categories = 0
    current_write = 0

    buffers_filled = 0
    max_buffers_filled = 2

    reading_threads = ThreadPoolExecutor(max_workers=2)
    determine_categories_threads = ThreadPoolExecutor(max_workers=1)
    writing_threads = ThreadPoolExecutor(max_workers=1)

    locations = {}

    def __init__(self, read_dir, write_dir, initial_files, **kwargs):
        self.read_dir = read_dir
        self.write_dir = write_dir
        self.initial_files = initial_files

    def read_file(self, file_name):
        if self.files_read.get(file_name):
            return
        print(f"Reading file {file_name}")
        self.current_read += 1
        self.buffers_filled += 1

        with open(f'{self.read_dir}/{file_name}', 'rb') as file:
            file_content = file.read()
            buf = io.BytesIO()
            buf.write(file_content)
            self.files_read.update({file_name: {'buffer': buf.getbuffer(), 'status': FileStatus.READ}})
            self.read_files += 1

        self.current_read -= 1

    def determine_categories(self, file_name):
        file_info = self.files_read.get(file_name)
        if file_info['status'] != FileStatus.READ:
            return
        print(f'Determine categories on file {file_name}')
        self.current_determine_categories += 1
        file_info['status'] = FileStatus.DETERMINING_CATEGORIES

        np_buffer = np.frombuffer(file_info['buffer'], dtype=np.dtype([('key', 'V2'), ('rest', 'V98')]))
        record_arr = np.sort(np_buffer, order='key')
        file_info['buffer'] = record_arr

        file_info['status'] = FileStatus.DETERMINED_CATEGORIES

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
        self.determined_categories_files += 1
        self.current_determine_categories -= 1

    def write_file(self, file_name):
        file_info = self.files_read.get(file_name)
        if file_info['status'] != FileStatus.DETERMINED_CATEGORIES:
            return
        print(f'Writing file {file_name}')
        self.current_write += 1
        file_info['status'] = FileStatus.WRITING

        with open(f'{self.write_dir}/{file_name}', 'wb') as file:
            file.write(file_info['buffer'])

        file_info['buffer'] = None
        file_info['status'] = FileStatus.WRITTEN
        self.written_files += 1
        self.current_write -= 1
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
                    self.reading_threads.submit(self.read_file, file)
                elif (
                        file_data and
                        file_data['status'] == FileStatus.READ and
                        self.current_determine_categories < self.max_determine_categories
                ):
                    # self.determine_categories(file)
                    self.determine_categories_threads.submit(self.determine_categories, file)
                elif (
                        file_data and
                        file_data['status'] == FileStatus.DETERMINED_CATEGORIES and
                        self.current_write < self.max_write
                ):
                    self.writing_threads.submit(self.write_file, file)

        with open(f'results/locations_{self.initial_files[0]}.json', 'w') as locations_file:
            json.dump(self.locations, locations_file)


class FileStatus(Enum):
    READ = 'READ'
    DETERMINING_CATEGORIES = 'DETERMINING_CATEGORIES'
    DETERMINED_CATEGORIES = 'DETERMINED_CATEGORIES'
    WRITING = 'WRITING'
    WRITTEN = 'WRITTEN'
