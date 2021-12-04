import glob
import io
import threading
from concurrent.futures.thread import ThreadPoolExecutor
from time import sleep
import numpy as np


files = glob.glob('read_files/*')
files_to_read = {i: {'file': files[i], 'status': 'NOT_READ'} for i in range(len(files))}
files_read = {}
in_processing = []
processed_files = {}
written_files = []
reading_threads = ThreadPoolExecutor(max_workers=2)
processing_threads = ThreadPoolExecutor(max_workers=2)
writing_threads = ThreadPoolExecutor(max_workers=2)


def read_file(file_path):
    with open(file_path, 'rb') as file:
        print("READING")
        file_content = file.read()
        buf = io.BytesIO()
        buf.write(file_content)
        files_read.update({file_path: buf.getbuffer()})
        thread = threading.Thread(target=process_file_data, args=(file_path, buf.getbuffer(),))
        thread.start()
        # processing_threads.submit(process_file_data, file_path, buf.getbuffer())


def process_file_data(file_path, data_to_process):
    print("Processing")
    buffer = data_to_process
    np_buffer = np.frombuffer(buffer, dtype=np.dtype([('key', 'V2'), ('rest', 'V98')]))

    record_arr = np.sort(np_buffer, order='key')
    processed_files.update({file_path: record_arr})
    thread = threading.Thread(target=write_file, args=(file_path, record_arr,))
    thread.start()


def write_file(file_path, data):
    file_name = file_path.split('/')[1]
    with open(f'write_files/{file_name}', 'wb') as file:
        file.write(data)
    written_files.append(file_name)
    print(f"data written for file {file_name}")


if __name__ == '__main__':
    for file_index, file_info in files_to_read.items():
        reading_threads.submit(read_file, file_info['file'])

    while len(written_files) != len(files_to_read):
        print('sleeping')
        sleep(1)
