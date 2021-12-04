import glob
import io
from concurrent.futures.thread import ThreadPoolExecutor
from time import sleep
import numpy as np

# files_to_read = glob.glob('read_files/*')
files = glob.glob('read_files/*')
files_to_read = {i: {'file': files[i], 'status': 'NOT_READ'} for i in range(len(files))}
files_read = {}
in_processing = []
processed_files = {}


def read_file(file_path):
    with open(file_path, 'rb') as file:
        file_content = file.read()
        buf = io.BytesIO()
        buf.write(file_content)
        files_read.update({file_path: buf.getbuffer()})


def process_file_data(key):
    in_processing.append(key)
    buffer = files_read.get(key)
    np_buffer = np.frombuffer(buffer, dtype=np.dtype([('key', 'V2'), ('rest', 'V98')]))

    record_arr = np.sort(np_buffer, order='key')
    processed_files.update({key: record_arr})


if __name__ == '__main__':
    reading_threads = ThreadPoolExecutor(max_workers=2)
    processing_threads = ThreadPoolExecutor(max_workers=2)

    for file_index, file_info in files_to_read.items():
        reading_threads.submit(read_file, file_info['file'])

    while not files_read:
        print('sleeping')
        sleep(1)

    while len(files_to_read) != len(processed_files):
        for file_index, file_info in files_to_read.items():
            try:
                if not (
                        processed_files.get(file_info['file']) and file_info['file'] in in_processing
                ):
                    print('processing')
                    processing_threads.submit(process_file_data, file_info['file'])
            except Exception as exc:
                print(exc)
    reading_threads.shutdown()
    processing_threads.shutdown()
    print("DONE")
    exit()
