import json
from threading import Thread

from flask import Flask, request

from handler_stage_1 import SortingHandlerStage1
from handler_stage_2 import SortingHandlerStage2

import sys

app = Flask(__name__)
minio_ip = ''
SERVER_NUMBER = None

@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route("/sorting/pipeline/stage1", methods=['POST'])
def sorting_stage1():
    request_data = json.loads(request.data)
    # read_dir = request_data.get('read_dir')
    # write_dir = request_data.get('write_dir')
    file_names = request_data.get('file_names')
    experiment_number = request_data.get('experiment_number')
    config = request_data.get('config')
    reading_threads = request_data.get('reading_threads')
    det_cat_threads = request_data.get('det_cat_threads')
    writing_threads = request_data.get('writing_threads')

    # if read_dir is None:
    #     return "'read_dir' attribute not found", 400
    # if write_dir is None:
    #     return "'write_dir' attribute not found", 400
    if file_names is None:
        return "'file_names' attribute not found", 400
    if experiment_number is None:
        return "'experiment_number' attribute not found", 400
    if reading_threads is None:
        return "'reading_threads' attribute not found", 400
    if det_cat_threads is None:
        return "'det_cat_threads' attribute not found", 400
    if writing_threads is None:
        return "'writing_threads' attribute not found", 400
    if config is None:
        return "'config' attribute not found", 400
    else:
        if config.get('file_size') is None:
            return "'file_size' attribute not found for config", 400
        if config.get('nr_files') is None:
            return "'nr_files' attribute not found for config", 400
        if config.get('intervals') is None:
            return "'intervals' attribute not found for config", 400

    handler = SortingHandlerStage1(
        read_bucket='read',
        write_bucket='final',
        intermediate_bucket='intermediate',
        status_bucket='status',
        initial_files=file_names,
        experiment_number=experiment_number,
        config=config,
        minio_ip=minio_ip,
        server_number=SERVER_NUMBER
    )
    # handler.execute_stage1()
    Thread(target=handler.execute_stage1).start()
    return "Processing"


@app.route("/sorting/pipeline/stage2", methods=['POST'])
def sorting_stage2():
    request_data = json.loads(request.data)
    partitions = request_data.get('partitions')
    experiment_number = request_data.get('experiment_number')
    config = request_data.get('config')
    reading_threads = request_data.get('reading_threads')
    sort_threads = request_data.get('sort_threads')
    writing_threads = request_data.get('writing_threads')
    if partitions is None:
        return "'partitions' attribute not found", 400
    if experiment_number is None:
        return "'experiment_number' attribute not found", 400
    if reading_threads is None:
        return "'reading_threads' attribute not found", 400
    if sort_threads is None:
        return "'sort_threads' attribute not found", 400
    if writing_threads is None:
        return "'writing_threads' attribute not found", 400
    if config is None:
        return "'config' attribute not found", 400
    else:
        if config.get('file_size') is None:
            return "'file_size' attribute not found for config", 400
        if config.get('nr_files') is None:
            return "'nr_files' attribute not found for config", 400
        if config.get('intervals') is None:
            return "'intervals' attribute not found for config", 400

    handler = SortingHandlerStage2(
        read_bucket='read',
        write_bucket='final',
        intermediate_bucket='intermediate',
        status_bucket='status',
        partitions=partitions,
        experiment_number=experiment_number,
        config=config,
        minio_ip=minio_ip,
        reading_threads=reading_threads,
        sort_threads=sort_threads,
        writing_threads=writing_threads,
        server_number=SERVER_NUMBER
    )
    Thread(target=handler.execute_stage2).start()
    # handler.execute_stage2()
    return "Sorting"


@app.route("/sorting/no-pipeline/stage1", methods=['POST'])
def sorting_stage1_no_pipeline():
    request_data = json.loads(request.data)
    file_names = request_data.get('file_names')
    experiment_number = request_data.get('experiment_number')
    config = request_data.get('config')

    if file_names is None:
        return "'file_names' attribute not found", 400
    if experiment_number is None:
        return "'experiment_number' attribute not found", 400
    if config is None:
        return "'config' attribute not found", 400
    else:
        if config.get('file_size') is None:
            return "'file_size' attribute not found for config", 400
        if config.get('nr_files') is None:
            return "'nr_files' attribute not found for config", 400
        if config.get('intervals') is None:
            return "'intervals' attribute not found for config", 400

    handler = SortingHandlerStage1(
        read_bucket='read',
        write_bucket='final',
        intermediate_bucket='intermediate',
        status_bucket='status',
        initial_files=file_names,
        experiment_number=experiment_number,
        config=config,
        minio_ip=minio_ip,
        server_number=SERVER_NUMBER
    )
    # handler.execute_stage1()
    Thread(target=handler.execute_stage1_without_pipelining).start()
    return "Processing"


@app.route("/sorting/no-pipeline/stage2", methods=['POST'])
def sorting_stage2_no_pipeline():
    request_data = json.loads(request.data)
    partitions = request_data.get('partitions')
    experiment_number = request_data.get('experiment_number')
    config = request_data.get('config')
    if partitions is None:
        return "'partitions' attribute not found", 400
    if experiment_number is None:
        return "'experiment_number' attribute not found", 400
    if config is None:
        return "'config' attribute not found", 400
    else:
        if config.get('file_size') is None:
            return "'file_size' attribute not found for config", 400
        if config.get('nr_files') is None:
            return "'nr_files' attribute not found for config", 400
        if config.get('intervals') is None:
            return "'intervals' attribute not found for config", 400

    handler = SortingHandlerStage2(
        read_bucket='read',
        write_bucket='final',
        intermediate_bucket='intermediate',
        status_bucket='status',
        partitions=partitions,
        experiment_number=experiment_number,
        config=config,
        minio_ip=minio_ip,
        server_number=SERVER_NUMBER
    )
    Thread(target=handler.execute_stage2_without_pipelining).start()
    return "Sorting"


if __name__ == '__main__':
    # e.g. python sorting.py 127.0.0.1 1
    port = '5000'

    minio_ip = sys.argv[1]
    SERVER_NUMBER = sys.argv[1]

    # if len(sys.argv) == 4:
    #     port = sys.argv[3]
    app.run(host=f'10.149.0.{SERVER_NUMBER}', port=int(port))

    # Local settings
    # SERVER_NUMBER = 1
    # minio_ip = '127.0.0.1'
    # app.run(host='0.0.0.0', port=5000)
