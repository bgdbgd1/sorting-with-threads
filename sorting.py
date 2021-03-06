import json
import os
import sys


from flask import Flask, request

from stage_1_no_pipeline import execute_stage_1_no_pipeline
from stage_2_no_pipeline import execute_stage_2_no_pipeline
# from stage_1_pipeline_shared_manager import execute_stage_1_pipeline
# from stage_2_pipeline import execute_stage_2_pipeline
from stage_1_pipeline_disk_files import execute_stage_1_pipeline
# from stage_1_pipeline_queues import execute_stage_1_pipeline
from stage_2_pipeline_disk_files import execute_stage_2_pipeline

from constants import SERVER_NUMBER, PREFIX_REMOTE, PREFIX_LOCAL
app = Flask(__name__)
minio_ip = ''


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route("/sorting/pipeline/stage1", methods=['POST'])
def sorting_stage1():
    request_data = json.loads(request.data)
    file_names = request_data.get('file_names')
    experiment_number = request_data.get('experiment_number')
    config = request_data.get('config')
    reading_threads = request_data.get('reading_threads')
    det_cat_threads = request_data.get('det_cat_threads')
    writing_threads = request_data.get('writing_threads')

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

    execute_stage_1_pipeline(
        file_names,
        minio_ip,
        'read',
        'intermediate',
        'status',
        config['nr_files'],
        config['file_size'],
        config['intervals'],
        SERVER_NUMBER,
        experiment_number,
        reading_threads,
        det_cat_threads,
        writing_threads,
    )
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
    execute_stage_2_pipeline(
        partitions,
        minio_ip,
        'intermediate',
        'final',
        'status',
        config['nr_files'],
        config['file_size'],
        config['intervals'],
        SERVER_NUMBER,
        experiment_number,
        reading_threads,
        sort_threads,
        writing_threads
    )
    return "Sorting"


@app.route("/sorting/no-pipeline/stage1", methods=['POST'])
def sorting_stage1_no_pipeline():
    request_data = json.loads(request.data)
    file_names = request_data.get('file_names')
    experiment_number = request_data.get('experiment_number')
    config = request_data.get('config')
    no_pipeline_threads = request_data.get('no_pipeline_threads')
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
    execute_stage_1_no_pipeline(
        file_names,
        minio_ip,
        'read',
        'intermediate',
        'status',
        config['nr_files'],
        config['file_size'],
        config['intervals'],
        SERVER_NUMBER,
        experiment_number,
        no_pipeline_threads,
    )
    return "Processing"


@app.route("/sorting/no-pipeline/stage2", methods=['POST'])
def sorting_stage2_no_pipeline():
    request_data = json.loads(request.data)
    partitions = request_data.get('partitions')
    experiment_number = request_data.get('experiment_number')
    config = request_data.get('config')
    no_pipeline_threads = request_data.get('no_pipeline_threads')

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
    execute_stage_2_no_pipeline(
        partitions,
        minio_ip,
        'intermediate',
        'final',
        'status',
        config['nr_files'],
        config['file_size'],
        config['intervals'],
        SERVER_NUMBER,
        experiment_number,
        no_pipeline_threads,
    )
    return "Sorting"


def create_local_storage_dirs(run_local):
    if run_local:
        PREFIX = PREFIX_LOCAL
    else:
        PREFIX = PREFIX_REMOTE

    stages = [f'{PREFIX}stage_1', f'{PREFIX}stage_2']
    for stage in stages:
        if not os.path.isdir(stage):
            os.mkdir(stage)
        if not os.path.isdir(f'{stage}/server_{SERVER_NUMBER}'):
            os.mkdir(f'{stage}/server_{SERVER_NUMBER}')

        if not os.path.isdir(f'{stage}/server_{SERVER_NUMBER}/read'):
            os.mkdir(f'{stage}/server_{SERVER_NUMBER}/read')
        if not os.path.isdir(f'{stage}/server_{SERVER_NUMBER}/read_finished'):
            os.mkdir(f'{stage}/server_{SERVER_NUMBER}/read_finished')

        if not os.path.isdir(f'{stage}/server_{SERVER_NUMBER}/sorted'):
            os.mkdir(f'{stage}/server_{SERVER_NUMBER}/sorted')
        if not os.path.isdir(f'{stage}/server_{SERVER_NUMBER}/sorted_finished'):
            os.mkdir(f'{stage}/server_{SERVER_NUMBER}/sorted_finished')

        if not os.path.isdir(f'{stage}/server_{SERVER_NUMBER}/written'):
            os.mkdir(f'{stage}/server_{SERVER_NUMBER}/written')


if __name__ == '__main__':
    run_local = False
    create_local_storage_dirs(run_local)

    # e.g. python sorting.py 127.0.0.1 1
    if not run_local:
        port = '5000'
        minio_ip = f'10.149.0.{sys.argv[1]}'
        serv_nr = sys.argv[2]

        if len(sys.argv) == 4:
            port = sys.argv[3]
        app.run(host=f'10.149.0.{serv_nr}', port=int(port))
    else:
        # Local settings
        minio_ip = '127.0.0.1'
        app.run(host='0.0.0.0', port=5000)

