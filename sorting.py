import json
from threading import Thread

from flask import Flask, request

from handler_stage_1 import SortingHandlerStage1
from handler_stage_2 import SortingHandlerStage2
app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route("/sorting/stage1", methods=['POST'])
def sorting_stage1():
    request_data = json.loads(request.data)
    read_dir = request_data.get('read_dir')
    write_dir = request_data.get('write_dir')
    file_names = request_data.get('file_names')
    experiment_number = request_data.get('experiment_number')
    config = request_data.get('config')

    if read_dir is None:
        return "'read_dir' attribute not found", 400
    if write_dir is None:
        return "'write_dir' attribute not found", 400
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
        read_bucket='input-sorting-experiments',
        write_bucket='output-sorting-experiments',
        read_dir=read_dir,
        write_dir=write_dir,
        initial_files=file_names,
        experiment_number=experiment_number,
        config=config,
    )
    Thread(target=handler.execute_stage1).start()
    return "Processing"


@app.route("/sorting/stage2", methods=['POST'])
def sorting_stage2():
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
        read_bucket='output-sorting-experiments',
        write_bucket='output-sorting-experiments',
        read_dir='10mb-10files-intermediate',
        write_dir='10mb-10files-output',
        partitions=partitions,
        experiment_number=experiment_number,
        config=config
    )
    Thread(target=handler.execute_stage2).start()
    # handler.execute_stage2()
    return "Sorting"


if __name__ == '__main__':
    app.run()
