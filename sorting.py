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
    if not read_dir:
        return "'read_dir' attribute not found", 400
    if not write_dir:
        return "'write_dir' attribute not found", 400
    if not file_names:
        return "'file_names' attribute not found", 400

    handler = SortingHandlerStage1(
        read_bucket='input-sorting-experiments',
        write_bucket='output-sorting-experiments',
        read_dir=read_dir,
        write_dir=write_dir,
        initial_files=file_names
    )
    Thread(target=handler.execute_stage1).start()
    return "Processing"


@app.route("/sorting/stage2", methods=['POST'])
def sorting_stage2():
    request_data = json.loads(request.data)
    partitions = request_data.get('partitions')
    if not partitions:
        return "'partitions' attribute not found", 400


    handler = SortingHandlerStage2(
        read_bucket='output-sorting-experiments',
        write_bucket='output-sorting-experiments',
        read_dir='10mb-10files-intermediate',
        write_dir='10mb-10files-output',
        partitions=partitions
    )
    Thread(target=handler.execute_stage2).start()
    # handler.execute_stage2()
    return "Sorting"


if __name__ == '__main__':
    app.run()
