import glob
import json
import re
from slugify import slugify

stages = [
    'main_handler',
    'stage_1',
    'stage_2'
]

phrases_main_handler = [
    'Start stage 1.',
    'Finish stage 1.',
    'Start stage 2.',
    'Finish stage 2.',
]

phrases_stage_1 = {
    'read_initial_files_tasks': [
        'Started reading file',
        'Finished reading file'
    ],
    'determine_categories_tasks': [
        'Started sorting determine categories',
        'Finished sorting determine categories',
        'Started determine categories',
        'Finished determine categories'
    ],
    'write_first_file_tasks': [
        'Started writing file',
        'Finished writing file'
    ]
}

phrases_stage_2 = {
    'read_partitions_tasks': [
        'Started reading partition',
        'Finished reading partition'
    ],
    'sort_tasks': [
        'Started sorting partition',
        'Finished sorting partition'
    ],
    'write_partition_tasks': [
        'Started writing partition',
        'Finished writing partition'
    ]
}

nr_errored_lines = 1
# results_data data structure
# {
#     'experiment_{experiment_number}': {
#         'stage_name': {
#             'task_name': {
#               'function_uuid': {
#                   'slugify_phrase': 'timestamp',
#               }
#             }
#         }
#     }
# }
formatted_data = {}


def format_line(stage_name, phrase, line, task_name):
    results = re.search(f'(.*) {stage_name} INFO experiment_number:(.*); uuid:(.*);', line)
    # rs = results.groups()
    try:
        time = results.group(1)
        experiment_number = results.group(2)
        process_uuid = results.group(3)
        update_formatted_data(experiment_number, stage_name, phrase, process_uuid, time, task_name)
    except:
        # print(nr_errored_lines)
        print(line)
        # nr_errored_lines += 1


def update_formatted_data(experiment_number, stage_name, phrase, process_uuid, time, task_name):
    if formatted_data.get(experiment_number) is None:
        formatted_data.update(
            {
                experiment_number: {
                    stage_name: {
                        task_name: {
                            process_uuid: {
                                slugify(phrase, separator='_'): time
                            }
                        }
                    }
                }
            }
        )
    elif (
            formatted_data.get(experiment_number) is not None and
            formatted_data[experiment_number].get(stage_name) is None
    ):
        formatted_data[experiment_number].update(
            {
                stage_name: {
                    task_name: {
                        process_uuid: {
                            slugify(phrase, separator='_'): time
                        }
                    }
                }
            }
        )
    elif (
            formatted_data.get(experiment_number) is not None and
            formatted_data[experiment_number].get(stage_name) is not None and
            formatted_data[experiment_number][stage_name].get(task_name) is None
    ):
        formatted_data[experiment_number][stage_name].update(
            {
                task_name: {
                    process_uuid: {
                        slugify(phrase, separator='_'): time
                    }
                }
            }
        )
    elif (
            formatted_data.get(experiment_number) is not None and
            formatted_data[experiment_number].get(stage_name) is not None and
            formatted_data[experiment_number][stage_name].get(task_name) is not None and
            formatted_data[experiment_number][stage_name][task_name].get(process_uuid) is None
    ):
        formatted_data[experiment_number][stage_name][task_name].update(
            {
                process_uuid: {
                    slugify(phrase, separator='_'): time
                }
            }
        )
    elif (
            formatted_data.get(experiment_number) is not None and
            formatted_data[experiment_number].get(stage_name) is not None and
            formatted_data[experiment_number][stage_name].get(task_name) is not None and
            formatted_data[experiment_number][stage_name][task_name].get(process_uuid) is not None
    ):
        formatted_data[experiment_number][stage_name][task_name][process_uuid].update(
            {
                slugify(phrase, separator='_'): time
            }
        )


def process_logs(nr_files, file_size, intervals):
    log_files = glob.glob(f'logs_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_no_pipeline/*.log')
    for experiment_log_file in log_files:
        stage_name = [stage for stage in stages if stage in experiment_log_file][0]
        with open(experiment_log_file, 'r') as log_file:
            for line in log_file.readlines():
                for phrase in phrases_main_handler:
                    if phrase in line:
                        format_line(stage_name, phrase, line, stage_name)

                for task_name, phrase_tasks in phrases_stage_1.items():
                    for phrase in phrase_tasks:
                        if phrase in line:
                            format_line(stage_name, phrase, line, task_name)

                for task_name, phrase_tasks in phrases_stage_2.items():
                    for phrase in phrase_tasks:
                        if phrase in line:
                            format_line(stage_name, phrase, line, task_name)
    with open(f'results/results_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}.json', 'w+') as results_file:
        json.dump(formatted_data, results_file)


if __name__ == '__main__':
    process_logs('1000', '100MB', '256')
