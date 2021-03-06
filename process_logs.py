import glob
import json
import re
from slugify import slugify

stages = [
    'client_handler',
    'stage_1',
    'stage_2',
    # 'threads_test'
]

phrases_main_handler = [
    'Start stage 1.',
    'Finish stage 1.',
    'Start stage 2.',
    'Finish stage 2.',
]

phrases_stage_1 = threads_test = {
    'read_initial_files_tasks': [
        'Started reading file',
        'Finished reading file',
    ],
    'determine_categories_tasks': [
        'Started sorting determine categories',
        'Finished sorting determine categories',
        'Started determine categories',
        'Finished determine categories',
    ],
    'write_first_file_tasks': [
        'Started writing file',
        'Finished writing file',
    ]
}

phrases_stage_2 = {
    'read_categories_tasks': [
        'Started reading category',
        'Finished reading category',
        # 'Started reading partition file ',
        # 'Finished reading partition file ',
        'Started reading partition',
        'Finished reading partition',
        # 'Start updating files_in_read',
        # 'Finish updating files_in_read',
        # 'Started initializing minio client partition',
        # 'Finished initializing minio client partition',
        # 'read_partition Start updating files_read',
        # 'read_partition Finish updating files_read'
    ],
    'sort_tasks': [
        'Started sorting category',
        'Finished sorting category',
        'Started reading buffers category',
        'Finished reading buffers category'
    ],
    'write_partition_tasks': [
        'Started writing category',
        'Finished writing category'
    ]
}

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
    exception_phrases = False
    # if phrase == 'Started reading partition ' or phrase == 'Finished reading partition ':
    #     exception_phrases = True
    #     results = re.search(f'(.*) {stage_name} INFO experiment_number:(.*); uuid:(.*); {phrase}(.*).', line)
    # else:
    results = re.search(f'(.*) {stage_name} INFO experiment_number:(.*); uuid:(.*);', line)

    # rs = results.groups()
    try:
        time = results.group(1)
        experiment_number = results.group(2)
        process_uuid = results.group(3)
        if exception_phrases:
            file_nr = results.group(4)
            phrase += str(file_nr)
        update_formatted_data(experiment_number, stage_name, phrase, process_uuid, time, task_name)
    except:
        print(line)


def update_formatted_data(experiment_number, stage_name, phrase, process_uuid, time, task_name):
    # if phrase == 'Started sorting determine categories' and task_name == 'read_initial_files_tasks':
    #     phrase_name = 'Finished reading file'
    # else:
    phrase_name = phrase
    if formatted_data.get(experiment_number) is None:
        formatted_data.update(
            {
                experiment_number: {
                    stage_name: {
                        task_name: {
                            process_uuid: {
                                slugify(phrase_name, separator='_'): time
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
                            slugify(phrase_name, separator='_'): time
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
                        slugify(phrase_name, separator='_'): time
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
                    slugify(phrase_name, separator='_'): time
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
                slugify(phrase_name, separator='_'): time
            }
        )


def process_logs(
        nr_files,
        file_size,
        intervals,
        pipeline,
        nr_parallel_threads,
        nr_servers,
        nr_reading_proc_stage_1=8,
        nr_det_cat_proc_stage_1=8,
        nr_writing_proc_stage_1=8,
        nr_reading_proc_stage_2=8,
        nr_det_cat_proc_stage_2=8,
        nr_writing_proc_stage_2=8,
):
    # log_files = glob.glob(f'logs_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_{pipeline}_eth4/*.log')
    # log_files = glob.glob(f'logs_determine_bandwidth/logs_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_50_iterations_{nr_parallel_threads}_threads_with_rules_50MB/*.log')
    if pipeline == 'no_pipeline':
        log_files = glob.glob(f'logs_experiments/no-pipeline/{file_size}-{nr_files}files-NO-pipeline-10-experiments-{nr_servers}-servers/logs_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_{pipeline}/*.log')
    elif pipeline == 'with_pipeline':
        # log_files = glob.glob(
        #     f'logs_experiments/'
        #     f'with-pipeline/'
        #     f'{file_size}-{nr_files}files-WITH-pipeline-10-experiments-{nr_servers}-servers-'
        #     f'{nr_reading_proc_stage_1}-{nr_det_cat_proc_stage_1}-{nr_writing_proc_stage_1}-{nr_reading_proc_stage_2}-{nr_det_cat_proc_stage_2}-{nr_writing_proc_stage_2}/'
        #     f'logs_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_with_pipeline/'
        #     f'*.log')
        log_files = glob.glob(
            f'logs_experiments/'
            f'with-pipeline/'
            f'test_nr_servers/'
            f'{nr_reading_proc_stage_1}-{nr_det_cat_proc_stage_1}-{nr_writing_proc_stage_1}-{nr_reading_proc_stage_2}-{nr_det_cat_proc_stage_2}-{nr_writing_proc_stage_2}/'
            f'logs_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_with_pipeline/'
            f'*.log')

    for experiment_log_file in log_files:
        try:
            stage_name = [stage for stage in stages if stage in experiment_log_file][0]
        except:
            print("PROBLEM")
        with open(experiment_log_file, 'r') as log_file:
            for line in log_file.readlines():
                for phrase in phrases_main_handler:
                    if phrase in line:
                        format_line(stage_name, phrase, line, stage_name)

                for task_name, phrase_tasks in phrases_stage_1.items():
                    for phrase in phrase_tasks:
                        if phrase in line:
                            format_line(stage_name, phrase, line, task_name)
                # for task_name, phrase_tasks in threads_test.items():
                #     for phrase in phrase_tasks:
                #         if phrase in line:
                #             format_line(stage_name, phrase, line, task_name)
                for task_name, phrase_tasks in phrases_stage_2.items():
                    for phrase in phrase_tasks:
                        if phrase in line:
                            format_line(stage_name, phrase, line, task_name)
    # file_path = f'logs_determine_bandwidth/logs_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_50_iterations_{nr_parallel_threads}_threads_with_rules_50MB/results_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_{pipeline}.json'
    if pipeline == 'no_pipeline':
        file_path = f'logs_experiments/no-pipeline/{file_size}-{nr_files}files-NO-pipeline-10-experiments-{nr_servers}-servers/logs_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_{pipeline}/results_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_{pipeline}.json'
    elif pipeline == 'with_pipeline':
        # file_path = f'logs_experiments/' \
        #             f'with-pipeline/' \
        #             f'{file_size}-{nr_files}files-with-pipeline-10-experiments-{nr_servers}-servers-' \
        #             f'{nr_reading_proc_stage_1}-{nr_det_cat_proc_stage_1}-{nr_writing_proc_stage_1}-{nr_reading_proc_stage_2}-{nr_det_cat_proc_stage_2}-{nr_writing_proc_stage_2}/' \
        #             f'logs_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_{pipeline}/results_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_{pipeline}.json'
        file_path = f'logs_experiments/' \
                    f'with-pipeline/' \
                    f'test_nr_servers/' \
                    f'{nr_reading_proc_stage_1}-{nr_det_cat_proc_stage_1}-{nr_writing_proc_stage_1}-{nr_reading_proc_stage_2}-{nr_det_cat_proc_stage_2}-{nr_writing_proc_stage_2}/' \
                    f'logs_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_{pipeline}/results_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_{pipeline}.json'

    with open(file_path, 'w+') as results_file:
        json.dump(formatted_data, results_file)


if __name__ == '__main__':
    # process_logs('1000', '100MB', '256', pipeline='pipeline')
    process_logs(
        nr_files='1000',
        file_size='100MB',
        intervals='256',
        pipeline='with_pipeline',
        nr_parallel_threads=24,
        nr_servers=11,
        nr_reading_proc_stage_1=8,
        nr_det_cat_proc_stage_1=8,
        nr_writing_proc_stage_1=8,
        nr_reading_proc_stage_2=18,
        nr_det_cat_proc_stage_2=4,
        nr_writing_proc_stage_2=2,

    )
