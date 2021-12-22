import os
from datetime import datetime

import matplotlib.pyplot as plt
import json
import numpy as np


read_initial_file_tasks_timestamps_per_experiment = {}
read_initial_file_tasks_timestamps_total = []
determine_categories_tasks_timestamps_per_experiment = {}
determine_categories_tasks_timestamps_total = []
write_first_file_tasks_timestamps_per_experiment = {}
write_first_file_tasks_timestamps_total = []
read_partitions_tasks_timestamps_per_experiment = {}
read_partitions_tasks_timestamps_total = []
sort_tasks_timestamps_per_experiment = {}
sort_tasks_timestamps_total = []
write_partition_tasks_timestamps_per_experiment = {}
write_partition_tasks_timestamps_total = []


def parse_results(experiments_data, stage_name, task_name, start, finish):
    tasks_timestamps_per_experiment = {}
    tasks_timestamps_total = []
    for experiment_number, experiment_data in experiments_data.items():
        for task, task_data in experiment_data[stage_name][task_name].items():
            finish = datetime.strptime(task_data[finish], '%Y-%m-%d %H:%M:%S,%f')
            start = datetime.strptime(task_data[start], '%Y-%m-%d %H:%M:%S,%f')
            task_completion_time = finish - start
            task_completion_time_in_seconds = task_completion_time.total_seconds()
            tasks_timestamps_total.append(task_completion_time_in_seconds)
            if tasks_timestamps_per_experiment.get(experiment_number) is None:
                tasks_timestamps_per_experiment.update(
                    {
                        experiment_number: [task_completion_time_in_seconds]
                    }
                )
            else:
                tasks_timestamps_per_experiment[experiment_number].append(
                    task_completion_time_in_seconds
                )

    return {
        'tasks_timestamps_per_experiment': tasks_timestamps_per_experiment,
        'tasks_timestamps_total': tasks_timestamps_total
    }


def generate_ecdf(data, dir_name, file_name):
    x = np.sort(data)
    n = x.size
    y = np.arange(1, n+1)/n
    plt.scatter(x=x, y=y)
    plt.xlabel('x', fontsize=16)
    plt.ylabel('y', fontsize=16)
    if not os.path.exists(f'plots/{dir_name}'):
        os.makedirs(dir_name)

    plt.savefig(f'/plots/{dir_name}/{file_name}')
    plt.clf()


def create_plots(nr_files, file_size, intervals):
    with open(f'results/results_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}.json', 'r') as results_file:
        experiments_data = json.loads(results_file.read())

    parsed_results_read_initial_files_tasks = parse_results(
        experiments_data,
        'stage_1',
        'read_initial_files_tasks',
        'finished_reading_file',
        'started_reading_file'
    )
    read_initial_file_tasks_timestamps_per_experiment = parsed_results_read_initial_files_tasks['tasks_timestamps_per_experiment']
    read_initial_file_tasks_timestamps_total = parsed_results_read_initial_files_tasks['tasks_timestamps_total']

    parsed_results_write_first_file_tasks = parse_results(
        experiments_data,
        'stage_1',
        'write_first_file_tasks',
        'started_writing_file',
        'finished_writing_file'
    )
    write_partition_tasks_timestamps_per_experiment = parsed_results_write_first_file_tasks['tasks_timestamps_per_experiment']
    write_partition_tasks_timestamps_total = parsed_results_write_first_file_tasks['tasks_timestamps_total']

    parsed_read_partitions_tasks = parse_results(
        experiments_data,
        'stage_2',
        'read_partitions_tasks',
        'started_reading_partition',
        'finished_reading_partition'
    )
    read_partitions_tasks_timestamps_per_experiment = parsed_read_partitions_tasks['tasks_timestamps_per_experiment']
    read_partitions_tasks_timestamps_total = parsed_read_partitions_tasks['tasks_timestamps_total']

    parsed_write_partitions_tasks = parse_results(
        experiments_data,
        'stage_2',
        'write_partitions_tasks',
        'started_writing_partition',
        'finished_writing_partition'
    )

    generate_ecdf(
        read_initial_file_tasks_timestamps_total,
        f'logs_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}',
        'ecdf_read_initial_file_all_experiments.png'
    )


if __name__ == '__main__':
    create_plots('10', '10MB', '256')
