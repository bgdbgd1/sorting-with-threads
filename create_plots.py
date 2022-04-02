import os
from datetime import datetime

import matplotlib.pyplot as plt
import json
import numpy as np


def parse_results(experiments_data, stage_name, task_name, start_attribute, finish_attribute):
    tasks_timestamps_per_experiment = {}
    tasks_timestamps_total = []
    for experiment_number, experiment_data in experiments_data.items():
        for task, task_data in experiment_data[stage_name][task_name].items():
            try:
                finish = datetime.strptime(task_data[finish_attribute], '%Y-%m-%d %H:%M:%S,%f')
                start = datetime.strptime(task_data[start_attribute], '%Y-%m-%d %H:%M:%S,%f')
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
            except:
                pass

    return {
        'tasks_timestamps_per_experiment': tasks_timestamps_per_experiment,
        'tasks_timestamps_total': sorted(tasks_timestamps_total)
    }


def generate_ecdf(data, dir_name, file_name, xlabel, ylabel):
    x = np.sort(data)
    n = x.size
    y = np.arange(1, n+1)/n
    plt.scatter(x=x, y=y)
    plt.xlabel(xlabel, fontsize=16)
    plt.ylabel(ylabel, fontsize=16)
    # if not os.path.exists(f'{dir_name}/plots/{dir_name}'):
    #     os.makedirs(f'plots/{dir_name}')

    plt.savefig(f'{dir_name}/{file_name}')
    plt.clf()


def create_plots(nr_files, file_size, intervals, pipeline, nr_threads):
    # dir_name =f'logs_determine_bandwidth/logs_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_50_iterations_{nr_threads}_threads_with_rules_50MB_with_distributed_minio'
    dir_name = 'logs_determine_bandwidth/logs_threads_test/8_threads_1GB'
    with open(f'{dir_name}/results_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_{pipeline}.json', 'r') as results_file:
        experiments_data = json.loads(results_file.read())

    # READ INITIAL FILES
    parsed_results_read_initial_files_tasks = parse_results(
        experiments_data,
        'threads_test',
        'read_initial_files_tasks',
        'started_reading_file',
        'finished_reading_file',
    )
    # read_initial_file_tasks_timestamps_per_experiment = parsed_results_read_initial_files_tasks['tasks_timestamps_per_experiment']
    read_initial_file_tasks_timestamps_total = parsed_results_read_initial_files_tasks['tasks_timestamps_total']
    generate_ecdf(
        read_initial_file_tasks_timestamps_total,
        dir_name,
        'ecdf_read_initial_file_all_experiments.png',
        xlabel='Read initial files duration (s)',
        ylabel='CDF'
    )

    # SORT DETERMINE CATEGORIES
    parsed_results_sort_det_cat = parse_results(
        experiments_data,
        'threads_test',
        'determine_categories_tasks',
        'started_sorting_determine_categories',
        'finished_sorting_determine_categories'
    )
    sort_det_cat_timestamps_total = parsed_results_sort_det_cat['tasks_timestamps_total']
    generate_ecdf(
        sort_det_cat_timestamps_total,
        dir_name,
        'ecdf_sort_det_cat_all_experiments.png',
        xlabel='Sort initial data by the first two bytes (s)',
        ylabel='CDF'
    )

    # Determine Categories
    # parsed_results_sort_det_cat = parse_results(
    #     experiments_data,
    #     'threads_test',
    #     'determine_categories_tasks',
    #     'started_determine_categories',
    #     'finished_determine_categories'
    # )
    # sort_det_cat_timestamps_total = parsed_results_sort_det_cat['tasks_timestamps_total']
    # generate_ecdf(
    #     sort_det_cat_timestamps_total,
    #     dir_name,
    #     'ecdf_determine_categories_all_experiments.png',
    #     xlabel='Determine categories duration (s)',
    #     ylabel='CDF'
    # )

    # Write initial files
    parsed_results_write_first_file_tasks = parse_results(
        experiments_data,
        'threads_test',
        'write_first_file_tasks',
        'started_writing_file',
        'finished_writing_file'
    )
    write_first_file_tasks_timestamps_total = parsed_results_write_first_file_tasks['tasks_timestamps_total']
    generate_ecdf(
        write_first_file_tasks_timestamps_total,
        dir_name,
        'ecdf_write_first_file_all_experiments.png',
        xlabel='Write files to storage duration (s)',
        ylabel='CDF'
    )
    return

    ###################### STAGE 2 ########################

    # READ PARTITIONS
    ## ENTIRE CATEGORY - ALL PARTITIONS FROM ALL INTERMEDIATE FILES

    parsed_read_categories_tasks = parse_results(
        experiments_data,
        'stage_2',
        'read_categories_tasks',
        'started_reading_category',
        'finished_reading_category'
    )
    read_categories_tasks_timestamps_total = parsed_read_categories_tasks['tasks_timestamps_total']
    generate_ecdf(
        read_categories_tasks_timestamps_total,
        dir_name,
        'ecdf_read_categories_stage_2_all_experiments.png',
        xlabel='Read categories duration (s)',
        ylabel='CDF'
    )

    ## READ EACH PARTITION OF ALL CATEGORIES

    read_partition_tasks_timestamps_total = []
    for experiment_number, experiment_data in experiments_data.items():
        for task_name, task_data in experiment_data['stage_2']['read_categories_tasks'].items():
            if 'started_reading_partition_file_' in task_name or 'finished_reading_partition_file_' in task_name:
                is_start = False
                try:
                    file_nr = task_name.split('started_reading_partition_file_')[0]
                    is_start = True
                except:
                    file_nr = task_name.split('finished_reading_partition_file_')[0]

                if is_start:
                    start = datetime.strptime(task_data[task_name], '%Y-%m-%d %H:%M:%S,%f')
                    finish = datetime.strptime(task_data[f'finished_reading_partition_file_{file_nr}'], '%Y-%m-%d %H:%M:%S,%f')
                else:
                    start = datetime.strptime(task_data[f'started_reading_partition_file_{file_nr}'], '%Y-%m-%d %H:%M:%S,%f')
                    finish = datetime.strptime(task_data[task_name], '%Y-%m-%d %H:%M:%S,%f')

                task_completion_time = finish - start
                task_completion_time_in_seconds = task_completion_time.total_seconds()
                read_partition_tasks_timestamps_total.append(task_completion_time_in_seconds)

    # SORT CATEGORIES

    parsed_sort_categories_tasks = parse_results(
        experiments_data,
        'stage_2',
        'sort_tasks',
        'started_sorting_partition',
        'finished_sorting_partition'
    )
    sort_categories_tasks_timestamps_total = parsed_sort_categories_tasks['tasks_timestamps_total']
    generate_ecdf(
        sort_categories_tasks_timestamps_total,
        dir_name,
        'ecdf_sort_categories_stage_2_all_experiments.png',
        xlabel='Sort categories duration (s)',
        ylabel='CDF'
    )

    # WRITE CATEGORIES

    parsed_write_categories_tasks = parse_results(
        experiments_data,
        'stage_2',
        'write_partitions_tasks',
        'started_writing_partition',
        'finished_writing_partition'
    )
    write_categories_tasks_timestamps_total = parsed_write_categories_tasks['tasks_timestamps_total']
    generate_ecdf(
        write_categories_tasks_timestamps_total,
        dir_name,
        'ecdf_write_categories_all_experiments.png',
        xlabel='Write categories duration (s)',
        ylabel='CDF'
    )


if __name__ == '__main__':
    create_plots('50', '100MB', '256', 'no_pipeline', 4)
    # create_plots('10', '100MB', '256', 'pipeline')
