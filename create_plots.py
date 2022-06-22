import os
import re
from datetime import datetime
import collections
import os
from datetime import datetime, timedelta

import pylab
import pandas as pd
import json
import seaborn as sns
import matplotlib.patches as mpatches
import time

import matplotlib.pyplot as plt
import json
import numpy as np


def create_timeline_stage_1_for_1_experiment(start_stage_1_timestamp, stats, dst):
    stats_df = pd.DataFrame(stats)
    total_calls = len(stats_df)

    palette = sns.color_palette("deep", 15)

    fig = pylab.figure(figsize=(10, 6))
    ax = fig.add_subplot(1, 1, 1)

    y = np.arange(total_calls)
    point_size = 10

    fields = [
        ('start stage 1', start_stage_1_timestamp),
        ('start reading file', stats_df.start_reading_file_timestamp - start_stage_1_timestamp),
        ('finish reading file', stats_df.finish_reading_file_timestamp - start_stage_1_timestamp),
        ('start sorting determine categories', stats_df.start_sorting_determine_categories_timestamp - start_stage_1_timestamp),
        ('finish sorting determine categories', stats_df.finish_sorting_determine_categories_timestamp - start_stage_1_timestamp),
        ('start determine categories', stats_df.start_determine_categories_timestamp - start_stage_1_timestamp),
        ('finish determine categories', stats_df.finish_determine_categories_timestamp - start_stage_1_timestamp),
        ('start writing file', stats_df.start_writing_file_timestamp - start_stage_1_timestamp)
    ]

    patches = []
    for f_i, (field_name, val) in enumerate(fields):
        ax.scatter(val, y, c=[palette[f_i]], edgecolor='none', s=point_size, alpha=0.8)
        patches.append(mpatches.Patch(color=palette[f_i], label=field_name))

    ax.set_xlabel('Execution Time (sec)')
    ax.set_ylabel('Function Call')

    legend = pylab.legend(handles=patches, loc='upper right', frameon=True)
    legend.get_frame().set_facecolor('#FFFFFF')

    yplot_step = int(np.max([1, total_calls / 20]))
    y_ticks = np.arange(total_calls // yplot_step + 2) * yplot_step
    ax.set_yticks(y_ticks)
    ax.set_ylim(-0.02 * total_calls, total_calls * 1.02)
    for y in y_ticks:
        ax.axhline(y, c='k', alpha=0.1, linewidth=1)
    max_seconds = np.max(stats_df.end_tstamp - start_stage_1_timestamp) * 1.25
    xplot_step = max(int(max_seconds / 8), 1)
    x_ticks = np.arange(max_seconds // xplot_step + 2) * xplot_step
    ax.set_xlim(0, max_seconds)

    ax.set_xticks(x_ticks)
    for x in x_ticks:
        ax.axvline(x, c='k', alpha=0.2, linewidth=0.8)

    ax.grid(False)
    fig.tight_layout()

    if dst is None:
        os.makedirs('plots', exist_ok=True)
        dst = os.path.join(os.getcwd(), 'plots', '{}_{}'.format(int(time.time()), 'timeline.png'))
    else:
        dst = os.path.expanduser(dst) if '~' in dst else dst
        dst = '{}_{}'.format(os.path.realpath(dst), 'timeline.png')

    fig.savefig(dst)


def parse_results_partitions(experiments_data, stage_name, task_name, nr_files):
    tasks_timestamps_per_experiment = {}
    tasks_timestamps_total = []
    for experiment_number, experiment_data in experiments_data.items():
        for task, task_data in experiment_data[stage_name][task_name].items():
            partition_nr = '0'
            for key in task_data.keys():
                if 'started_reading_partition' in key:
                    result = re.search('started_reading_partition(.*)from_file_(.*)', key)
                    partition_nr = result.group(1)
                    break
            for file_nr in range(nr_files):
                # TODO: determine partition nr because each task handles a partition and is not a for loop
                    start = datetime.strptime(
                        task_data[f'started_reading_partition{partition_nr}from_file_{file_nr}'],
                        '%Y-%m-%d %H:%M:%S,%f'
                    )
                    finish = datetime.strptime(
                        task_data[f'finished_reading_partition{partition_nr}from_file_{file_nr}'],
                        '%Y-%m-%d %H:%M:%S,%f'
                    )
                    task_completion_time = finish - start
                    task_completion_time_in_seconds = task_completion_time.total_seconds()
                    tasks_timestamps_total.append(task_completion_time_in_seconds)
                    if tasks_timestamps_per_experiment.get(experiment_number) is None:
                        tasks_timestamps_per_experiment.update(
                            {
                                experiment_number: [
                                    {
                                        'completion_time': task_completion_time_in_seconds,
                                        'start_time': start,
                                        'finish_time': finish
                                    }
                                ]
                            }
                        )
                    else:
                        tasks_timestamps_per_experiment[experiment_number].append(
                            {
                                'completion_time': task_completion_time_in_seconds,
                                'start_time': start,
                                'finish_time': finish
                            }
                        )

    return {
        'tasks_timestamps_per_experiment': tasks_timestamps_per_experiment,
        'tasks_timestamps_total': tasks_timestamps_total
    }


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
                            experiment_number: [
                                {
                                    'completion_time': task_completion_time_in_seconds,
                                    'start_time': start,
                                    'finish_time': finish
                                }
                            ]
                        }
                    )
                else:
                    tasks_timestamps_per_experiment[experiment_number].append(
                        {
                            'completion_time': task_completion_time_in_seconds,
                            'start_time': start,
                            'finish_time': finish
                        }
                    )
            except:
                pass

    return {
        'tasks_timestamps_per_experiment': tasks_timestamps_per_experiment,
        'tasks_timestamps_total': tasks_timestamps_total
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


def create_plots(nr_files, file_size, intervals, pipeline, nr_experiments, nr_servers, skip_stage_1=False, skip_stage_2=False, skip_client_handler=True):
    if not pipeline:
        dir_name = f'logs_experiments/' \
                   f'no-pipeline/' \
                   f'{file_size}-{nr_files}files-NO-pipeline-{nr_experiments}-experiments-{nr_servers}-servers/' \
                   f'logs_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_no_pipeline/'
        json_file_path = dir_name + f'results_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_no_pipeline.json'
    else:
        dir_name = f'logs_experiments/' \
                         f'with-pipeline/' \
                         f'{file_size}-{nr_files}files-WITH-pipeline-{nr_experiments}-experiments-{nr_servers}-servers/' \
                         f'logs_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_with_pipeline/'
        json_file_path = dir_name + f'results_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_with_pipeline.json'

    with open(json_file_path, 'r') as results_file:
        experiments_data = json.loads(results_file.read())

    if not skip_client_handler:
        # CLIENT HANDLER
        parsed_results_stage_1_duration_tasks = parse_results(
            experiments_data,
            'client_handler',
            'client_handler',
            'start_stage_1',
            'finish_stage_1',
        )

        stage_1_duration_tasks_timestamps_total = parsed_results_stage_1_duration_tasks['tasks_timestamps_total']
        generate_ecdf(
            stage_1_duration_tasks_timestamps_total,
            dir_name,
            'ecdf_stage_1_duration_all_experiments.png',
            xlabel='Stage 1 duration (s)',
            ylabel='CDF'
        )

        parsed_results_stage_2_duration_tasks = parse_results(
            experiments_data,
            'client_handler',
            'client_handler',
            'start_stage_2',
            'finish_stage_2',
        )

        stage_2_duration_tasks_timestamps_total = parsed_results_stage_2_duration_tasks['tasks_timestamps_total']
        generate_ecdf(
            stage_2_duration_tasks_timestamps_total,
            dir_name,
            'ecdf_stage_2_duration_all_experiments.png',
            xlabel='Stage 2 duration (s)',
            ylabel='CDF'
        )
    # return
    # READ INITIAL FILES
    if not skip_stage_1:
        parsed_results_read_initial_files_tasks = parse_results(
            experiments_data,
            'stage_1',
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
        if pipeline:
            parsed_results_init_minio_client_tasks = parse_results(
                experiments_data,
                'stage_1',
                'read_initial_files_tasks',
                'started_initializing_minio_client_file_name',
                'finished_initializing_minio_client_file_name',
            )
            # read_initial_file_tasks_timestamps_per_experiment = parsed_results_read_initial_files_tasks['tasks_timestamps_per_experiment']
            init_minio_client_tasks_timestamps_total = parsed_results_init_minio_client_tasks['tasks_timestamps_total']
            generate_ecdf(
                init_minio_client_tasks_timestamps_total,
                dir_name,
                'ecdf_read_initial_file_initialize_minio_all_experiments.png',
                xlabel='Read initial files duration (s)',
                ylabel='CDF'
            )

            parsed_results_update_files_read_tasks = parse_results(
                experiments_data,
                'stage_1',
                'read_initial_files_tasks',
                'read_file_start_updating_files_read',
                'read_file_finish_updating_files_read',
            )
            # read_initial_file_tasks_timestamps_per_experiment = parsed_results_read_initial_files_tasks['tasks_timestamps_per_experiment']
            update_files_read_tasks_timestamps_total = parsed_results_update_files_read_tasks['tasks_timestamps_total']
            generate_ecdf(
                update_files_read_tasks_timestamps_total,
                dir_name,
                'ecdf_update_files_read_all_experiments.png',
                xlabel='Read initial files duration (s)',
                ylabel='CDF'
            )

        # SORT DETERMINE CATEGORIES
        parsed_results_sort_det_cat = parse_results(
            experiments_data,
            'stage_1',
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
        parsed_results_sort_det_cat = parse_results(
            experiments_data,
            'stage_1',
            'determine_categories_tasks',
            'started_determine_categories',
            'finished_determine_categories'
        )
        sort_det_cat_timestamps_total = parsed_results_sort_det_cat['tasks_timestamps_total']
        generate_ecdf(
            sort_det_cat_timestamps_total,
            dir_name,
            'ecdf_determine_categories_all_experiments.png',
            xlabel='Determine categories duration (s)',
            ylabel='CDF'
        )

        # Write initial files
        parsed_results_write_first_file_tasks = parse_results(
            experiments_data,
            'stage_1',
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
    # return
    if skip_stage_2:
        return
    ###################### STAGE 2 ########################

    # READ PARTITIONS
    ## ENTIRE CATEGORY - ALL PARTITIONS FROM ALL INTERMEDIATE FILES

    parsed_read_partitions_tasks = parse_results_partitions(
        experiments_data=experiments_data,
        stage_name='stage_2',
        task_name='read_categories_tasks',
        nr_files=int(nr_files),
    )
    read_partitions_tasks_timestamps_total = parsed_read_partitions_tasks['tasks_timestamps_total']
    generate_ecdf(
        read_partitions_tasks_timestamps_total,
        dir_name,
        'ecdf_read_partitions_stage_2_all_experiments.png',
        xlabel='Read partitions duration (s)',
        ylabel='CDF'
    )
    if not pipeline:
        parsed_read_category_tasks = parse_results(
            experiments_data,
            'stage_2',
            'read_categories_tasks',
            'started_reading_category',
            'finished_reading_category'
        )
        read_category_tasks_timestamps_total = parsed_read_category_tasks['tasks_timestamps_total']
        generate_ecdf(
            read_category_tasks_timestamps_total,
            dir_name,
            'ecdf_read_category_stage_2_all_experiments.png',
            xlabel='Read partitions duration (s)',
            ylabel='CDF'
        )

    ## READ EACH PARTITION OF ALL CATEGORIES
    if not pipeline:
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
        'started_sorting_category',
        'finished_sorting_category'
    )
    sort_categories_tasks_timestamps_total = parsed_sort_categories_tasks['tasks_timestamps_total']
    generate_ecdf(
        sort_categories_tasks_timestamps_total,
        dir_name,
        'ecdf_sort_categories_stage_2_all_experiments.png',
        xlabel='Sort categories duration (s)',
        ylabel='CDF'
    )
    if pipeline:
    # Reading buffers

        parsed_reading_buffers_categories_tasks = parse_results(
            experiments_data,
            'stage_2',
            'sort_tasks',
            'started_reading_buffers_category',
            'finished_reading_buffers_category'
        )
        reading_buffers_tasks_timestamps_total = parsed_reading_buffers_categories_tasks['tasks_timestamps_total']
        generate_ecdf(
            reading_buffers_tasks_timestamps_total,
            dir_name,
            'ecdf_reading_buffers_categories_stage_2_all_experiments.png',
            xlabel='Read buffers duration (s)',
            ylabel='CDF'
        )

    # WRITE CATEGORIES

    parsed_write_categories_tasks = parse_results(
        experiments_data,
        'stage_2',
        'write_partition_tasks',
        'started_writing_category',
        'finished_writing_category'
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
    create_plots(
        nr_files='100',
        file_size='1GB',
        intervals='256',
        pipeline=False,
        nr_experiments=10,
        nr_servers=11,
        skip_stage_1=True,
        skip_stage_2=False,
        skip_client_handler=True
    )
    # create_plots('10', '100MB', '256', 'pipeline')
