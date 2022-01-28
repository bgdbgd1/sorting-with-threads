from custom_logger import get_logger
import uuid
import json
from handler_stage_1 import SortingHandlerStage1
from handler_stage_2 import SortingHandlerStage2


def run_experiment(experiment_number, nr_files, file_size, intervals):
    process_uuid = uuid.uuid4()
    results_bucket = 'output-sorting-experiments'
    prefix_results_stage_1 = f'results_stage1/experiment_{experiment_number}_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}/results_stage1_'
    logger = get_logger(
        'main_handler',
        'main_handler',
        nr_files,
        file_size,
        intervals
    )
    file_names = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
    read_dir = f"read_files_generated/{file_size}MB-{nr_files}files"
    intermediate_dir = "intermediate_files"
    write_dir = "write_files"
    config = {
                "file_size": file_size,
                'nr_files': nr_files,
                'intervals': intervals
            }
    logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Start stage 1.')

    stage_1_handler = SortingHandlerStage1(
        read_bucket='input-sorting-experiments',
        write_bucket='output-sorting-experiments',
        read_dir=read_dir,
        write_dir=intermediate_dir,
        initial_files=file_names,
        experiment_number=experiment_number,
        config=config,
    )

    data_from_stage_1 = stage_1_handler.execute_stage1()

    logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finish stage 1.')

    data_for_stage_2 = {}
    for file, file_data in data_from_stage_1.items():
        for file_partition, positions in file_data.items():
            if data_for_stage_2.get(file_partition) is None:
                data_for_stage_2.update(
                    {
                        file_partition: [positions]
                    }
                )
            else:
                data_for_stage_2[file_partition].append(positions)

    stage_2_handler = SortingHandlerStage2(
        read_bucket='output-sorting-experiments',
        write_bucket='output-sorting-experiments',
        read_dir=intermediate_dir,
        write_dir=write_dir,
        partitions=data_for_stage_2,
        experiment_number=experiment_number,
        config=config
    )
    logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Start stage 2.')

    stage_2_handler.execute_stage2()
    logger.info(f'experiment_number:{experiment_number}; uuid:{process_uuid}; Finish stage 2.')
    logger.handlers.pop()
    logger.handlers.pop()


if __name__ == '__main__':
    for i in range(1):
        run_experiment(i, '10', '10MB', '256')
