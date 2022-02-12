from custom_logger import get_logger
import uuid
from handler_stage_1 import SortingHandlerStage1
from handler_stage_2 import SortingHandlerStage2


def run_experiment(experiment_number, nr_files, file_size, intervals):
    read_bucket = "read"
    intermediate_bucket = "intermediate"
    write_bucket = "final"
    status_bucket = "status"

    process_uuid = uuid.uuid4()
    logger = get_logger(
        'main_handler',
        'main_handler',
        nr_files,
        file_size,
        intervals
    )
    file_names = [str(i) for i in range(int(nr_files))]

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
        read_bucket=read_bucket,
        intermediate_bucket=intermediate_bucket,
        write_bucket=write_bucket,
        status_bucket=status_bucket,
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
        read_bucket=read_bucket,
        intermediate_bucket=intermediate_bucket,
        write_bucket=write_bucket,
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
    print(f"--------------------------FINISH EXPERIMENT NUMBER {experiment_number}-------------------------------")
    print(f"--------------------------FINISH EXPERIMENT NUMBER {experiment_number}-------------------------------")
    print(f"--------------------------FINISH EXPERIMENT NUMBER {experiment_number}-------------------------------")
    print(f"--------------------------FINISH EXPERIMENT NUMBER {experiment_number}-------------------------------")
    print(f"--------------------------FINISH EXPERIMENT NUMBER {experiment_number}-------------------------------")


if __name__ == '__main__':
    for i in range(1):
        run_experiment(i, '10', '10MB', '256')
