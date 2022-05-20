import logging
import sys

file_handler = None
console_handler = None


def get_logger(logger_name, process_name, nr_files, file_size, intervals, server_number, with_pipeline):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    log_format = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)
    file_handler = logging.FileHandler(f'logs_nr_files_{nr_files}_file_size_{file_size}_intervals_{intervals}_{with_pipeline}/{process_name}_server_{server_number}.log', mode='a')
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)
    return logger
