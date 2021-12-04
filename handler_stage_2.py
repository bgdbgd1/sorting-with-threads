import io
import json
from concurrent.futures.thread import ThreadPoolExecutor
from enum import Enum

import numpy as np


class SortingHandlerStage2:
    read_dir = None
    write_dir = None

    initial_files = []
    files_read = {}

    read_files = 0
    determined_categories_files = 0
    written_files = 0

    max_read = 2
    max_determine_categories = 1
    max_write = 1

    current_read = 0
    current_determine_categories = 0
    current_write = 0

    buffers_filled = 0
    max_buffers_filled = 2

    reading_threads = ThreadPoolExecutor(max_workers=2)
    determine_categories_threads = ThreadPoolExecutor(max_workers=1)
    writing_threads = ThreadPoolExecutor(max_workers=1)

    locations = {}
