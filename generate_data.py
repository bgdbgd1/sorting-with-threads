import glob
import os
import subprocess


def generate_data(files_dir, prefix_dir, nr_files, num_records):
    current_dir = os.getcwd()
    main_read_dirs = glob.glob(f"{current_dir}/*")
    res = [i for i in main_read_dirs if files_dir in i]
    if len(res) == 0:
        os.mkdir(f'{current_dir}/{files_dir}')

    dirs = glob.glob(f'{current_dir}/{files_dir}/*')
    res = [i for i in dirs if prefix_dir in i]
    if len(res) == 0:
        os.mkdir(f'{current_dir}/{files_dir}/{prefix_dir}')
    for i in range(nr_files):
        cmd = ['./gensort-1.5/gensort', f'-b{i * num_records}', str(num_records), f'{files_dir}/{prefix_dir}/{i}']
        subprocess.Popen(cmd, stdout=subprocess.PIPE)


if __name__ == '__main__':
    generate_data('read_files_generated', '10MB-10files', 10, 100000)
