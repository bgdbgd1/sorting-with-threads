import glob
import io
import os
import subprocess
from minio import Minio


def generate_data_with_minio(files_dir, nr_files, num_records, minio_ip, bucket):
    for i in range(nr_files):
        cmd = ['./gensort-1.5/gensort', f'-b{i * num_records}', str(num_records), f'{files_dir}/{i}']
        subprocess.Popen(cmd, stdout=subprocess.PIPE)

    files = glob.glob(f'{files_dir}/*')
    minio_client = Minio(
        f"{minio_ip}:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    found = minio_client.bucket_exists(bucket)
    if not found:
        minio_client.make_bucket(bucket)

    for file_name in files:
        f_name = file_name.split('/')[-1]
        with open(file_name, 'rb') as file:
            minio_client.put_object(bucket, f_name, file, length=os.path.getsize(file_name))


def generate_data(files_dir, nr_files, num_records):
    # current_dir = os.getcwd()
    # main_read_dirs = glob.glob(f"{current_dir}/*")
    # res = [i for i in main_read_dirs if files_dir in i]
    # if len(res) == 0:
    #     os.mkdir(f'{current_dir}/{files_dir}')
    #
    # dirs = glob.glob(f'{current_dir}/{files_dir}/*')
    # res = [i for i in dirs if prefix_dir in i]
    # if len(res) == 0:
    #     os.mkdir(f'{current_dir}/{files_dir}/{prefix_dir}')
    for i in range(nr_files):
        cmd = ['./gensort-1.5/gensort', f'-b{i * num_records}', str(num_records), f'{files_dir}/{i}']
        subprocess.Popen(cmd, stdout=subprocess.PIPE)


if __name__ == '__main__':
    # generate_data('/local/bee700/minio_storage/read', 100, 1000000)
    generate_data_with_minio('/local/bee700/gen_data', 100, 1000000, '127.0.0.1', 'read')
