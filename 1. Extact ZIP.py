import os
import shutil
import multiprocessing

MAIN_PATH = './'
cpu_count = int(multiprocessing.cpu_count() / 2)

print(f'Using {cpu_count} CPUs', flush = True)

shutil.unpack_archive(MAIN_PATH + 'Data.zip', MAIN_PATH + 'Data')
shutil.unpack_archive(MAIN_PATH + 'Data/Stress_dataset.zip', MAIN_PATH + 'Data/Stress_dataset')

stress_data_path = MAIN_PATH + 'Data/Stress_dataset'

file_list = [
    (file, sub_file)
    for file in os.listdir(stress_data_path) 
    for sub_file in os.listdir(os.path.join(stress_data_path, file))
]

def unzip_parallel(file, sub_file):
    shutil.unpack_archive(
        os.path.join(stress_data_path, file, sub_file), 
        os.path.join(stress_data_path, file, sub_file[:-4])
    )

print("Unpacking sub-files")

if __name__ == '__main__':
    multiprocessing.freeze_support()
    with multiprocessing.get_context('spawn').Pool(cpu_count) as pool:
        pool.starmap(unzip_parallel, file_list)
    pool.close

    print("All files unpacked successfully", flush = True)