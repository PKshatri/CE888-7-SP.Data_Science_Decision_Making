import os
import multiprocessing

import pandas as pd

MAIN_PATH = './'
cpu_count = int(multiprocessing.cpu_count() / 2)

COMBINED_DATA_PATH = MAIN_PATH + "Merged_sensor_data"
SAVE_PATH = MAIN_PATH + "Final_data"

if not os.path.exists(SAVE_PATH):
    os.mkdir(SAVE_PATH)

def merge_parallel(file):
    print(f'processing {file}', flush = True)
    
    df = pd.DataFrame(columns = ['X', 'Y', 'Z', 'BVP', 'EDA', 'HR', 'IBI', 'TEMP', 'id', 'datetime'])
    acc = pd.read_csv(os.path.join(COMBINED_DATA_PATH, file, "combined_acc.csv"), dtype={'id': str})
    bvp = pd.read_csv(os.path.join(COMBINED_DATA_PATH, file, "combined_bvp.csv"), dtype={'id': str}, usecols=lambda x: x != "id")
    eda = pd.read_csv(os.path.join(COMBINED_DATA_PATH, file, "combined_eda.csv"), dtype={'id': str}, usecols=lambda x: x != "id")
    hr = pd.read_csv(os.path.join(COMBINED_DATA_PATH, file, "combined_hr.csv"), dtype={'id': str}, usecols=lambda x: x != "id")
    ibi = pd.read_csv(os.path.join(COMBINED_DATA_PATH, file, "combined_ibi.csv"), dtype={'id': str}, usecols=lambda x: x != "id")
    temp = pd.read_csv(os.path.join(COMBINED_DATA_PATH, file, "combined_temp.csv"), dtype={'id': str}, usecols=lambda x: x != "id")

    df = acc.merge(bvp, on='datetime', how='outer')
    df = df.merge(eda, on='datetime', how='outer')
    df = df.merge(hr, on='datetime', how='outer')
    df = df.merge(ibi, on='datetime', how='outer')
    df = df.merge(temp, on='datetime', how='outer')
    
    df.fillna(method='ffill', inplace=True)
    df.fillna(method='bfill', inplace=True)
    
    print(f'Saving {file}', flush = True)
    df.to_csv(os.path.join(SAVE_PATH, f"merged_{file}.csv"), index = None)

print("Reading data...", flush = True)

file_list = [file for file in os.listdir(COMBINED_DATA_PATH)]

print("Processing data...", flush = True)

if __name__ == '__main__':
    multiprocessing.freeze_support()
    with multiprocessing.get_context('spawn').Pool(cpu_count) as pool:
        pool.map(merge_parallel, file_list)
    pool.close()

    print("Successfully merged sensor data")