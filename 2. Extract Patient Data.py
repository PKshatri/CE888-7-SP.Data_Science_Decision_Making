import os
import multiprocessing

import pandas as pd

MAIN_PATH = './'
cpu_count = int(multiprocessing.cpu_count() / 2)

DATA_PATH = MAIN_PATH + "Data/Stress_dataset"
SAVE_PATH = MAIN_PATH + "Merged_sensor_data"
if not os.path.exists(SAVE_PATH):
    os.mkdir(SAVE_PATH)
    
final_columns = {
    'ACC': ['id', 'X', 'Y', 'Z', 'datetime'],
    'EDA': ['id', 'EDA', 'datetime'],
    'HR': ['id', 'HR', 'datetime'],
    'TEMP': ['id', 'TEMP', 'datetime'],
    'BVP': ['id', 'BVP', 'datetime'],
    'IBI': ['id', 'IBI', 'datetime'],
}

names = {
    'ACC.csv': ['X', 'Y', 'Z'],
    'EDA.csv': ['EDA'],
    'HR.csv': ['HR'],
    'TEMP.csv': ['TEMP'],
    'BVP.csv': ['BVP'],
    'IBI.csv': ['IBI'],
}

desired_signals = ['ACC.csv', 'EDA.csv', 'HR.csv', 'TEMP.csv', 'BVP.csv', 'IBI.csv']

def process_df(df, file):
    start_timestamp = df.iloc[0,0]
    sample_rate = df.iloc[1,0]
    new_df = pd.DataFrame(df.iloc[2:].values, columns=df.columns)
    new_df['id'] =  file[-2:]
    new_df['datetime'] = [(start_timestamp + i/sample_rate) for i in range(len(new_df))]
    return new_df

def process_ibi(df, file):
    time = df.index
    start_timestamp = time[0]
    new_df = pd.DataFrame(df.iloc[1:].values, columns=df.columns)
    new_df['id'] =  file[-2:]
    new_df['datetime'] = [(start_timestamp + time[i]) for i in range(1, len(time))]
    return new_df

def merge_sensor_data(file):
    print(f'Processing {file}', flush = True)
    acc = pd.DataFrame(columns=final_columns['ACC'])
    eda = pd.DataFrame(columns=final_columns['EDA'])
    hr = pd.DataFrame(columns=final_columns['HR'])
    temp = pd.DataFrame(columns=final_columns['TEMP'])
    bvp = pd.DataFrame(columns=final_columns['BVP'])
    ibi = pd.DataFrame(columns=final_columns['IBI'])
    for sub_file in os.listdir(os.path.join(DATA_PATH, file)):
        if sub_file.endswith(".zip"):
            os.remove(os.path.join(DATA_PATH, file, sub_file))
        else:
            for signal in os.listdir(os.path.join(DATA_PATH, file, sub_file)):
                if signal in desired_signals:
                    df = pd.read_csv(os.path.join(DATA_PATH, file, sub_file, signal), names=names[signal], header=None)
                    if not df.empty:
                        if signal == 'ACC.csv':
                            acc = pd.concat([acc, process_df(df, file)])             
                        if signal == 'EDA.csv':
                            eda = pd.concat([eda, process_df(df, file)])
                        if signal == 'HR.csv':
                            hr = pd.concat([hr, process_df(df, file)])
                        if signal == 'TEMP.csv':
                            temp = pd.concat([temp, process_df(df, file)])
                        if signal == 'BVP.csv':
                            bvp = pd.concat([bvp, process_df(df, file)])
                        if signal == 'IBI.csv':
                            ibi = pd.concat([ibi, process_ibi(df, file)])
    print(f'Saving {file}', flush = True)
    temp_save = os.path.join(SAVE_PATH, file)
    os.mkdir(temp_save)
    acc.to_csv(os.path.join(temp_save, 'combined_acc.csv'), index=False)
    eda.to_csv(os.path.join(temp_save, 'combined_eda.csv'), index=False)
    hr.to_csv(os.path.join(temp_save, 'combined_hr.csv'), index=False)
    temp.to_csv(os.path.join(temp_save, 'combined_temp.csv'), index=False)
    bvp.to_csv(os.path.join(temp_save, 'combined_bvp.csv'), index=False)
    ibi.to_csv(os.path.join(temp_save, 'combined_ibi.csv'), index=False)

file_list = [file for file in os.listdir(DATA_PATH)]

if __name__ == '__main__':
    multiprocessing.freeze_support()
    with multiprocessing.get_context('spawn').Pool(cpu_count) as pool:
        pool.map(merge_sensor_data, file_list)
    pool.close()

    print("All sensor data files saved successfully", flush = True)