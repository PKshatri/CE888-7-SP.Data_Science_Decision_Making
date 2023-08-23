import os
import multiprocessing

import pandas as pd
import numpy as np

from datetime import timedelta, datetime

MAIN_PATH = './'
cpu_count = int(multiprocessing.cpu_count() / 2)

PATH = MAIN_PATH + 'Final_data'
SAVE_PATH = MAIN_PATH + 'Processed_data'

if not os.path.exists(SAVE_PATH):
    os.mkdir(SAVE_PATH)

print("Reading Survey Results...")
survey_path = MAIN_PATH + 'Data/SurveyResults.xlsx'

survey_df = pd.read_excel(survey_path, usecols=['ID', 'Start time', 'End time', 'date', 'Stress level'], dtype={'ID': str})
survey_df['Stress level'].replace('na', np.nan, inplace=True)
survey_df.dropna(inplace=True)

survey_df['Start datetime'] =  pd.to_datetime(survey_df['date'].map(str) + ' ' + survey_df['Start time'].map(str))
survey_df['End datetime'] =  pd.to_datetime(survey_df['date'].map(str) + ' ' + survey_df['End time'].map(str))
survey_df.drop(['Start time', 'End time', 'date'], axis=1, inplace=True)

# Convert SurveyResults.xlsx to GMT-00:00
print("Processing Survey Results...")
daylight = pd.to_datetime(datetime(2020, 11, 1, 0, 0))

survey_df1 = survey_df[survey_df['End datetime'] <= daylight].copy()
survey_df1['Start datetime'] = survey_df1['Start datetime'].apply(lambda x: x + timedelta(hours=5))
survey_df1['End datetime'] = survey_df1['End datetime'].apply(lambda x: x + timedelta(hours=5))

survey_df2 = survey_df.loc[survey_df['End datetime'] > daylight].copy()
survey_df2['Start datetime'] = survey_df2['Start datetime'].apply(lambda x: x + timedelta(hours=6))
survey_df2['End datetime'] = survey_df2['End datetime'].apply(lambda x: x + timedelta(hours=6))

survey_df = pd.concat([survey_df1, survey_df2], ignore_index=True)
# survey_df = survey_df.loc[survey_df['Stress level'] != 1.0]

survey_df.reset_index(drop=True, inplace=True)

def parallel(file):
    
    id = file[-6:-4]
    # Read Files
    print(f'Processing {id}', flush = True)

    df = pd.read_csv(os.path.join(PATH, f'{file}'), dtype={'id': str})
    df['datetime'] = pd.to_datetime(df['datetime'].apply(lambda x: x * (10 ** 9)))
    
    new_df = pd.DataFrame(columns=['X', 'Y', 'Z', 'BVP', 'IBI', 'EDA', 'HR', 'TEMP', 'id', 'datetime', 'label'])

    sdf = df.copy()
    survey_sdf = survey_df[survey_df['ID'] == id].copy()

    for _, survey_row in survey_sdf.iterrows():
        ssdf = sdf[(sdf['datetime'] >= survey_row['Start datetime']) & (sdf['datetime'] <= survey_row['End datetime'])].copy()

        if not ssdf.empty:
            ssdf['label'] = np.repeat(survey_row['Stress level'], len(ssdf.index))
            new_df = pd.concat([new_df, ssdf], ignore_index=True)
        else:
            print(f"{survey_row['ID']} is missing label {survey_row['Stress level']} at {survey_row['Start datetime']} to {survey_row['End datetime']}")
    
    new_df.to_csv(os.path.join(SAVE_PATH, f'{id}.csv'), index = None)

print("Processing data...")

file_list = [file for file in os.listdir(PATH)]

if __name__ == '__main__':
    multiprocessing.freeze_support()
    with multiprocessing.get_context('spawn').Pool(cpu_count) as pool:
        pool.map(parallel, file_list)
    pool.close()

    print("Successfully labelled data", flush = True)