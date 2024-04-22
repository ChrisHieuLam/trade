#version 1


import os
import pandas as pd
import shutil
import time
from pathlib import Path


# Directory where CSV files are located
csv_directory = '/home/appcoder/csvs-tv/'

# Directory where the merged Excel files will be saved
excel_directory = '/home/appcoder/excels-tv/'

# Function to delete all files from a folder
def remove_files(directory_path):
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {str(e)}')

# Function to merge CSV files into an Excel file
def merge_csv_to_excel(csv_files, csv_directory, excel_directory):
    dfs = []
    for csv_file in csv_files:
        csv_file_path = os.path.join(csv_directory, csv_file)
        try:
            df = pd.read_csv(csv_file_path)
            dfs.append(df)
        except Exception as e:
            print(f'Failed to read {csv_file_path}. Reason: {str(e)}')
    
    if len(dfs) >= 2:
        try:
            for csv_file in csv_files:
                if 'Performance' in csv_file:
                    excel_filename = Path(csv_file).stem + '.xlsx'
                    break
            else:
                raise ValueError("No CSV file containing 'Performance' in its filename found.")
            excel_file_path = os.path.join(excel_directory, excel_filename)
            with pd.ExcelWriter(excel_file_path) as writer:
                for i, df in enumerate(dfs):
                    df.to_excel(writer, sheet_name=f'Sheet{i+1}', index=False)
                print(f'Merged Excel file saved to: {excel_file_path}')
                remove_files(csv_directory)
                remove_files(csv_directory)
        except Exception as e:
            print(f'Failed to merge CSV files. Reason: {str(e)}')

# Continuously monitor the directory for CSV files
while True:
    try:
        csv_files = [file for file in os.listdir(csv_directory) if file.endswith('.csv')]
        if len(csv_files) >= 2:
            merge_csv_to_excel(csv_files, csv_directory, excel_directory)
        else:
            time.sleep(2)  
    except Exception as e:
        print(f'Error occurred: {str(e)}')
