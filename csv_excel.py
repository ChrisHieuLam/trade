#DATE: 09/05/2024
#VERSION: 1.0



import os
import pandas as pd
import shutil
import time
from pathlib import Path


# Directory where CSV files are located
csv_directory = '/home/sysboss/csvs-tv/'

# Directory where the merged Excel files will be saved
excel_directory = '/home/sysboss/excels-tv/'

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
    dfs = {}
    for csv_file in csv_files:
        csv_file_path = os.path.join(csv_directory, csv_file)
        try:
            sheetname = os.path.splitext(os.path.basename(csv_file))[0]
            df = pd.read_csv(csv_file_path)
            dfs[sheetname] = pd.read_csv(csv_file_path)
        except Exception as e:
            print(f'Failed to read {csv_file_path}. Reason: {str(e)}')
    
    if len(dfs) >= 3:
        try:
            for csv_file in csv_files:
                if 'Performance' in csv_file:
                    excel_filename = Path(csv_file).stem + '.xlsx'
                    break
            else:
                raise ValueError("No CSV file containing 'Performance' in its filename found.")
            excel_file_path = os.path.join(excel_directory, excel_filename)
            with pd.ExcelWriter(excel_file_path) as writer:
                for sheet_name, df in dfs.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f'Merged Excel file saved to: {excel_file_path}')
                remove_files(csv_directory)
        except Exception as e:
            print(f'Failed to merge CSV files. Reason: {str(e)}')

# Continuously monitor the directory for CSV files
while True:
    try:
        csv_files = [file for file in os.listdir(csv_directory) if file.endswith('.csv')]
        csv_files.sort(key=lambda x: os.path.getctime(os.path.join(csv_directory, x)))
        if len(csv_files) >= 3:
            merge_csv_to_excel(csv_files, csv_directory, excel_directory)
        else:
            time.sleep(2)  
    except Exception as e:
        print(f'Error occurred: {str(e)}')
