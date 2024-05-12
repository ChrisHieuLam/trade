#DATE: 09/05/2024
#VERSION: 1.1

import os
import paramiko
import shutil
import time

# Delete all files from a folder
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
            
# Delete one single file from a folder            
def remove_file(file_path):
    try:
        if os.path.isfile(file_path):
            os.remove(file_path)
            print(f"{file_path} removed successfully")
        else:
            print(f"Error: {file_path} not a valid filename")
    except Exception as e:
        print(f"Error occurred while deleting file {file_path}. Reason: {str(e)}")


def is_folder_empty(hostname, port, username, password, folder_path):
    try:
        # Connect to the server
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname, port, username, password)

        # Execute the command to check if the folder is empty
        command = f"find {folder_path} -mindepth 1 -print -quit | head -n 1 | wc -l"
        stdin, stdout, stderr = ssh.exec_command(command)

        # Get the output of the command
        output = stdout.read().decode().strip()

        # Close the SSH connection
        ssh.close()

        # Check if the folder is empty
        return int(output) == 0
    except Exception as e:
        print("An error occurred:", e)
        return False
def is_folder_3(hostname, port, username, password, folder_path):
    try:
        # Connect to the server
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname, port, username, password)

        # Execute the command to check if the folder is empty
        command = f"find {folder_path} -mindepth 1 -print -quit | head -n 1 | wc -l"
        stdin, stdout, stderr = ssh.exec_command(command)

        # Get the output of the command
        output = stdout.read().decode().strip()

        # Close the SSH connection
        ssh.close()

        # Check if the folder is 3
        return int(output) == 3
    except Exception as e:
        print("An error occurred:", e)
        return False



# Upload files from local machine to remote server 
def transfer_files(directory_path, hostname, port, username, password, remote_directory):
    try:
        # Create an SSH client
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Connect to the server
        ssh.connect(hostname, port, username, password)

        # Create an SFTP client
        sftp = ssh.open_sftp()

        while not is_folder_empty(hostname, port, username, password, remote_directory):
            time.sleep(2)
        files_to_transfer = [f for f in os.listdir(directory_path) if f.endswith(".csv")]
        files_to_transfer.sort(key=lambda x: os.path.getctime(os.path.join(directory_path, x)))
        # Loop through all files in the local directory
        for filename in files_to_transfer[:3]:
            local_file = os.path.join(directory_path, filename)
            remote_file = os.path.join(remote_directory, filename)
            # Copy our file to the SFTP server then DELETE it
            sftp.put(local_file, remote_file)
            remove_file(local_file)
            print("Files transferred and deleted successfully!", local_file)
        files_to_transfer.clear()
            
                    

        # Close the SFTP connection
        sftp.close()
        # Close the SSH connection
        ssh.close()

    except Exception as e:
        print(f"An error occurred: {str(e)}")

# check,  upload then delete .csv file from tv to bottactic.com
while True:
    transfer_files("C:/Users/bigwh/Downloads/", "tradingbot.click", 22, "sysboss", "Team_500", "/home/sysboss/csvs-tv")
    time.sleep(1)