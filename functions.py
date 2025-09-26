from cryptography.fernet import Fernet
import pandas as pd
import os
import sys
import json
from datetime import datetime

from defines import bar_len, excluded_statuses, user_entered_columns
from local_secrets import PASSWORD_FILE_PATH, ENCRYPTED_KEY_PATH

def blue_gradient_bar(progress, total):
    """
    Prints a blue gradient progress bar to the terminal.
    progress: current progress (int)
    total: total steps (int)
    bar_len: length of the bar (int)
    """
    # Light blue (start): RGB(173, 216, 230)
    # Dark blue (end):   RGB(0, 0, 139)
    start_rgb = (173, 216, 230)
    end_rgb = (0, 0, 139)
    filled_len = int(bar_len * progress // total) if total else bar_len
    bar = ""
    for i in range(bar_len):
        # Interpolate color
        ratio = i / (bar_len - 1) if bar_len > 1 else 0
        r = int(start_rgb[0] + (end_rgb[0] - start_rgb[0]) * ratio)
        g = int(start_rgb[1] + (end_rgb[1] - start_rgb[1]) * ratio)
        b = int(start_rgb[2] + (end_rgb[2] - start_rgb[2]) * ratio)
        color = f"\033[38;2;{r};{g};{b}m"
        if i < filled_len:
            bar += f"{color}█"
        else:
            bar += f"\033[0m-"
    reset = "\033[0m"
    percent = int((progress / total) * 100) if total else 100
    sys.stdout.write(f"\r[{bar}{reset}] {progress}/{total} directories processed, {percent}%")
    sys.stdout.flush()

def get_api_key_file():
    """
    Reads an encryption key from a text file and uses it to decrypt the API key.

    Args:
        None

    Returns:
        str: The decrypted API key as a string, used for authenticating with the Smartsheet API.

    Raises:
        FileNotFoundError: If the password or encrypted key file is missing.
        Exception: If decryption fails.
    """
    with open(PASSWORD_FILE_PATH, "r") as f:
        key = f.read().strip().encode()  # Read and encode the key
    fernet = Fernet(key)
    with open(ENCRYPTED_KEY_PATH, "rb") as f:
        encrypted = f.read()

    return fernet.decrypt(encrypted).decode()

def convert_sheet_to_dataframe(sheet):
    """
    Convert a Smartsheet sheet object to a pandas DataFrame.

    Args:
        sheet: Smartsheet sheet object as returned by smartsheet_client.Sheets.get_sheet().

    Returns:
        pd.DataFrame: DataFrame containing the sheet's data, with columns matching the sheet's 
        column titles. Empty cells are returned as None. Column order matches the Smartsheet sheet.
    """
    data = []
    columns = [col.title for col in sheet.columns]
    total_rows = len(sheet.rows)
    
    for idx, row in enumerate(sheet.rows):
        row_data = {'_row_id': row.id}  # Add Smartsheet row ID
        for cell in row.cells:
            if cell.column_id in [col.id for col in sheet.columns]:
                col_index = [col.id for col in sheet.columns].index(cell.column_id)
                row_data[columns[col_index]] = cell.value

        data.append(row_data)

        # Show progress bar
        blue_gradient_bar(idx + 1, total_rows)
    # Newline after progress bar
    print()
    print() 

    # Add '_row_id' to columns for DataFrame
    return pd.DataFrame(data, columns=['_row_id'] + columns)

def load_assembly_job_data(network_dir: str, log_camData_path: str) -> pd.DataFrame:
    """
    Scans the top-level directories in the given network path for camReadme.txt files, parses their contents,
    and returns a DataFrame of the results.

    Args:
        network_dir (str): The network directory to search. Each subdirectory is checked for a camReadme.txt file.

    Returns:
        pd.DataFrame: DataFrame containing parsed data from all camReadme.txt files found.
            Each row includes a '__file_path__' column with the source file path.

    Notes:
        - Only the top-level subdirectories of network_dir are searched (not recursive).
        - Progress is printed to the terminal using a bar of length bar_len from defines.py.
    """
    data = []

    # Step 1: Load previous data if available
    if os.path.isfile(log_camData_path) and os.path.getsize(log_camData_path) > 0:
        try:
            with open(log_camData_path, "r") as f:
                old_data = json.load(f)
            old_lookup = {entry["__file_path__"]: entry for entry in old_data}
        except Exception:
            old_lookup = {}
    else:
        old_lookup = {}

    dir_names = [entry_name for entry_name in os.listdir(network_dir) if os.path.isdir(os.path.join(network_dir, entry_name))]
    total_dirs = len(dir_names)

    for idx, entry_name in enumerate(dir_names):
        entry_path = os.path.join(network_dir, entry_name)
        camreadme_path = os.path.normpath(os.path.join(entry_path, "camReadme.txt"))

        if os.path.isfile(camreadme_path):
            mtime = os.path.getmtime(camreadme_path)
            # Step 3: Check if file is unchanged
            if camreadme_path in old_lookup and old_lookup[camreadme_path].get("__file_mtime__") == mtime:
                entry = old_lookup[camreadme_path]
                data.append(entry)
            else:
                # Step 4: Parse new/changed file
                try:
                    with open(camreadme_path, "r", encoding="utf-8", errors="ignore") as f:
                        entry = {}
                        lines = f.readlines()
                        for line in lines:
                            if '|' in line:
                                key, value = line.strip().split('|', 1)
                                entry[key.strip()] = value.strip()
                        entry['__file_path__'] = camreadme_path
                        entry['__file_mtime__'] = mtime

                    for k, v in entry.items():
                        if isinstance(v, str):
                            entry[k] = v.rstrip('|')

                    data.append(entry)

                except Exception as e:
                    print(f"Error reading {camreadme_path}: {e}")
                    continue

        # Print color gradient progress bar
        blue_gradient_bar(idx + 1, total_dirs)
    # Newline after progress bar
    print()
    print() 

    # Return all camData file information
    return pd.DataFrame(data)

def build_active_credithold_files(cam_data: list) -> tuple:
    # Initialize lists for the two output files
    active_jobs = []
    credit_hold_jobs = []
    
    # Get current date for tracking purposes
    current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    total_records = len(cam_data)
    
    # Process each record in the source data
    for idx, record in enumerate(cam_data):
        status = record.get('Status', '')
        credit_hold = record.get('Credit Hold', '')
        
        # Check if job is on credit hold
        if credit_hold == 'YES':
            # Add to credit hold list with tracking date
            credit_hold_record = record.copy()
            credit_hold_record['tracking_date'] = current_date
            credit_hold_jobs.append(credit_hold_record)
        # Check if job is active (not in excluded statuses and not on credit hold)
        elif status not in excluded_statuses and credit_hold != 'YES':
            active_jobs.append(record)

        # Print color gradient progress bar
        blue_gradient_bar(idx + 1, total_records)
    # Newline after progress bar
    print(f"Active jobs - {len(active_jobs)} records")
    print(f"Credit hold jobs - {len(credit_hold_jobs)} records")
    print()
    print() 

    return active_jobs, credit_hold_jobs

def generate_statistics_file(cam_data: list, active_jobs: list, credit_hold_jobs: list):
    print("Generating job statistics Excel file...")
    
    # Step 1: Initialize and calculate statistics
    blue_gradient_bar(1, 4)
    job_statistics = {
        "total_jobs": 0,
        "active_jobs": 0,
        "credit_hold_jobs": 0
    }

    # Update statistics based on processed jobs
    job_statistics["total_jobs"] = len(cam_data)
    job_statistics["active_jobs"] = len(active_jobs)
    job_statistics["credit_hold_jobs"] = len(credit_hold_jobs)

    # Step 2: Prepare Excel file
    blue_gradient_bar(2, 4)
    excel_file_path = 'SaveFiles/job_statistics.xlsx'

    # Create Excel writer object
    with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
        
        # Step 3: Create Sheet 1 - Job Statistics Summary
        blue_gradient_bar(3, 4)
        stats_df = pd.DataFrame([
            {'Metric': 'Total Jobs', 'Count': job_statistics["total_jobs"]},
            {'Metric': 'Active Jobs', 'Count': job_statistics["active_jobs"]}, 
            {'Metric': 'Credit Hold Jobs', 'Count': job_statistics["credit_hold_jobs"]}
        ])
        stats_df.to_excel(writer, sheet_name='Job Statistics', index=False)
        
        # Step 4: Create Sheet 2 - Active Jobs Detail from active jobs log file
        blue_gradient_bar(4, 4)
        try:
            with open('SaveFiles/log_active_jobs.json', 'r') as file:
                active_jobs_data = json.load(file)
            
            # Create DataFrame with specified columns
            if active_jobs_data:
                active_jobs_df = pd.DataFrame([
                    {
                        'WO#': job.get('WO#', ''),
                        'Quote#': job.get('Quote#', ''),
                        'Status': job.get('Status', ''),
                        'Order Date': job.get('Order Date', ''),
                        'Customer': job.get('Customer', '')
                    }
                    for job in active_jobs_data
                ])
            else:
                # Create empty DataFrame with headers if no data
                active_jobs_df = pd.DataFrame(columns=['WO#', 'Quote#', 'Status', 'Order Date', 'Customer'])
            
            active_jobs_df.to_excel(writer, sheet_name='Active Jobs Detail', index=False)
            
        except FileNotFoundError:
            # If active jobs file doesn't exist, create empty sheet
            empty_df = pd.DataFrame(columns=['WO#', 'Quote#', 'Status', 'Order Date', 'Customer'])
            empty_df.to_excel(writer, sheet_name='Active Jobs Detail', index=False)

    # Complete - show final results
    print(f"Job statistics Excel file created: {excel_file_path}")
    print(f"Sheet 1: Job Statistics Summary with {len(stats_df)} metrics")
    if 'active_jobs_data' in locals():
        print(f"Sheet 2: Active Jobs Detail with {len(active_jobs_data)} records")

def store_smartsheet_user_data(smartsheet_part_tracking_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts user-entered data from specified columns in the Smartsheet DataFrame.
    Stores data in a json log file with row ID and WO#

    Args:
        smartsheet df (pd.DataFrame): DataFrame containing Smartsheet data, including a '_row_id' column.

    Returns:
        None.
    """
    try:
        # Extract user-entered data from smartsheet
        if 'smartsheet_part_tracking_df' in locals() and not smartsheet_part_tracking_df.empty:
            # Filter to only include rows that have data in user-entered columns
            user_entered_data = []
            
            for index, row in smartsheet_part_tracking_df.iterrows():
                # Check if any of the user-entered columns have non-null values
                has_user_data = False
                row_data = {}
                
                # Include essential columns only: _row_id, WO#, and user-entered columns
                essential_columns = ['_row_id', 'WO#'] + user_entered_columns
                
                # Add _row_id for reference if it exists
                if '_row_id' in smartsheet_part_tracking_df.columns:
                    row_data['_row_id'] = row['_row_id']
                
                # Add WO# for identification if it exists
                if 'WO#' in smartsheet_part_tracking_df.columns:
                    row_data['WO#'] = row['WO#'] if pd.notna(row['WO#']) else None
                
                # Process user-entered columns
                for col in user_entered_columns:
                    if col in smartsheet_part_tracking_df.columns:
                        # Check if this user-entered column has data
                        if pd.notna(row[col]) and str(row[col]).strip() != '':
                            has_user_data = True
                            row_data[col] = row[col]
                        else:
                            row_data[col] = None
                
                # Only include rows that have some user-entered data
                if has_user_data:
                    user_entered_data.append(row_data)
            
            # Ensure SaveFiles directory exists
            os.makedirs('SaveFiles', exist_ok=True)
            
            # Save to JSON file
            user_data_file_path = 'SaveFiles/log_user_entered_data.json'
            with open(user_data_file_path, 'w') as file:
                json.dump(user_entered_data, file, indent=2, default=str)
            
            print(f"User-entered data saved: {len(user_entered_data)} records with user input")
            print(f"File location: {user_data_file_path}")
        
        else:
            print("No smartsheet data available to extract user-entered information")
            # Create empty file
            with open('SaveFiles/log_user_entered_data.json', 'w') as file:
                json.dump([], file)

    except Exception as e:
        print(f"Error storing user-entered data: {e}")
        # Create empty file on error
        with open('SaveFiles/log_user_entered_data.json', 'w') as file:
            json.dump([], file)

