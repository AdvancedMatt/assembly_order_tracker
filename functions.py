from operator import index
from cryptography.fernet import Fernet
import pandas as pd
import os
import sys
import json
from datetime import datetime
from dateutil import parser

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

def load_json_file(file_path: str, default_value=None):
    """
    Loads data from a JSON file with error handling.

    Args:
        file_path (str): Path to the JSON file to load
        default_value: Value to return if file doesn't exist or is corrupted (default: None)

    Returns:
        The loaded JSON data, or default_value if file doesn't exist/is corrupted
    """
    try:
        if os.path.isfile(file_path) and os.path.getsize(file_path) > 0:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            print(f"⚠ File not found or empty: {file_path}")
            return default_value if default_value is not None else []
    except json.JSONDecodeError as e:
        print(f"⚠ Error: Corrupted JSON file {file_path}: {e}")
        return default_value if default_value is not None else []
    except FileNotFoundError:
        print(f"⚠ File not found: {file_path}")
        return default_value if default_value is not None else []
    except Exception as e:
        print(f"⚠ Unexpected error loading {file_path}: {e}")
        return default_value if default_value is not None else []

def save_json_file(data, file_path: str, create_dir=True):
    """
    Saves data to a JSON file with error handling.

    Args:
        data: The data to save (must be JSON serializable)
        file_path (str): Path where to save the JSON file
        create_dir (bool): Whether to create the directory if it doesn't exist (default: True)

    Returns:
        bool: True if successful, False if failed
    """
    try:
        # Create directory if it doesn't exist and create_dir is True
        if create_dir:
            directory = os.path.dirname(file_path)
            if directory:
                os.makedirs(directory, exist_ok=True)
        
        # Save the data
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=str, ensure_ascii=False)
        
        return True
        
    except TypeError as e:
        print(f"⚠ Error: Data not JSON serializable for {file_path}: {e}")
        return False
    except PermissionError as e:
        print(f"⚠ Error: Permission denied writing to {file_path}: {e}")
        return False
    except OSError as e:
        print(f"⚠ Error: OS error writing to {file_path}: {e}")
        return False
    except Exception as e:
        print(f"⚠ Unexpected error saving {file_path}: {e}")
        return False

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

def build_active_credithold_files(cam_data: list, existing_credit_holds: set) -> tuple:
    # Initialize lists for the two output files
    active_jobs = []
    credit_hold_jobs = []
    credit_hold_released = []
    
    # Get current date for tracking purposes - should be formatted for smartsheet API
    current_date = datetime.now().strftime('%m/%d/%y')

    total_records = len(cam_data)
    
    # Process each record in the source data
    for idx, record in enumerate(cam_data):
        status = record.get('Status', '')
        credit_hold = record.get('Credit Hold', '')
        wo_number = record.get('WO#', '')
        
        # Check if job is on credit hold
        if credit_hold == 'YES':
            # Add to credit hold list with only specified columns
            credit_hold_record = {
                'Status': record.get('Status', ''),
                'Quote#': record.get('Quote#', ''),
                'WO#': record.get('WO#', ''),
                'Customer': record.get('Customer', ''),
                'Credit Hold': record.get('Credit Hold', ''),
                '__file_path__': record.get('__file_path__', ''),
                '__file_mtime__': record.get('__file_mtime__', ''),
                'internal_status': record.get('internal_status', ''),
                'tracking_date': current_date
            }
            credit_hold_jobs.append(credit_hold_record)

        # Check if job was released from credit hold
        elif credit_hold == 'NO' and wo_number in existing_credit_holds:
            # This WO# was previously on credit hold but now released
            credit_hold_released.append({
                'WO#': wo_number,
                'released_date': current_date
            })
            # Also add to active jobs if it meets active criteria
            if status not in excluded_statuses:
                active_jobs.append(record)

        # Check if job is active (not in excluded statuses and not on credit hold)
        elif status not in excluded_statuses and credit_hold != 'YES':
            active_jobs.append(record)

        # Print color gradient progress bar
        blue_gradient_bar(idx + 1, total_records)
    # Newline after progress bar
    print(f"Active jobs - {len(active_jobs)} records")
    print(f"Credit hold jobs - {len(credit_hold_jobs)} records")
    print(f"Credit hold released - {len(credit_hold_released)} records")
    print()
    print() 

    return active_jobs, credit_hold_jobs, credit_hold_released

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

            total_rows = len(smartsheet_part_tracking_df)
            
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

                # Show progress bar
                blue_gradient_bar(index + 1, total_rows)
            # Newline after progress bar
            print()
            print()
            
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

def extract_first_designator(designators_string):
    """
    Extract the first designator from a string that may contain multiple designators
    separated by comma, space, semicolon, or ranges with dashes.
    
    Args:
        designators_string (str): String containing designators
        
    Returns:
        str: First designator found, stripped of whitespace
    """
    if not designators_string:
        return ''
    
    # Handle different separators: comma, semicolon, space
    # First try comma separation (most common)
    if ',' in designators_string:
        return designators_string.split(',')[0].strip()
    
    # Then try semicolon separation
    if ';' in designators_string:
        return designators_string.split(';')[0].strip()
    
    # Handle range notation like "C1-C70" - return the first part
    if '-' in designators_string and not designators_string.startswith('-'):
        # Make sure it's not a negative number by checking if there's a letter before the dash
        parts = designators_string.split('-')
        if len(parts) >= 2 and any(c.isalpha() for c in parts[0]):
            return parts[0].strip()
    
    # Handle space separation (least common, check last to avoid splitting ranges)
    if ' ' in designators_string:
        return designators_string.split(' ')[0].strip()
    
    # If no separators found, return the whole string stripped
    return designators_string.strip()

def format_mmddyy(date_str):
    if not date_str or str(date_str).lower() in ['none', 'null', '']:
        return ""
    try:
        dt = parser.parse(str(date_str), fuzzy=True)
        # return dt.strftime("%m-%d-%y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return str(date_str)  # fallback to original if parsing fails

def build_master_bom(jobs_df: pd.DataFrame, assembly_active_directory: str, debug_output: bool = False) -> pd.DataFrame:
    print("Building master BOM dataframe...")
    
    # Initialize list to store all BOM data
    master_bom_data = []
    
    # Get WO# values from active jobs for filtering
    active_wo_numbers = {job.get('WO#', '') for job in jobs_df if job.get('WO#')}
    print(f"Found {len(active_wo_numbers)} active work orders to process")

    # Create a lookup dictionary for WO# to Quote# mapping
    wo_to_quote = {job.get('WO#', ''): job.get('Quote#', '') for job in jobs_df if job.get('WO#')}
    
    # Get all directories in ASSEMBLY_ACTIVE_DIRECTORY
    if os.path.exists(assembly_active_directory):
        directories = [d for d in os.listdir(assembly_active_directory) 
                      if os.path.isdir(os.path.join(assembly_active_directory, d))]
        
        total_dirs = len(directories)
        processed_count = 0
        
        for idx, directory in enumerate(directories):
            # Check if any active WO# is part of the directory name
            matching_wo = None
            for wo_number in active_wo_numbers:
                if wo_number and wo_number in directory:
                    matching_wo = wo_number
                    break
            
            if matching_wo:
                dir_path = os.path.join(assembly_active_directory, directory)
                
                # Find stdBOM file in the directory
                try:
                    files = os.listdir(dir_path)
                    stdbom_files = [f for f in files if 'stdBOM' in f and f.endswith('.txt')]
                    
                    if stdbom_files:
                        # Use the first stdBOM file found
                        stdbom_file = stdbom_files[0]
                        stdbom_path = os.path.join(dir_path, stdbom_file)
                        
                        # Parse the stdBOM file
                        try:
                            with open(stdbom_path, 'r', encoding='utf-8', errors='ignore') as f:
                                lines = f.readlines()
                                
                            for line in lines:
                                line = line.strip()
                                if line and '|' in line:
                                    # Split by pipe delimiter
                                    parts = line.split('|')
                                    
                                    # Create BOM record with WO# as first column
                                    bom_record = {
                                        'WO#': matching_wo,
                                        'Quote#': wo_to_quote.get(matching_wo, '')
                                    }
                                    
                                    # Add all columns
                                    if len(parts) >= 1:
                                        bom_record['Part_Number'] = parts[0] if len(parts) > 0 else ''      # white
                                        bom_record['MPN'] = parts[1] if len(parts) > 1 else ''              # blue
                                        bom_record['API_URL'] = parts[2] if len(parts) > 2 else ''          # yellow
                                        bom_record['Description'] = parts[3] if len(parts) > 3 else ''      # green
                                        bom_record['Designators'] = parts[4] if len(parts) > 4 else ''      # orange
                                        bom_record['Designator_Count'] = parts[5] if len(parts) > 5 else '' # lt blue
                                        bom_record['Source'] = parts[6] if len(parts) > 6 else ''           # lt green
                                        bom_record['Line_Number'] = parts[7] if len(parts) > 7 else ''      # teal - 0=pcb, 1000=stencil
                                        bom_record['Req_Qty'] = parts[8] if len(parts) > 8 else ''          # med blue
                                        bom_record['Recvd_Qty'] = parts[9] if len(parts) > 9 else ''        # red
                                        bom_record['Date_Complete'] = parts[10] if len(parts) > 10 else ''  # white
                                        bom_record['Notes'] = parts[11] if len(parts) > 11 else ''          # blue
                                        bom_record['Cust_Supplied'] = parts[12] if len(parts) > 12 else ''  # yellow
                                    
                                    master_bom_data.append(bom_record)
                                    
                        except Exception as e:
                            print(f"Error reading stdBOM file {stdbom_path}: {e}")
                            continue
                            
                        processed_count += 1
                        
                except Exception as e:
                    print(f"Error accessing directory {dir_path}: {e}")
                    continue
            
            # Show progress
            blue_gradient_bar(idx + 1, total_dirs)
        
        # Newline after progress bar
        print()
        print(f"Processed {processed_count} directories with stdBOM files")
        print(f"Total BOM records found: {len(master_bom_data)}")
        
        # Create DataFrame
        if master_bom_data:
            master_bom_df = pd.DataFrame(master_bom_data)
            
            # Save as CSV file
            csv_file_path = 'SaveFiles/master_BOM_no_overage.csv'
            os.makedirs('SaveFiles', exist_ok=True)
            master_bom_df.to_csv(csv_file_path, index=False)
            print(f"Master BOM (no overage) saved to: {csv_file_path}")
            print(f"DataFrame shape: {master_bom_df.shape}")
            
            # Show sample data
            if debug_output:
                print("\nSample BOM data:")
                print(master_bom_df.head())
                print(f"\nUnique WO# count: {master_bom_df['WO#'].nunique()}")
                
        else:
            print("No BOM data found")
            master_bom_df = pd.DataFrame()
            
    else:
        print(f"Assembly directory not found: {assembly_active_directory}")
        master_bom_df = pd.DataFrame()

    return master_bom_df

def add_overage_to_master_bom(master_bom_df, QUOTE_DIR):
    if not master_bom_df.empty:
        print("Adding purchasing overage to master BOM...")

        # Create a copy to avoid modifying the original
        master_bom_with_overage = master_bom_df.copy()
        
        # Get unique Quote# values from the BOM
        unique_quotes = master_bom_df['Quote#'].dropna().unique()
        print(f"Found {len(unique_quotes)} unique quotes to process")
        
        # Pre-load all purchasing data to avoid repeated file I/O
        purchasing_data_cache = {}

        total_dirs = len(unique_quotes)
        processed_count = 0
        
        for quote_num in unique_quotes:
            processed_count += 1

            if quote_num:  # Skip empty quote numbers
                quote_dir_path = os.path.join(QUOTE_DIR, quote_num)
                
                if os.path.exists(quote_dir_path) and os.path.isdir(quote_dir_path):
                    purchasing_dir = os.path.join(quote_dir_path, "purchasing")
                    
                    if os.path.exists(purchasing_dir) and os.path.isdir(purchasing_dir):
                        try:
                            # Find Excel files in the purchasing directory
                            excel_files = [f for f in os.listdir(purchasing_dir) 
                                         if f.endswith(('.xlsx', '.xls'))]
                            
                            if excel_files:
                                # Use the first Excel file found
                                excel_file = excel_files[0]
                                excel_path = os.path.join(purchasing_dir, excel_file)
                                
                                try:
                                    # Read the Excel file
                                    purchasing_df = pd.read_excel(excel_path)
                                    
                                    # Check if required columns exist
                                    if 'MPN' in purchasing_df.columns and 'Buy Quantity' in purchasing_df.columns:
                                        # Cache the lookup dictionary
                                        purchasing_data_cache[quote_num] = dict(zip(purchasing_df['MPN'], purchasing_df['Buy Quantity']))
                                        
                                except Exception as e:
                                    print(f"Error reading Excel file {excel_path}: {e}")
                        except Exception as e:
                            print(f"Error accessing purchasing directory {purchasing_dir}: {e}")

            # Show progress
            blue_gradient_bar(processed_count + 1, total_dirs)
        
        # Newline after progress bar
        print()
        print(f"Processed {processed_count} directories with stdBOM files")
        
        # Now update the DataFrame using vectorized operations where possible
        updates_made = 0
        processed_quotes = len(purchasing_data_cache)
        processed_count = 0
        
        for quote_num, mpn_to_buy_qty in purchasing_data_cache.items():
            processed_count += 1
            # Get all rows for this quote
            quote_mask = master_bom_with_overage['Quote#'] == quote_num
            
            # Update quantities for matching MPNs
            for mpn, buy_qty in mpn_to_buy_qty.items():
                mpn_mask = master_bom_with_overage['MPN'] == mpn
                combined_mask = quote_mask & mpn_mask
                
                if combined_mask.any():
                    master_bom_with_overage.loc[combined_mask, 'Req_Qty'] = buy_qty
                    updates_made += combined_mask.sum()

            # Show progress
            blue_gradient_bar(processed_count + 1, processed_quotes)
        
        # Newline after progress bar
        print()        
        print(f"Processed {processed_quotes} quotes with purchasing data")
        print(f"Made {updates_made} quantity updates to BOM")
        
        # Save updated BOM as CSV file (faster than Excel)
        updated_csv_path = 'SaveFiles/master_BOM.csv'
        master_bom_with_overage.to_csv(updated_csv_path, index=False)
        print(f"Updated master BOM saved to: {updated_csv_path}")
            
    else:
        print("No master BOM data available to update")

    return master_bom_with_overage

def missing_purchase_parts_file(master_bom_df, LOG_MISSING_PURCH_PARTS, debug_output=False):
    """
    Analyzes the master BOM DataFrame to identify missing purchase parts and saves the results to a JSON file.
    
    This function filters the master BOM data to find parts that meet the following criteria:
    - MPN is not "PCB" or "STENCIL"
    - Part is not customer supplied (Cust_Supplied is false/no/empty)
    - Received quantity is less than required quantity
    
    For each missing part found, extracts the first designator and creates a record with
    essential tracking information.

    Args:
        master_bom_df (pd.DataFrame): DataFrame containing master BOM data with columns including
            'MPN', 'Cust_Supplied', 'Req_Qty', 'Recvd_Qty', 'WO#', 'Quote#', 'Designators'
        LOG_MISSING_PURCH_PARTS (str): File path where the missing purchase parts JSON log should be saved
        debug_output (bool, optional): Whether to display sample missing parts data for debugging.
            Defaults to False.

    Returns:
        None: Function saves results to JSON file and prints status messages. Does not return data.
        
    Side Effects:
        - Creates/overwrites the JSON file at LOG_MISSING_PURCH_PARTS path
        - Prints progress bar during processing
        - Prints summary statistics and status messages
        - If debug_output=True, prints sample records
        
    Notes:
        - Uses extract_first_designator() helper function to parse designator strings
        - Handles numeric conversion errors gracefully by defaulting to 0
        - Creates empty file if master_bom_df is empty
        - Progress is displayed using blue_gradient_bar() function
    """
    if not master_bom_df.empty:
        print("Building missing purchase parts file...")
        
        # Filter the master BOM for missing purchase parts
        missing_parts_data = []

        total_count = len(master_bom_df)
        counter = 0
        
        for idx, row in master_bom_df.iterrows():
            counter += 1
            mpn = row.get('MPN', '')
            cust_supplied = row.get('Cust_Supplied', '').lower()
            req_qty = row.get('Req_Qty', '')
            recvd_qty = row.get('Recvd_Qty', '')
            
            # Convert quantities to numeric for comparison, default to 0 if conversion fails
            try:
                req_qty_num = float(req_qty) if req_qty else 0
            except (ValueError, TypeError):
                req_qty_num = 0
                
            try:
                recvd_qty_num = float(recvd_qty) if recvd_qty else 0
            except (ValueError, TypeError):
                recvd_qty_num = 0
            
            # Check conditions: MPN not PCB/Stencil, not customer supplied, received qty < required qty
            if (mpn and 
                mpn.upper() not in ['PCB', 'STENCIL'] and 
                cust_supplied in ['false', 'no', ''] and 
                recvd_qty_num < req_qty_num):
                
                # Extract first designator from comma-separated list
                designators = row.get('Designators', '')
                first_designator = extract_first_designator(designators)
                
                # Create missing parts record
                missing_part = {
                    'WO#': row.get('WO#', ''),
                    'Quote#': row.get('Quote#', ''),
                    'MPN': mpn,
                    'Designator': first_designator,
                    'Req_Qty': req_qty,
                    'Recvd_Qty': recvd_qty
                }
                
                missing_parts_data.append(missing_part)

            # Show progress bar
            blue_gradient_bar(counter, total_count)
        # Newline after progress bar
        print()
        print()
        
        print(f"Found {len(missing_parts_data)} missing purchase parts")
        
        # Save to JSON file
        if save_json_file(missing_parts_data, LOG_MISSING_PURCH_PARTS):
            print(f"✓ Missing purchase parts saved: {LOG_MISSING_PURCH_PARTS}")
        else:
            print("✗ Failed to save missing purchase parts file")
            
        # Show sample data if debug enabled
        if debug_output and missing_parts_data:
            print("\nSample missing purchase parts:")
            for i, part in enumerate(missing_parts_data[:3]):
                print(f"  {i+1}: {part}")
                
    else:
        print("No master BOM data available to process missing purchase parts")
        # Create empty file
        save_json_file([], LOG_MISSING_PURCH_PARTS)

def missing_purchase_parts_designator_file(master_bom_df, LOG_MISSING_PURCH_PARTS_DESIGNATOR, debug_output=False):
    pass