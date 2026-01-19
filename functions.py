from operator import index
from cryptography.fernet import Fernet
import pandas as pd
import os
import sys
import json
from datetime import datetime
from dateutil import parser
import random
import numpy as np
import smartsheet
import logging
import traceback

from defines import *
from local_secrets import PASSWORD_FILE_PATH, ENCRYPTED_KEY_PATH, SQL_PASSWORD_PATH, SQL_PASSWORD_KEY_PATH

# Get logger for this module
logger = logging.getLogger(__name__)

def sanitize_cam_data(df: pd.DataFrame) -> tuple:
    """
    Sanitizes camReadme data by validating and correcting field types.
    Handles cases where users enter strings in numeric/date fields.
    
    Args:
        df (pd.DataFrame): Raw DataFrame from camReadme.txt files
        
    Returns:
        tuple: (sanitized_df, corrections_list) where corrections_list contains
               details of all corrections made
    """
    if df.empty:
        logger.warning("Cannot sanitize empty DataFrame")
        return df, []
    
    corrections = []
    logger.info(f"Starting data sanitization for {len(df)} records...")
    
    # Sanitize missing WO# fields with unique placeholder values
    missing_wo_counter = 1
    if 'WO#' in df.columns:
        for idx in df.index:
            wo_value = df.at[idx, 'WO#']
            if wo_value is None or (isinstance(wo_value, str) and wo_value.strip() == '') or pd.isna(wo_value):
                # Generate unique placeholder WO#
                placeholder_wo = f"99999_{missing_wo_counter:02d}"
                customer = df.at[idx, 'Customer'] if 'Customer' in df.columns else 'Unknown'
                
                correction = {
                    'Record': idx,
                    'WO#': placeholder_wo,
                    'Customer': customer,
                    'Field': 'WO#',
                    'Original_Value': 'MISSING',
                    'Corrected_Value': placeholder_wo,
                    'Type': 'Missing Field'
                }
                corrections.append(correction)
                
                df.at[idx, 'WO#'] = placeholder_wo
                missing_wo_counter += 1
                
                logger.warning(
                    f"Corrected missing WO#: Record {idx} | Customer {customer} | "
                    f"Set to '{placeholder_wo}'"
                )
        
        if missing_wo_counter > 1:
            total_missing = missing_wo_counter - 1
            logger.warning(f"Replaced {total_missing} missing WO# field(s) with placeholder values (99999_01, etc.)")
            print(f"⚠ WARNING: {total_missing} records had missing WO# - replaced with placeholder values (99999_01, 99999_02, etc.)")
    
    # Sanitize numeric fields
    for field in NUMERIC_FIELDS:
        if field not in df.columns:
            continue
            
        for idx in df.index:
            original_value = df.at[idx, field]
            
            # Skip if already None/NaN or empty
            if pd.isna(original_value) or original_value == '':
                continue
            
            # Try to convert to numeric
            try:
                # If it's already numeric, skip
                if isinstance(original_value, (int, float)):
                    continue
                    
                # Try to convert string to numeric
                numeric_value = pd.to_numeric(original_value, errors='raise')
                df.at[idx, field] = numeric_value
                
            except (ValueError, TypeError):
                # Invalid numeric value - set to default
                wo_number = df.at[idx, 'WO#'] if 'WO#' in df.columns else 'Unknown'
                customer = df.at[idx, 'Customer'] if 'Customer' in df.columns else 'Unknown'
                
                correction = {
                    'Record': idx,
                    'WO#': wo_number,
                    'Customer': customer,
                    'Field': field,
                    'Original_Value': str(original_value),
                    'Corrected_Value': DEFAULT_NUMERIC_VALUE,
                    'Type': 'Numeric'
                }
                corrections.append(correction)
                
                df.at[idx, field] = DEFAULT_NUMERIC_VALUE
                
                logger.warning(
                    f"Corrected numeric field: Record {idx} | WO# {wo_number} | "
                    f"Field '{field}': '{original_value}' → {DEFAULT_NUMERIC_VALUE}"
                )
    
    # Sanitize date fields
    for field in DATE_FIELDS:
        if field not in df.columns:
            continue
            
        for idx in df.index:
            original_value = df.at[idx, field]
            
            # Skip if already None/NaN or empty
            if pd.isna(original_value) or original_value == '':
                continue
            
            # Try to parse as date
            try:
                # Try pandas to_datetime
                parsed_date = pd.to_datetime(original_value, errors='raise')
                # If successful, keep the original value
                continue
                
            except (ValueError, TypeError, parser.ParserError):
                # Invalid date value - set to default
                wo_number = df.at[idx, 'WO#'] if 'WO#' in df.columns else 'Unknown'
                customer = df.at[idx, 'Customer'] if 'Customer' in df.columns else 'Unknown'
                
                correction = {
                    'Record': idx,
                    'WO#': wo_number,
                    'Customer': customer,
                    'Field': field,
                    'Original_Value': str(original_value),
                    'Corrected_Value': DEFAULT_DATE_VALUE,
                    'Type': 'Date'
                }
                corrections.append(correction)
                
                df.at[idx, field] = DEFAULT_DATE_VALUE
                
                logger.warning(
                    f"Corrected date field: Record {idx} | WO# {wo_number} | "
                    f"Field '{field}': '{original_value}' → {DEFAULT_DATE_VALUE}"
                )
    
    if corrections:
        logger.warning(f"\n{'='*70}")
        logger.warning(f"DATA SANITIZATION SUMMARY: {len(corrections)} corrections made")
        logger.warning(f"{'='*70}")
        
        # Group corrections by WO#
        wo_corrections = {}
        for corr in corrections:
            wo = corr['WO#']
            if wo not in wo_corrections:
                wo_corrections[wo] = []
            wo_corrections[wo].append(corr)
        
        # Log summary by WO#
        for wo, corr_list in wo_corrections.items():
            customer = corr_list[0]['Customer']
            logger.warning(f"\n  WO# {wo} | Customer: {customer}")
            for corr in corr_list:
                logger.warning(
                    f"    Field: {corr['Field']:15} '{corr['Original_Value']}' → {corr['Corrected_Value']}"
                )
        
        logger.warning(f"{'='*70}\n")
        
        # Also print to console
        print(f"\n⚠ WARNING: Found {len(corrections)} invalid data entries that were corrected:")
        for wo, corr_list in wo_corrections.items():
            customer = corr_list[0]['Customer']
            print(f"  WO# {wo} | {customer} - {len(corr_list)} field(s) corrected")
        print(f"  See {ERROR_LOG_PATH} for details\n")
    else:
        logger.info("Data sanitization complete - no corrections needed")
    
    return df, corrections

def safe_float(value, default=0.0):
    """
    Safely convert a value to float, returning default if conversion fails.
    
    Args:
        value: Value to convert (can be string, numeric, or None)
        default: Default value to return if conversion fails (default: 0.0)
        
    Returns:
        float: Converted value or default
    """
    if value is None or value == '' or pd.isna(value):
        return default
    
    try:
        return float(value)
    except (ValueError, TypeError):
        logger.debug(f"Could not convert '{value}' to float, using default {default}")
        return default

def safe_int(value, default=0):
    """
    Safely convert a value to int, returning default if conversion fails.
    
    Args:
        value: Value to convert (can be string, numeric, or None)
        default: Default value to return if conversion fails (default: 0)
        
    Returns:
        int: Converted value or default
    """
    if value is None or value == '' or pd.isna(value):
        return default
    
    try:
        return int(float(value))  # Convert through float first to handle "5.0" strings
    except (ValueError, TypeError):
        logger.debug(f"Could not convert '{value}' to int, using default {default}")
        return default

def blue_gradient_bar(progress, total, end_color=None, use_color=False):
    """
    Prints a progress bar to the terminal with optional color gradient.
    
    Args:
        progress (int): Current progress value
        total (int): Total steps/maximum progress value
        end_color (tuple, optional): RGB tuple for end color. If None, a random vibrant color is chosen.
        use_color (bool, optional): Whether to use colored gradient bar. If False, uses basic ASCII progress bar.
                                   Defaults to True.
    
    Returns:
        None: Prints progress bar directly to terminal
    """
    if use_color:
        # Colored gradient bar (original functionality)
        # Light blue (start): RGB(173, 216, 230)
        start_rgb = (173, 216, 230)
        
        # If no end color specified, generate a random vibrant color
        if end_color is None:
            # Generate random vibrant colors by ensuring at least one RGB component is high
            end_rgb = random.choice(color_options)
        else:
            end_rgb = end_color

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
    else:
        # Basic ASCII progress bar (no color)
        filled_len = int(bar_len * progress // total) if total else bar_len
        bar = "=" * filled_len + "-" * (bar_len - filled_len)
        percent = int((progress / total) * 100) if total else 100
        sys.stdout.write(f"\r[{bar}] {progress}/{total} processed, {percent}%")
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
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        logger.debug(f"Reading password file: {PASSWORD_FILE_PATH}")
        with open(PASSWORD_FILE_PATH, "r") as f:
            key = f.read().strip().encode()  # Read and encode the key
        logger.debug("Password file read successfully")
        
        fernet = Fernet(key)
        
        logger.debug(f"Reading encrypted key file: {ENCRYPTED_KEY_PATH}")
        with open(ENCRYPTED_KEY_PATH, "rb") as f:
            encrypted = f.read()
        logger.debug("Encrypted key file read successfully")
        
        decrypted = fernet.decrypt(encrypted).decode()
        logger.debug("API key decrypted successfully")
        return decrypted
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        raise
    except Exception as e:
        logger.error(f"Error decrypting API key: {e}")
        raise

def get_sql_password():
    """
    Reads an encryption key from a text file and uses it to decrypt the SQL database password.

    Args:
        None

    Returns:
        str: The decrypted SQL password as a string, used for authenticating with the MS SQL database.

    Raises:
        FileNotFoundError: If the SQL password key or encrypted password file is missing.
        Exception: If decryption fails.
    """
    with open(SQL_PASSWORD_KEY_PATH, "r") as f:
        key = f.read().strip().encode()  # Read and encode the key
    fernet = Fernet(key)
    with open(SQL_PASSWORD_PATH, "rb") as f:
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
        Includes '_row_id' column with Smartsheet row IDs for tracking purposes.
    """
    try:
        logger.debug(f"Converting Smartsheet with {len(sheet.rows)} rows and {len(sheet.columns)} columns")
        
        data = []
        columns = [col.title for col in sheet.columns]
        total_rows = len(sheet.rows)
        
        logger.debug(f"Column names: {columns}")
        
        for idx, row in enumerate(sheet.rows):
            row_data = {'_row_id': row.id}  # Add Smartsheet row ID
            for cell in row.cells:
                if cell.column_id in [col.id for col in sheet.columns]:
                    col_index = [col.id for col in sheet.columns].index(cell.column_id)
                    row_data[columns[col_index]] = cell.value

            data.append(row_data)

            # Show progress bar
            blue_gradient_bar(idx + 1, total_rows, color_options[2])  # Using Dark Blue as end color
        # Newline after progress bar
        print()
        print() 

        logger.debug(f"Extracted {len(data)} rows of data")
        
        # Add '_row_id' to columns for DataFrame
        df = pd.DataFrame(data, columns=['_row_id'] + columns)
        logger.debug(f"DataFrame created with shape: {df.shape}")
        return df
        
    except Exception as e:
        logger.error(f"Error converting Smartsheet to DataFrame: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

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
        if os.path.isfile(file_path):
            if os.path.getsize(file_path) > 0:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                print(f"⚠ File is empty: {file_path}")
                return default_value if default_value is not None else []
        else:
            print(f"⚠ File not found, creating: {file_path}")
            # Create the directory if it doesn't exist
            directory = os.path.dirname(file_path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            
            # Create the file with default value
            default_data = default_value if default_value is not None else []
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(default_data, f, indent=2)
            
            return default_data
        
    except json.JSONDecodeError as e:
        print(f"⚠ Error: Corrupted JSON file {file_path}: {e}")
        return default_value if default_value is not None else []
    except FileNotFoundError:
        print(f"⚠ File not found: {file_path}")
        return default_value if default_value is not None else []
    except Exception as e:
        print(f"⚠ Unexpected error loading {file_path}: {e}")
        return default_value if default_value is not None else []
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
    try:
        logger.info(f"Loading assembly job data from: {network_dir}")
        data = []

        # Step 1: Load previous data if available
        if os.path.isfile(log_camData_path) and os.path.getsize(log_camData_path) > 0:
            try:
                with open(log_camData_path, "r") as f:
                    old_data = json.load(f)
                old_lookup = {entry["__file_path__"]: entry for entry in old_data}
                logger.debug(f"Loaded {len(old_lookup)} existing records from cache")
            except json.JSONDecodeError as je:
                logger.warning(f"JSON decode error in {log_camData_path}: {je}. Starting fresh.")
                old_lookup = {}
            except Exception as e:
                logger.warning(f"Error loading previous data: {e}. Starting fresh.")
                old_lookup = {}
        else:
            logger.debug("No existing cache found, will scan all files")
            old_lookup = {}

        # Verify network directory exists and is accessible
        if not os.path.exists(network_dir):
            logger.error(f"Network directory does not exist: {network_dir}")
            raise FileNotFoundError(f"Network directory not found: {network_dir}")
        
        try:
            dir_names = [entry_name for entry_name in os.listdir(network_dir) if os.path.isdir(os.path.join(network_dir, entry_name))]
        except PermissionError as pe:
            logger.error(f"Permission denied accessing network directory: {pe}")
            raise
        except Exception as e:
            logger.error(f"Error listing network directory: {e}")
            raise
        
        total_dirs = len(dir_names)
        logger.info(f"Found {total_dirs} directories to scan")
        
        files_read = 0
        files_cached = 0
        files_errors = 0

        for idx, entry_name in enumerate(dir_names):
            entry_path = os.path.join(network_dir, entry_name)
            camreadme_path = os.path.normpath(os.path.join(entry_path, "camReadme.txt"))

            if os.path.isfile(camreadme_path):
                try:
                    mtime = os.path.getmtime(camreadme_path)
                    # Step 3: Check if file is unchanged
                    if camreadme_path in old_lookup and old_lookup[camreadme_path].get("__file_mtime__") == mtime:
                        entry = old_lookup[camreadme_path]
                        data.append(entry)
                        files_cached += 1
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
                            files_read += 1

                        except Exception as e:
                            logger.error(f"Error reading {camreadme_path}: {e}")
                            files_errors += 1
                            if DEBUG:
                                logger.debug(f"Traceback: {traceback.format_exc()}")
                            continue
                except OSError as ose:
                    logger.error(f"OS error accessing {camreadme_path}: {ose}")
                    files_errors += 1
                    continue

            # Print color gradient progress bar
            blue_gradient_bar(idx + 1, total_dirs, color_options[1])
        # Newline after progress bar
        print()
        print() 

        logger.info(f"Scan complete: {files_read} files read, {files_cached} from cache, {files_errors} errors")
        
        # Return all camData file information
        df = pd.DataFrame(data)
        logger.info(f"Created DataFrame with {len(df)} records")
        return df
        
    except Exception as e:
        logger.error(f"Fatal error in load_assembly_job_data: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

def build_active_credithold_files(cam_data: list, existing_credit_holds: set) -> tuple:
    """
    Processes CAM data to separate active jobs from credit hold jobs and identify released credit holds.
    Cross-references with SQL database for accurate credit hold status.
    
    Args:
        cam_data (list): List of dictionaries containing CAM data records with job information
        existing_credit_holds (set): Set of WO# numbers that were previously on credit hold
        
    Returns:
        tuple: Three-element tuple containing:
            - active_jobs (list): Jobs not excluded and not on credit hold
            - credit_hold_jobs (list): Jobs currently on credit hold with tracking date
            - credit_hold_released (list): Jobs released from credit hold with release date
    """
    from database_utils import execute_custom_query
    
    active_jobs = []
    credit_hold_jobs = []
    credit_hold_released = []
    
    # Step 1: Extract all order numbers and create mapping
    print("Extracting order numbers from CAM data...")
    wo_to_order_map = {}  # Maps WO# to 5-digit order number
    
    for job in cam_data:
        wo_number = str(job.get('WO#', ''))
        
        # Extract 5-digit order number from WO# (remove everything AFTER and including first underscore)
        if '_' in wo_number:
            order_no = wo_number.split('_', 1)[0]  # Get everything BEFORE first underscore
        else:
            order_no = wo_number
        
        # Remove any non-numeric characters and ensure it's 5 digits
        order_no = ''.join(filter(str.isdigit, order_no))
        
        if len(order_no) >= 5:
            order_no = order_no[:5]  # Take first 5 digits
            wo_to_order_map[wo_number] = order_no
    
    print(f"✓ Extracted {len(wo_to_order_map)} order numbers")
    
    # Step 2: Batch query database for all credit holds at once
    print("Querying SQL database for credit hold status (batch query)...")
    db_credit_holds = set()
    
    if wo_to_order_map:
        try:
            # Get unique order numbers for the query
            unique_order_nos = list(set(wo_to_order_map.values()))

            # DEBUG: Save unique order numbers to file
            debug_order_nos_path = 'SaveFiles/debug_order_numbers.txt'
            with open(debug_order_nos_path, 'w') as f:
                f.write("Unique Order Numbers for Query:\n")
                f.write("=" * 50 + "\n\n")
                for order_no in sorted(unique_order_nos):
                    f.write(f"{order_no}\n")
                f.write("\n" + "=" * 50 + "\n")
                f.write(f"Total unique order numbers: {len(unique_order_nos)}\n")
            print(f"  DEBUG: Saved order numbers to {debug_order_nos_path}")
            
            # Build parameterized query with IN clause
            placeholders = ','.join(['?' for _ in unique_order_nos])
            query = f"""
            SELECT [order_no], [credit_hold]
            FROM [advcircuits].[dbo].[R4Order]
            WHERE [ar_entity] = 'AC' AND [order_no] IN ({placeholders})
            """

             # DEBUG: Save query with placeholders to file
            debug_query_path = 'SaveFiles/debug_query.txt'
            with open(debug_query_path, 'w') as f:
                f.write("SQL Query with Placeholders:\n")
                f.write("=" * 50 + "\n\n")
                f.write(query)
                f.write("\n\n" + "=" * 50 + "\n")
                f.write(f"Number of placeholders: {len(unique_order_nos)}\n")
                f.write(f"Parameters tuple length: {len(unique_order_nos)}\n")
            print(f"  DEBUG: Saved query to {debug_query_path}")
            
            # Execute single batch query
            results = execute_custom_query(query, tuple(unique_order_nos))
            
            print(f"  DEBUG: Query returned {len(results)} results")

            # DEBUG: Save query results to file
            debug_results_path = 'SaveFiles/debug_query_results.txt'
            with open(debug_results_path, 'w') as f:
                f.write("SQL Query Results:\n")
                f.write("=" * 50 + "\n\n")
                f.write(f"Total results: {len(results)}\n\n")
                
                if results:
                    f.write("Sample results (first 10):\n")
                    for i, row in enumerate(results[:10]):
                        f.write(f"\nResult {i+1}:\n")
                        f.write(f"  order_no: {row['order_no']} (type: {type(row['order_no'])})\n")
                        f.write(f"  credit_hold: {row['credit_hold']} (type: {type(row['credit_hold'])})\n")
                    
                    f.write("\n" + "=" * 50 + "\n")
                    f.write("All results:\n\n")
                    for row in results:
                        f.write(f"order_no: {row['order_no']}, credit_hold: {row['credit_hold']}\n")
                    
                    # Count credit holds
                    credit_hold_count = sum(1 for row in results if row['credit_hold'] == 1)
                    f.write("\n" + "=" * 50 + "\n")
                    f.write(f"Credit hold count: {credit_hold_count}\n")
                else:
                    f.write("No results returned from query\n")
            
            # Create lookup dictionary of order_no to credit_hold status
            # Handle both string and integer order numbers from database
            order_credit_hold = {}
            for row in results:
                # Convert order_no to string and strip any whitespace/padding
                order_key = str(row['order_no']).strip()
                order_credit_hold[order_key] = row['credit_hold']
            
            print(f"  DEBUG: Sample DB results: {list(order_credit_hold.items())[:5]}")
            print(f"  DEBUG: Sample WO mappings: {list(wo_to_order_map.items())[:5]}")
            
            # Map back to WO# numbers
            for wo_number, order_no in wo_to_order_map.items():
                # Ensure we're comparing strings
                order_no_str = str(order_no).strip()
                
                if order_no_str in order_credit_hold:
                    if order_credit_hold[order_no_str] == 1:
                        db_credit_holds.add(wo_number)
                        print(f"  DEBUG: Found credit hold in DB for {wo_number} (order {order_no_str})")
            
            # DEBUG: Save WO to order mapping
            debug_mapping_path = 'SaveFiles/debug_wo_to_order_mapping.txt'
            with open(debug_mapping_path, 'w') as f:
                f.write("WO# to Order Number Mapping:\n")
                f.write("=" * 50 + "\n\n")
                for wo, order in sorted(wo_to_order_map.items()):
                    credit_status = order_credit_hold.get(order, 'NOT FOUND')
                    f.write(f"WO#: {wo} -> Order: {order} -> Credit Hold: {credit_status}\n")
                f.write("\n" + "=" * 50 + "\n")
                f.write(f"Total mappings: {len(wo_to_order_map)}\n")
            print(f"  DEBUG: Saved WO mappings to {debug_mapping_path}")
            
            # DEBUG: Save credit holds found
            debug_credit_holds_path = 'SaveFiles/debug_credit_holds_found.txt'
            with open(debug_credit_holds_path, 'w') as f:
                f.write("Credit Holds Found in Database:\n")
                f.write("=" * 50 + "\n\n")
                for wo in sorted(db_credit_holds):
                    order = wo_to_order_map.get(wo, 'UNKNOWN')
                    f.write(f"WO#: {wo} (Order: {order})\n")
                f.write("\n" + "=" * 50 + "\n")
                f.write(f"Total credit holds found: {len(db_credit_holds)}\n")
            print(f"  DEBUG: Saved credit holds to {debug_credit_holds_path}")
            
            print(f"✓ Database query complete: {len(db_credit_holds)} credit holds found")
            
        except Exception as e:
            print(f"⚠ Error querying database: {e}")
            import traceback
            traceback.print_exc()
            print("  Continuing with CAM data only...")
    
    # Step 3: Extract credit holds from CAM data
    print("Processing CAM data for credit hold status...")
    cam_credit_holds = set()
    
    for job in cam_data:
        wo_number = str(job.get('WO#', ''))
        credit_hold = job.get('Credit Hold', '')
        
        # Handle None values safely
        if credit_hold is None:
            credit_hold = ''
        else:
            credit_hold = str(credit_hold).strip().upper()
        
        if credit_hold == 'YES':
            cam_credit_holds.add(wo_number)
    
    print(f"✓ CAM data processing complete: {len(cam_credit_holds)} credit holds found")
    
    # Step 4: Compare and identify discrepancies
    db_only = db_credit_holds - cam_credit_holds  # In database but not in CAM
    cam_only = cam_credit_holds - db_credit_holds  # In CAM but not in database
    matching = db_credit_holds & cam_credit_holds  # In both
    
    discrepancies = []
    
    # Add database-only discrepancies
    for wo in db_only:
        discrepancies.append({
            'WO#': wo,
            'Source': 'Database Only',
            'DB_Credit_Hold': 'Yes',
            'CAM_Credit_Hold': 'No'
        })
    
    # Add CAM-only discrepancies
    for wo in cam_only:
        discrepancies.append({
            'WO#': wo,
            'Source': 'CAM Only',
            'DB_Credit_Hold': 'No',
            'CAM_Credit_Hold': 'Yes'
        })
    
    # Save discrepancies to CSV
    os.makedirs('SaveFiles', exist_ok=True)
    
    if discrepancies:
        discrepancy_df = pd.DataFrame(discrepancies)
        discrepancy_path = 'SaveFiles/credit_hold_discrepancies.csv'
        discrepancy_df.to_csv(discrepancy_path, index=False)
        print(f"⚠ Credit hold discrepancies found: {len(discrepancies)} records")
        print(f"  Discrepancy report saved: {discrepancy_path}")
        print(f"  - Database only: {len(db_only)}")
        print(f"  - CAM only: {len(cam_only)}")
    else:
        # Save empty discrepancy file with timestamp to show it was checked
        discrepancy_df = pd.DataFrame(columns=['WO#', 'Source', 'DB_Credit_Hold', 'CAM_Credit_Hold'])
        discrepancy_path = 'SaveFiles/credit_hold_discrepancies.csv'
        discrepancy_df.to_csv(discrepancy_path, index=False)
        
        # Also save a status file showing when the check was performed
        status_path = 'SaveFiles/credit_hold_check_status.txt'
        with open(status_path, 'w') as f:
            f.write("Credit Hold Discrepancy Check\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Last checked: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Database credit holds: {len(db_credit_holds)}\n")
            f.write(f"CAM credit holds: {len(cam_credit_holds)}\n")
            f.write(f"Matching credit holds: {len(matching)}\n")
            f.write(f"Discrepancies found: 0\n")
        
        print(f"✓ No discrepancies found between database and CAM data")
        print(f"  - Matching credit holds: {len(matching)}")
        print(f"  Empty discrepancy file saved: {discrepancy_path}")
        print(f"  Status file saved: {status_path}")
        
    # Step 5: Combine all credit holds (union of both sources)
    all_credit_holds = db_credit_holds | cam_credit_holds
    
    print(f"Processing {len(all_credit_holds)} total credit hold jobs...")
    
    # Step 6: Process jobs using combined credit hold list
    for job in cam_data:
        wo_number = str(job.get('WO#', ''))
        status = job.get('Status', '')
        
        # Handle None values safely for status
        if status is None:
            status = ''
        else:
            status = str(status).strip().upper()
        
        # Skip excluded statuses
        if status in excluded_statuses:
            continue
        
        # Check if job is on credit hold (from either source)
        if wo_number in all_credit_holds:
            # Add timestamp to track when credit hold was detected
            job_with_timestamp = job.copy()
            job_with_timestamp['Credit_Hold_Date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            credit_hold_jobs.append(job_with_timestamp)
            
        else:
            # Job is active (not on credit hold and not excluded)
            active_jobs.append(job)
            
            # Check if this job was previously on credit hold
            if wo_number in existing_credit_holds:
                release_record = job.copy()
                release_record['Credit_Hold_Released_Date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                credit_hold_released.append(release_record)
    
    print(f"✓ Job processing complete:")
    print(f"  - Active jobs: {len(active_jobs)}")
    print(f"  - Credit hold jobs: {len(credit_hold_jobs)}")
    print(f"  - Released from credit hold: {len(credit_hold_released)}")
    
    return active_jobs, credit_hold_jobs, credit_hold_released

def store_smartsheet_user_data(smartsheet_part_tracking_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts user-entered data from specified columns in the Smartsheet DataFrame.
    Stores data in a json log file with row ID and WO# for tracking user modifications.
    Also creates a timestamped CSV backup of the entire DataFrame and manages old backups.

    Args:
        smartsheet_part_tracking_df (pd.DataFrame): DataFrame containing Smartsheet data, 
            including a '_row_id' column for row identification.

    Returns:
        None: Saves user data to 'SaveFiles/log_user_entered_data.json' and prints status.

    None: Saves user data to 'SaveFiles/log_user_entered_data.json', creates CSV backup,
            and prints status. Deletes backups older than 15 days.
    """
    try:
        # Create Backups directory if it doesn't exist
        backups_dir = 'SaveFiles/Backups'
        os.makedirs(backups_dir, exist_ok=True)
        
        # Create timestamped backup of entire DataFrame
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_filename = f'smartsheet_backup_{timestamp}.csv'
        backup_filepath = os.path.join(backups_dir, backup_filename)
        
        if not smartsheet_part_tracking_df.empty:
            smartsheet_part_tracking_df.to_csv(backup_filepath, index=False)
            print(f"✓ Smartsheet backup saved: {backup_filepath}")
        
        # Delete backups older than 15 days
        current_time = datetime.now()
        deleted_count = 0
        
        try:
            for filename in os.listdir(backups_dir):
                if filename.startswith('smartsheet_backup_') and filename.endswith('.csv'):
                    file_path = os.path.join(backups_dir, filename)
                    file_modified_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                    
                    # Calculate age in days
                    age_days = (current_time - file_modified_time).days
                    
                    if age_days > 15:
                        os.remove(file_path)
                        deleted_count += 1
                        print(f"  Deleted old backup: {filename} (age: {age_days} days)")
            
            if deleted_count > 0:
                print(f"✓ Deleted {deleted_count} backup(s) older than 15 days")
                
        except Exception as e:
            print(f"⚠ Warning: Error cleaning old backups: {e}")
        
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
                blue_gradient_bar(index + 1, total_rows, color_options[7])
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
        designators_string (str): String containing designators (e.g., "C1,C2,C3" or "R1-R10")
        
    Returns:
        str: First designator found, stripped of whitespace. Returns empty string if input is None/empty.
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
    """
    Converts various date string formats to YYYY-MM-DD format compatible with smartsheet.
    
    Args:
        date_str (str): Date string in various formats to be parsed and standardized
        
    Returns:
        str: Date in YYYY-MM-DD format, or original string if parsing fails, 
             or empty string if input is None/null
    """
    if not date_str or str(date_str).lower() in ['none', 'null', '']:
        return ""
    try:
        dt = parser.parse(str(date_str), fuzzy=True)
        # return dt.strftime("%m-%d-%y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return str(date_str)  # fallback to original if parsing fails

def build_master_bom(jobs_df: pd.DataFrame, assembly_active_directory: str, debug_output: bool = False) -> pd.DataFrame:
    """
    Scans assembly directories for stdBOM files and builds a master BOM DataFrame from active jobs.
    
    Args:
        jobs_df (pd.DataFrame): DataFrame containing active job data with WO# and Quote# columns
        assembly_active_directory (str): Path to directory containing assembly job folders
        debug_output (bool, optional): Whether to display debug information. Defaults to False.
        
    Returns:
        pd.DataFrame: Master BOM DataFrame with all parts from active jobs' stdBOM files.
            Includes columns for WO#, Quote#, Part_Number, MPN, Description, quantities, etc.
            Saves CSV to 'SaveFiles/master_BOM_no_overage.csv'
    """
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
            blue_gradient_bar(idx + 1, total_dirs, color_options[8])
        
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

def add_overage_to_master_bom(master_bom_df, QUOTE_DIR, apply_overage=True):
    """
    Updates master BOM quantities with purchasing overage data from quote directories.
    
    Args:
        master_bom_df (pd.DataFrame): Master BOM DataFrame to update with overage quantities
        QUOTE_DIR (str): Path to quotes directory containing purchasing subdirectories with Excel files
        apply_overage (bool, optional): Whether to apply overage quantities from purchasing files. 
                                      If False, saves master BOM without overage processing. Defaults to True.
        
    Returns:
        pd.DataFrame: Updated master BOM DataFrame with purchasing quantities applied (if apply_overage=True).
                      Saves updated CSV to 'SaveFiles/master_BOM.csv'
    """
    if not master_bom_df.empty:
        # Create a copy to avoid modifying the original
        master_bom_with_overage = master_bom_df.copy()
        
        if apply_overage:
            print("Adding purchasing overage to master BOM...")
            
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
                blue_gradient_bar(processed_count + 1, total_dirs, color_options[9])
            
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
                blue_gradient_bar(processed_count + 1, processed_quotes, color_options[10])
            
            # Newline after progress bar
            print()        
            print(f"Processed {processed_quotes} quotes with purchasing data")
            print(f"Made {updates_made} quantity updates to BOM")
        else:
            print("Skipping overage processing (apply_overage=False)")
        
        # Save updated BOM as CSV file (faster than Excel)
        updated_csv_path = 'SaveFiles/master_BOM.csv'
        master_bom_with_overage.to_csv(updated_csv_path, index=False)
        
        if apply_overage:
            print(f"Master BOM with overage saved to: {updated_csv_path}")
        else:
            print(f"Master BOM (no overage applied) saved to: {updated_csv_path}")
            
    else:
        print("No master BOM data available to update")
        master_bom_with_overage = pd.DataFrame()

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
            blue_gradient_bar(counter, total_count, color_options[0])
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
    """
    Creates a summary file of designators for missing purchase parts, grouped by work order.
    
    Args:
        master_bom_df (pd.DataFrame): Master BOM DataFrame containing parts data
        LOG_MISSING_PURCH_PARTS_DESIGNATOR (str): File path for saving designator summary JSON
        debug_output (bool, optional): Whether to display sample data. Defaults to False.
        
    Returns:
        None: Saves designator summary to JSON file. Groups designators by WO# and uses 
              "many" if more than 10 designators per work order.
    """
    if not master_bom_df.empty:
        print("Building missing purchase parts designator file...")
        
        # Dictionary to collect designators by WO#
        wo_designators = {}
        
        for idx, row in master_bom_df.iterrows():
            mpn = row.get('MPN', '')
            cust_supplied = row.get('Cust_Supplied', '').lower()
            req_qty = row.get('Req_Qty', '')
            recvd_qty = row.get('Recvd_Qty', '')
            wo_number = row.get('WO#', '')
            
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
                recvd_qty_num < req_qty_num and
                wo_number):
                
                # Extract first designator
                designators = row.get('Designators', '')
                first_designator = extract_first_designator(designators)
                
                if first_designator:
                    # Initialize WO# entry if not exists
                    if wo_number not in wo_designators:
                        wo_designators[wo_number] = set()
                    
                    # Add designator to set (prevents duplicates)
                    wo_designators[wo_number].add(first_designator)
        
        # Convert to list format for JSON output
        designator_data = []
        for wo_number, designator_set in wo_designators.items():
            designator_list = sorted(list(designator_set))
            
            # If more than 10 designators, use "many"
            if len(designator_list) > 10:
                designator_string = "many"
            else:
                designator_string = ", ".join(designator_list)
            
            designator_data.append({
                'WO#': wo_number,
                'Designators': designator_string
            })
        
        # Sort by WO# for consistent output
        designator_data.sort(key=lambda x: x['WO#'])
        
        print(f"Found designators for {len(designator_data)} work orders")
        
        # Save to JSON file
        designator_path = LOG_MISSING_PURCH_PARTS_DESIGNATOR
        if save_json_file(designator_data, designator_path):
            print(f"✓ Purchase parts designators saved: {designator_path}")
        else:
            print("✗ Failed to save purchase parts designator file")
            
        # Show sample data if debug enabled
        if debug_output and designator_data:
            print("\nSample purchase parts designators:")
            for i, item in enumerate(designator_data[:3]):
                print(f"  {i+1}: WO#{item['WO#']} - {item['Designators']}")
                
    else:
        print("No master BOM data available to process purchase parts designators")
        # Create empty file
        save_json_file([], LOG_MISSING_PURCH_PARTS_DESIGNATOR)

def missing_cust_parts_file(master_bom_df, LOG_MISSING_CUST_PARTS, debug_output=False):
    """
    Identifies and saves missing customer-supplied parts from the master BOM.
    
    Args:
        master_bom_df (pd.DataFrame): Master BOM DataFrame to analyze
        LOG_MISSING_CUST_PARTS (str): File path for saving missing customer parts JSON
        debug_output (bool, optional): Whether to display sample data. Defaults to False.
        
    Returns:
        None: Saves missing customer parts data to JSON file. Includes parts where
              customer supplied is true and received quantity < required quantity.
    """
    if not master_bom_df.empty:
        print("Building missing customer parts file...")
        
        # Filter the master BOM for missing customer parts
        missing_customer_parts_data = []
        
        for idx, row in master_bom_df.iterrows():
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
            
            # Check conditions: MPN not PCB/Stencil, customer supplied is true, received qty < required qty
            if (mpn and 
                mpn.upper() not in ['PCB', 'STENCIL'] and 
                cust_supplied in ['true', 'yes'] and 
                recvd_qty_num < req_qty_num):
                
                # Extract first designator from comma-separated list
                designators = row.get('Designators', '')
                first_designator = extract_first_designator(designators)
                
                # Create missing customer parts record
                missing_customer_part = {
                    'WO#': row.get('WO#', ''),
                    'Quote#': row.get('Quote#', ''),
                    'MPN': mpn,
                    'Designator': first_designator,
                    'Req_Qty': req_qty,
                    'Recvd_Qty': recvd_qty
                }
                
                missing_customer_parts_data.append(missing_customer_part)
        
        print(f"Found {len(missing_customer_parts_data)} missing customer parts")
        
        # Save to JSON file
        missing_customer_parts_path = LOG_MISSING_CUST_PARTS
        if save_json_file(missing_customer_parts_data, missing_customer_parts_path):
            print(f"✓ Missing customer parts saved: {missing_customer_parts_path}")
        else:
            print("✗ Failed to save missing customer parts file")
            
        # Show sample data if debug enabled
        if debug_output and missing_customer_parts_data:
            print("\nSample missing customer parts:")
            for i, part in enumerate(missing_customer_parts_data[:3]):
                print(f"  {i+1}: {part}")
                
    else:
        print("No master BOM data available to process missing customer parts")
        # Create empty file
        save_json_file([], LOG_MISSING_CUST_PARTS)

def missing_cust_parts_designator_file(master_bom_df, LOG_MISSING_CUST_PARTS_DESIGNATOR, debug_output=False):
    """
    Creates a summary file of designators for missing customer parts, grouped by work order.
    
    Args:
        master_bom_df (pd.DataFrame): Master BOM DataFrame containing parts data
        LOG_MISSING_CUST_PARTS_DESIGNATOR (str): File path for saving customer designator summary JSON
        debug_output (bool, optional): Whether to display sample data. Defaults to False.
        
    Returns:
        None: Saves customer designator summary to JSON file. Groups designators by WO# 
              and uses "many" if more than 10 designators per work order.
    """
    if not master_bom_df.empty:
        print("Building missing customer parts designator file...")
        
        # Dictionary to collect designators by WO#
        wo_customer_designators = {}
        
        for idx, row in master_bom_df.iterrows():
            mpn = row.get('MPN', '')
            cust_supplied = row.get('Cust_Supplied', '').lower()
            req_qty = row.get('Req_Qty', '')
            recvd_qty = row.get('Recvd_Qty', '')
            wo_number = row.get('WO#', '')
            
            # Convert quantities to numeric for comparison, default to 0 if conversion fails
            try:
                req_qty_num = float(req_qty) if req_qty else 0
            except (ValueError, TypeError):
                req_qty_num = 0
                
            try:
                recvd_qty_num = float(recvd_qty) if recvd_qty else 0
            except (ValueError, TypeError):
                recvd_qty_num = 0
            
            # Check conditions: MPN not PCB/Stencil, customer supplied is true, received qty < required qty
            if (mpn and 
                mpn.upper() not in ['PCB', 'STENCIL'] and 
                cust_supplied in ['true', 'yes'] and 
                recvd_qty_num < req_qty_num and
                wo_number):
                
                # Extract first designator
                designators = row.get('Designators', '')
                first_designator = extract_first_designator(designators)
                
                if first_designator:
                    # Initialize WO# entry if not exists
                    if wo_number not in wo_customer_designators:
                        wo_customer_designators[wo_number] = set()
                    
                    # Add designator to set (prevents duplicates)
                    wo_customer_designators[wo_number].add(first_designator)
        
        # Convert to list format for JSON output
        customer_designator_data = []
        for wo_number, designator_set in wo_customer_designators.items():
            designator_list = sorted(list(designator_set))
            
            # If more than 10 designators, use "many"
            if len(designator_list) > 10:
                designator_string = "many"
            else:
                designator_string = ", ".join(designator_list)
            
            customer_designator_data.append({
                'WO#': wo_number,
                'Designators': designator_string
            })
        
        # Sort by WO# for consistent output
        customer_designator_data.sort(key=lambda x: x['WO#'])
        
        print(f"Found customer designators for {len(customer_designator_data)} work orders")
        
        # Save to JSON file
        customer_designator_path = LOG_MISSING_CUST_PARTS_DESIGNATOR
        if save_json_file(customer_designator_data, customer_designator_path):
            print(f"✓ Customer parts designators saved: {customer_designator_path}")
        else:
            print("✗ Failed to save customer parts designator file")
            
        # Show sample data if debug enabled
        if debug_output and customer_designator_data:
            print("\nSample customer parts designators:")
            for i, item in enumerate(customer_designator_data[:3]):
                print(f"  {i+1}: WO#{item['WO#']} - {item['Designators']}")
                
    else:
        print("No master BOM data available to process customer parts designators")
        # Create empty file
        save_json_file([], LOG_MISSING_CUST_PARTS_DESIGNATOR)

def missing_pcb_file(master_bom_df, LOG_PCB_STATUS, debug_output=False):
    """
    Analyzes master BOM to determine PCB completion status for each work order.
    
    Args:
        master_bom_df (pd.DataFrame): Master BOM DataFrame containing PCB status data
        LOG_PCB_STATUS (str): File path for saving PCB status JSON
        debug_output (bool, optional): Whether to display sample data. Defaults to False.
        
    Returns:
        None: Saves PCB status data to JSON file. Status is "Complete" if Date_Complete
              contains a valid date, otherwise "None".
    """
    if not master_bom_df.empty:
        print("Building PCB status file...")
        
        # Dictionary to collect PCB status by WO#
        wo_pcb_status = {}
        
        for idx, row in master_bom_df.iterrows():
            part_number = row.get('Part_Number', '').upper()
            wo_number = row.get('WO#', '')
            date_complete = row.get('Date_Complete', '')
            
            # Check if this is a PCB part
            if part_number == 'PCB' and wo_number:
                # Check if Date_Complete contains a date
                pcb_status = "None"  # Default status
                
                if date_complete and str(date_complete).strip() and str(date_complete).lower() not in ['null', 'none', '']:
                    # Try to parse as date to verify it's actually a date
                    try:
                        # Check if it looks like a date (contains numbers and date separators)
                        date_str = str(date_complete).strip()
                        if any(char.isdigit() for char in date_str) and any(sep in date_str for sep in ['-', '/', ':', ' ']):
                            # Attempt to parse the date
                            pd.to_datetime(date_str)
                            pcb_status = "Complete"
                    except (ValueError, TypeError):
                        # If parsing fails, keep as "None"
                        pcb_status = "None"
                
                # Store the status (overwrites if multiple PCB entries for same WO#)
                wo_pcb_status[wo_number] = pcb_status
        
        # Convert to list format for JSON output
        pcb_status_data = []
        for wo_number, status in wo_pcb_status.items():
            pcb_status_data.append({
                'WO#': wo_number,
                'Status': status
            })
        
        # Sort by WO# for consistent output
        pcb_status_data.sort(key=lambda x: x['WO#'])
        
        print(f"Found PCB status for {len(pcb_status_data)} work orders")
        
        # Count statuses for summary
        complete_count = sum(1 for item in pcb_status_data if item['Status'] == 'Complete')
        none_count = sum(1 for item in pcb_status_data if item['Status'] == 'None')
        print(f"PCB Complete: {complete_count}, PCB None: {none_count}")
        
        # Save to JSON file
        pcb_status_path = LOG_PCB_STATUS
        if save_json_file(pcb_status_data, pcb_status_path):
            print(f"✓ PCB status saved: {pcb_status_path}")
        else:
            print("✗ Failed to save PCB status file")
            
        # Show sample data if debug enabled
        if debug_output and pcb_status_data:
            print("\nSample PCB status:")
            for i, item in enumerate(pcb_status_data[:5]):
                print(f"  {i+1}: WO#{item['WO#']} - {item['Status']}")
                
    else:
        print("No master BOM data available to process PCB status")
        # Create empty file
        save_json_file([], LOG_PCB_STATUS)

def missing_stencil_file(master_bom_df, LOG_STENCIL_STATUS, debug_output=False):
    """
    Analyzes master BOM to determine stencil completion status for each work order.
    
    Args:
        master_bom_df (pd.DataFrame): Master BOM DataFrame containing stencil status data
        LOG_STENCIL_STATUS (str): File path for saving stencil status JSON
        debug_output (bool, optional): Whether to display sample data. Defaults to False.
        
    Returns:
        None: Saves stencil status data to JSON file. Status is "Complete" if Date_Complete
              contains a valid date, otherwise "None".
    """
    if not master_bom_df.empty:
        print("Building stencil status file...")
        
        # Dictionary to collect stencil status by WO#
        wo_stencil_status = {}
        
        for idx, row in master_bom_df.iterrows():
            part_number = row.get('Part_Number', '').upper()
            wo_number = row.get('WO#', '')
            date_complete = row.get('Date_Complete', '')
            
            # Check if this is a stencil part
            if part_number == 'STENCIL' and wo_number:
                # Check if Date_Complete contains a date
                stencil_status = "None"  # Default status
                
                if date_complete and str(date_complete).strip() and str(date_complete).lower() not in ['null', 'none', '']:
                    # Try to parse as date to verify it's actually a date
                    try:
                        # Check if it looks like a date (contains numbers and date separators)
                        date_str = str(date_complete).strip()
                        if any(char.isdigit() for char in date_str) and any(sep in date_str for sep in ['-', '/', ':', ' ']):
                            # Attempt to parse the date
                            pd.to_datetime(date_str)
                            stencil_status = "Complete"
                    except (ValueError, TypeError):
                        # If parsing fails, keep as "None"
                        stencil_status = "None"
                
                # Store the status (overwrites if multiple stencil entries for same WO#)
                wo_stencil_status[wo_number] = stencil_status
        
        # Convert to list format for JSON output
        stencil_status_data = []
        for wo_number, status in wo_stencil_status.items():
            stencil_status_data.append({
                'WO#': wo_number,
                'Status': status
            })
        
        # Sort by WO# for consistent output
        stencil_status_data.sort(key=lambda x: x['WO#'])
        
        print(f"Found stencil status for {len(stencil_status_data)} work orders")
        
        # Count statuses for summary
        complete_count = sum(1 for item in stencil_status_data if item['Status'] == 'Complete')
        none_count = sum(1 for item in stencil_status_data if item['Status'] == 'None')
        print(f"Stencil Complete: {complete_count}, Stencil None: {none_count}")
        
        # Save to JSON file
        stencil_status_path = LOG_STENCIL_STATUS
        if save_json_file(stencil_status_data, stencil_status_path):
            print(f"✓ Stencil status saved: {stencil_status_path}")
        else:
            print("✗ Failed to save stencil status file")
            
        # Show sample data if debug enabled
        if debug_output and stencil_status_data:
            print("\nSample stencil status:")
            for i, item in enumerate(stencil_status_data[:5]):
                print(f"  {i+1}: WO#{item['WO#']} - {item['Status']}")
                
    else:
        print("No master BOM data available to process stencil status")
        # Create empty file
        save_json_file([], LOG_STENCIL_STATUS)

def parts_po_file(active_jobs, ASSEMBLY_ACTIVE_DIRECTORY, LOG_PO_NUMBERS, debug_output=False):
    """
    Extracts PO numbers from R4_RECEIVING_BOM files for active jobs and saves to JSON.
    
    Args:
        active_jobs (list): List of active job dictionaries containing WO# information
        ASSEMBLY_ACTIVE_DIRECTORY (str): Path to directory containing assembly job folders
        LOG_PO_NUMBERS (str): File path for saving PO numbers JSON
        debug_output (bool, optional): Whether to display sample data. Defaults to False.
        
    Returns:
        None: Saves PO numbers data to JSON file. Each record contains WO# and 
              comma-separated string of PO numbers found in R4_RECEIVING_BOM files.
    """
    if active_jobs:
        print("Building PO numbers file...")
        
        # Dictionary to collect PO numbers by WO#
        wo_po_numbers = {}
        
        # Get all directories in ASSEMBLY_ACTIVE_DIRECTORY
        if os.path.exists(ASSEMBLY_ACTIVE_DIRECTORY):
            directories = [d for d in os.listdir(ASSEMBLY_ACTIVE_DIRECTORY) 
                          if os.path.isdir(os.path.join(ASSEMBLY_ACTIVE_DIRECTORY, d))]
            
            total_active_jobs = len(active_jobs)
            processed_count = 0
            
            for idx, job in enumerate(active_jobs):
                wo_number = job.get('WO#', '')
                
                if wo_number:
                    # Find directory that contains the WO# (with preceding character)
                    matching_dir = None
                    for directory in directories:
                        if wo_number in directory:
                            matching_dir = directory
                            break
                    
                    if matching_dir:
                        dir_path = os.path.join(ASSEMBLY_ACTIVE_DIRECTORY, matching_dir)
                        
                        try:
                            # Find R4_RECEIVING_BOM file in the directory
                            files = os.listdir(dir_path)
                            receiving_bom_files = [f for f in files if 'R4_RECEIVING_BOM' in f]
                            
                            if receiving_bom_files:
                                # Use the first R4_RECEIVING_BOM file found
                                receiving_bom_file = receiving_bom_files[0]
                                receiving_bom_path = os.path.join(dir_path, receiving_bom_file)
                                
                                # Parse the R4_RECEIVING_BOM file
                                po_numbers = set()  # Use set to store unique values
                                
                                try:
                                    with open(receiving_bom_path, 'r', encoding='utf-8', errors='ignore') as f:
                                        lines = f.readlines()
                                        
                                    for line in lines:
                                        line = line.strip()
                                        if line and '|' in line:
                                            # Split by pipe delimiter and get first value
                                            parts = line.split('|')
                                            if parts:
                                                first_value = parts[0].strip()
                                                
                                                # Try to convert to integer
                                                try:
                                                    po_number = int(first_value)
                                                    po_numbers.add(po_number)
                                                except (ValueError, TypeError):
                                                    # Not an integer, skip
                                                    continue
                                    
                                    # Store PO numbers for this WO# if any found
                                    if po_numbers:
                                        # Convert set to sorted list for consistent output
                                        po_list = sorted(list(po_numbers))
                                        wo_po_numbers[wo_number] = po_list
                                        processed_count += 1
                                        
                                except Exception as e:
                                    print(f"Error reading R4_RECEIVING_BOM file {receiving_bom_path}: {e}")
                                    continue
                                    
                        except Exception as e:
                            print(f"Error accessing directory {dir_path}: {e}")
                            continue
                
                # Show progress
                blue_gradient_bar(idx + 1, total_active_jobs, color_options[5])
            
            # Newline after progress bar
            print()
            print(f"Processed {processed_count} work orders with R4_RECEIVING_BOM files")
            
            # Convert to list format for JSON output
            po_numbers_data = []
            for wo_number, po_list in wo_po_numbers.items():
                # Create comma-separated string of PO numbers
                po_string = ", ".join(map(str, po_list))
                
                po_numbers_data.append({
                    'WO#': wo_number,
                    'PO_Numbers': po_string
                })
            
            # Sort by WO# for consistent output
            po_numbers_data.sort(key=lambda x: x['WO#'])
            
            print(f"Found PO numbers for {len(po_numbers_data)} work orders")
            
            # Save to JSON file
            po_numbers_path = LOG_PO_NUMBERS
            if save_json_file(po_numbers_data, po_numbers_path):
                print(f"✓ PO numbers saved: {po_numbers_path}")
            else:
                print("✗ Failed to save PO numbers file")
                
            # Show sample data if debug enabled
            if debug_output and po_numbers_data:
                print("\nSample PO numbers:")
                for i, item in enumerate(po_numbers_data[:5]):
                    print(f"  {i+1}: WO#{item['WO#']} - {item['PO_Numbers']}")
                    
        else:
            print(f"Assembly directory not found: {ASSEMBLY_ACTIVE_DIRECTORY}")
            # Create empty file
            save_json_file([], LOG_PO_NUMBERS)
            
    else:
        print("No active jobs data available to process PO numbers")
        # Create empty file
        save_json_file([], LOG_PO_NUMBERS)

def refine_active_jobs(LOG_ACTIVE_JOBS,
                       LOG_MISSING_CUST_PARTS,
                       LOG_MISSING_PURCH_PARTS,
                       LOG_PCB_STATUS,
                       LOG_STENCIL_STATUS):
    """
    Refines the active jobs list by filtering based on completion criteria and adds internal status tracking.
    
    Args:
        save_dir (str): Directory path for saving refined results
        LOG_ACTIVE_JOBS (str): File path to active jobs JSON
        LOG_MISSING_CUST_PARTS (str): File path to missing customer parts JSON
        LOG_MISSING_PURCH_PARTS (str): File path to missing purchase parts JSON
        LOG_PCB_STATUS (str): File path to PCB status JSON
        LOG_STENCIL_STATUS (str): File path to stencil status JSON
        
    Returns:
        None: Updates active jobs file and creates Excel file. Jobs are kept if they have
              missing parts, incomplete PCBs, or incomplete stencils. Adds internal_status field.
    """
    # Load log files
    missing_cust_parts = load_json_file(LOG_MISSING_CUST_PARTS, default_value=[])
    missing_purch_parts = load_json_file(LOG_MISSING_PURCH_PARTS, default_value=[])
    pcb_status = load_json_file(LOG_PCB_STATUS, default_value=[])
    stencil_status = load_json_file(LOG_STENCIL_STATUS, default_value=[])

    # Build sets for fast lookup
    wo_missing_cust = {entry.get('WO#') for entry in missing_cust_parts if entry.get('WO#')}
    wo_missing_purch = {entry.get('WO#') for entry in missing_purch_parts if entry.get('WO#')}
    pcb_status_dict = {entry.get('WO#'): entry.get('Status') for entry in pcb_status if entry.get('WO#')}
    stencil_status_dict = {entry.get('WO#'): entry.get('Status') for entry in stencil_status if entry.get('WO#')}

    # Load active jobs
    active_jobs_data = load_json_file(LOG_ACTIVE_JOBS, default_value=[])

    refined_active_jobs = []
    for job in active_jobs_data:
        wo_number = job.get('WO#', '')
        reasons = []

        # Criteria 1: Missing customer parts
        if wo_number in wo_missing_cust:
            reasons.append("missing_customer_parts")

        # Criteria 2: Missing purchase parts
        if wo_number in wo_missing_purch:
            reasons.append("missing_purchase_parts")

        # Criteria 3: PCB status not complete
        pcb_stat = pcb_status_dict.get(wo_number, None)
        if pcb_stat != "Complete":
            reasons.append("pcb_incomplete")

        # Criteria 4: Stencil status not complete
        stencil_stat = stencil_status_dict.get(wo_number, None)
        if stencil_stat != "Complete":
            reasons.append("stencil_incomplete")

        # If any reason applies, keep the job and set internal_status
        if reasons:
            job['internal_status'] = ", ".join(reasons)
            refined_active_jobs.append(job)

        # Otherwise, remove from active jobs

    # Save refined active jobs back to LOG_ACTIVE_JOBS
    save_json_file(refined_active_jobs, LOG_ACTIVE_JOBS, create_dir=True)
    print(f"Refined active jobs: {len(refined_active_jobs)} records remain")

def generate_statistics_file(cam_data: list, active_jobs: list, credit_hold_jobs: list):
    """
    Generates an Excel file with job statistics and active jobs detail for reporting purposes.
    
    Args:
        cam_data (list): List of all CAM data records for total count
        active_jobs (list): List of active job records
        credit_hold_jobs (list): List of credit hold job records
        
    Returns:
        None: Creates 'SaveFiles/job_statistics.xlsx' with two sheets:
              - Job Statistics: Summary counts
              - Active Jobs Detail: Detailed active jobs information
    """
    print("Generating job statistics Excel file...")
    
    # Step 1: Initialize and calculate statistics
    blue_gradient_bar(1, 4, color_options[3])
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
    blue_gradient_bar(2, 4, color_options[4])
    excel_file_path = 'SaveFiles/job_statistics.xlsx'

    # Create Excel writer object
    with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
        
        # Step 3: Create Sheet 1 - Job Statistics Summary
        blue_gradient_bar(3, 4, color_options[5])
        stats_df = pd.DataFrame([
            {'Metric': 'Total Jobs', 'Count': job_statistics["total_jobs"]},
            {'Metric': 'Active Jobs', 'Count': job_statistics["active_jobs"]}, 
            {'Metric': 'Credit Hold Jobs', 'Count': job_statistics["credit_hold_jobs"]}
        ])
        stats_df.to_excel(writer, sheet_name='Job Statistics', index=False)
        
        # Step 4: Create Sheet 2 - Active Jobs Detail from active jobs log file
        blue_gradient_bar(4, 4, color_options[6])
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

def build_smartsheet_upload_df(LOG_ACTIVE_JOBS,
                               LOG_USER_ENTERED_DATA,
                               LOG_PO_NUMBERS,
                               LOG_PURCH_DESIGNATOR,
                               LOG_CUSTOMER_DESIGNATORS,
                               LOG_MISSING_PURCH_PARTS,
                               LOG_MISSING_CUST_PARTS,
                               LOG_PCB_STATUS,
                               LOG_STENCIL_STATUS):
    """
    Builds a DataFrame for Smartsheet upload by combining data from multiple log files.
    
    Args:
        LOG_ACTIVE_JOBS (str): Path to active jobs JSON file
        LOG_USER_ENTERED_DATA (str): Path to user-entered data JSON file  
        LOG_PO_NUMBERS (str): Path to PO numbers JSON file
        LOG_PURCH_DESIGNATOR (str): Path to purchase designators JSON file
        LOG_CUSTOMER_DESIGNATORS (str): Path to customer designators JSON file
        LOG_MISSING_PURCH_PARTS (str): Path to missing purchase parts JSON file
        LOG_MISSING_CUST_PARTS (str): Path to missing customer parts JSON file
        LOG_PCB_STATUS (str): Path to PCB status JSON file
        LOG_STENCIL_STATUS (str): Path to stencil status JSON file
        
    Returns:
        pd.DataFrame: Formatted DataFrame ready for Smartsheet upload with all relevant
                      job tracking information, status indicators, and refresh timestamps.
    """
    # Load log_active_jobs.json
    active_jobs_data = load_json_file(LOG_ACTIVE_JOBS, default_value=[])

    # Load user entered data
    user_entered_data = load_json_file(LOG_USER_ENTERED_DATA, default_value=[])
    wo_user_data = {entry.get('WO#'): entry for entry in user_entered_data if entry.get('WO#')}

    # Load PO numbers data
    po_numbers_data = load_json_file(LOG_PO_NUMBERS, default_value=[])
    wo_po_numbers = {entry.get('WO#'): entry.get('PO_Numbers', '') for entry in po_numbers_data if entry.get('WO#')}

    # Load designator data
    purch_designator_data = load_json_file(LOG_PURCH_DESIGNATOR, default_value=[])
    wo_purch_designators = {entry.get('WO#'): entry.get('Designators', '') for entry in purch_designator_data if entry.get('WO#')}

    cust_designator_data = load_json_file(LOG_CUSTOMER_DESIGNATORS, default_value=[])
    wo_cust_designators = {entry.get('WO#'): entry.get('Designators', '') for entry in cust_designator_data if entry.get('WO#')}

    # Load missing parts data
    missing_purch_parts = load_json_file(LOG_MISSING_PURCH_PARTS, default_value=[])
    wo_missing_purch = {entry.get('WO#') for entry in missing_purch_parts if entry.get('WO#')}

    missing_cust_parts = load_json_file(LOG_MISSING_CUST_PARTS, default_value=[])
    wo_missing_cust = {entry.get('WO#') for entry in missing_cust_parts if entry.get('WO#')}

    # Load PCB and Stencil status data
    pcb_status_data = load_json_file(LOG_PCB_STATUS, default_value=[])
    pcb_status_dict = {entry.get('WO#'): entry.get('Status', '') for entry in pcb_status_data if entry.get('WO#')}

    stencil_status_data = load_json_file(LOG_STENCIL_STATUS, default_value=[])
    stencil_status_dict = {entry.get('WO#'): entry.get('Status', '') for entry in stencil_status_data if entry.get('WO#')}

    # Start with log_active_jobs.json as base
    update_rows = []
    current_datetime = datetime.now()

    for job in active_jobs_data:
        wo_number = job.get("WO#", "")
        user_data = wo_user_data.get(wo_number, {})
        pur_part_val = "P" if wo_number in wo_missing_purch else ""
        cus_part_val = "C" if wo_number in wo_missing_cust else ""
        pcb_val = "PCB" if pcb_status_dict.get(wo_number, "") != "Complete" else ""
        stencil_val = "ST" if stencil_status_dict.get(wo_number, "") != "Complete" else ""

        row = {
            "Sales Order Date": format_mmddyy(job.get("Order Date", "")),
            "Turn": job.get("Turn", ""),
            "Due Date": format_mmddyy(job.get("Ship Date", "")),
            "WO#": job.get("WO#", ""),
            "Quote #": job.get("Quote#", ""),
            "Customer": job.get("Customer", ""),
            "Date and Action": user_data.get("Date and Action", ""),
            "Purchase Order": wo_po_numbers.get(wo_number, ""),
            "Purch Des": wo_purch_designators.get(wo_number, ""),
            "Cust Des": wo_cust_designators.get(wo_number, ""),
            "spacer": "",
            "Pur Part": pur_part_val,
            "Cus Part": cus_part_val,
            "PCB": pcb_val,
            "Stencil": stencil_val,
            "spacer2": "",
            "Additional Notes": user_data.get("Additional Notes", ""),
            "Refresh Date": format_mmddyy(current_datetime.strftime('%m/%d/%y')),
            "Refresh Time": current_datetime.strftime('%I:%M %p')
        }
        update_rows.append(row)

    # Create DataFrame
    smartsheet_update_df = pd.DataFrame(update_rows, columns=smartsheet_headers)

    # Sort the data: rows with "Pur Part" values first, then by oldest due date at top
    if not smartsheet_update_df.empty:
        # Create a sorting helper column - 0 for rows with Pur Part values, 1 for empty
        smartsheet_update_df['pur_part_sort'] = smartsheet_update_df['Pur Part'].apply(lambda x: 0 if x else 1)
        
        # Convert Due Date to datetime for proper sorting
        smartsheet_update_df['due_date_sort'] = pd.to_datetime(smartsheet_update_df['Due Date'], errors='coerce')
        
        # Handle NaT values by replacing them with a future date for sorting purposes
        max_date = pd.to_datetime('2099-12-31')
        smartsheet_update_df['due_date_sort'] = smartsheet_update_df['due_date_sort'].fillna(max_date)
        
        # Sort: Pur Part values first (0), then by due date ascending (oldest first)
        smartsheet_update_df = smartsheet_update_df.sort_values(
            ['pur_part_sort', 'due_date_sort'], 
            ascending=[True, True]
        )
        
        # Drop the helper columns
        smartsheet_update_df = smartsheet_update_df.drop(['pur_part_sort', 'due_date_sort'], axis=1)

    # Show sample for review
    print("Sample Smartsheet update DataFrame:")
    print(smartsheet_update_df.head())

    return smartsheet_update_df

def update_smartsheet(smartsheet_update_df, 
                      smartsheet_client, 
                      assembly_part_tracking_id, 
                      smartsheet_part_tracking_df, 
                      smartsheet_sheet):
    """
    Updates Smartsheet by replacing all data with new formatted data including conditional formatting.
    
    Args:
        smartsheet_update_df (pd.DataFrame): DataFrame containing new data to upload
        smartsheet_client: Authenticated Smartsheet client object
        assembly_part_tracking_id (str): Smartsheet ID for the target sheet
        smartsheet_part_tracking_df (pd.DataFrame): Current Smartsheet data for row ID extraction
        smartsheet_sheet: Smartsheet sheet object for column mapping
        
    Returns:
        None: Deletes all existing rows and adds new rows with formatting applied.
              Applies color coding for status indicators and due dates.
    """
    try:
        logger.info(f"Starting Smartsheet update for sheet ID: {assembly_part_tracking_id}")
        logger.info(f"Rows to upload: {len(smartsheet_update_df)}")
        
        # Get all row IDs from the current Smartsheet
        if '_row_id' in smartsheet_part_tracking_df.columns:
            all_row_ids = smartsheet_part_tracking_df['_row_id'].tolist()
            logger.debug(f"Found {len(all_row_ids)} existing row IDs")
        else:
            all_row_ids = []
            logger.warning("No _row_id column found in existing data")

        # Delete all rows in batches
        logger.info(f"Deleting {len(all_row_ids)} rows from Smartsheet in batches of {DELETE_BATCH_SIZE}...")
        print(f"Deleting {len(all_row_ids)} rows from Smartsheet in batches of {DELETE_BATCH_SIZE}...")
        for i in range(0, len(all_row_ids), DELETE_BATCH_SIZE):
            batch_ids = all_row_ids[i:i+DELETE_BATCH_SIZE]
            if batch_ids:
                try:
                    smartsheet_client.Sheets.delete_rows(assembly_part_tracking_id, batch_ids)
                    logger.debug(f"Deleted batch: rows {i+1} to {i+len(batch_ids)}")
                    print(f"Deleted rows {i+1} to {i+len(batch_ids)}")
                except Exception as e:
                    logger.error(f"Error deleting batch {i//DELETE_BATCH_SIZE + 1}: {e}")
                    raise

        # Replace NaN values with empty strings before uploading to Smartsheet
        smartsheet_update_df = smartsheet_update_df.replace({np.nan: ""})
        logger.debug("Replaced NaN values with empty strings")

        # Prepare new rows for Smartsheet
        new_rows = []
        format_part_columns = ['Pur Part', 'Cus Part', 'PCB', 'Stencil']
        format_turn_column = 'Turn'
        format_due_column = 'Due Date'
        
        logger.info("Building rows with formatting...")
        formatting_errors = 0

        for idx, row in smartsheet_update_df.iterrows():
            try:
                cells = []
                for col in smartsheet_update_df.columns:
                    try:
                        col_id = smartsheet_sheet.columns[smartsheet_update_df.columns.get_loc(col)].id
                        cell = smartsheet.models.Cell()
                        cell.column_id = col_id
                        cell.value = row[col]

                        # Apply formatting for part status columns (Pur Part, Cus Part, PCB, Stencil)
                        if col in format_part_columns:
                            if row[col]:  # If there is text in the cell
                                cell.format = ",,,,,,,,2,19,,0,,,,0,"   # White text, red background for active status
                            else:  # If cell is empty
                                cell.format = ",,,,,,,,14,14,,0,,,,0,"  # Green text and background
                    
                        # Apply formatting for Turn column
                        if col == format_turn_column:
                            try:
                                turn_value = str(row[col]).strip().upper()
                                turn_num = float(turn_value)
                                if turn_num >= 7:
                                    cell.format = ",,,,,,,,,9,,0,,,,0,"
                                else:
                                    cell.format = ",,,,,,,,,0,,0,,,,0,"
                            except (ValueError, AttributeError) as e:
                                logger.debug(f"Turn formatting error for row {idx}: {e}")
                                formatting_errors += 1

                        # Apply formatting for Due Date column
                        if col == format_due_column:
                            try:
                                due_date = pd.to_datetime(row[col], errors='coerce')

                                if pd.notnull(due_date):
                                    today = pd.to_datetime(datetime.now().strftime("%Y-%m-%d"))
                                    delta_days = (due_date - today).days

                                    cell.format = ",,,,,,,,40,2,,0,,,,0,"   # Purple text, white background
                                    
                                    if delta_days < 0:
                                        cell.format = ",,,,,,,,2,19,,0,,,,0,"   # White text, red background
                                    elif delta_days <= 3:
                                        cell.format = ",,,,,,,,2,28,,0,,,,0,"   # White text, orange background

                            except Exception as ex:
                                logger.debug(f"Due date formatting error for row {idx}: {ex}")
                                formatting_errors += 1
                        
                        cells.append(cell)
                    except Exception as cell_err:
                        logger.error(f"Error processing cell in column '{col}' for row {idx}: {cell_err}")
                        raise
                        
                new_row = smartsheet.models.Row()
                new_row.to_bottom = True
                new_row.cells = cells
                new_rows.append(new_row)
            except Exception as row_err:
                logger.error(f"Error building row {idx}: {row_err}")
                logger.error(f"Row data: {row.to_dict()}")
                raise

        if formatting_errors > 0:
            logger.warning(f"Encountered {formatting_errors} formatting errors (non-critical)")

        # Add new rows in batches
        logger.info(f"Adding {len(new_rows)} rows to Smartsheet in batches of {ADD_BATCH_SIZE}...")
        print(f"Adding {len(new_rows)} rows to Smartsheet in batches of {ADD_BATCH_SIZE}...")
        for i in range(0, len(new_rows), ADD_BATCH_SIZE):
            batch_rows = new_rows[i:i+ADD_BATCH_SIZE]
            try:
                smartsheet_client.Sheets.add_rows(assembly_part_tracking_id, batch_rows)
                logger.debug(f"Added batch: rows {i+1} to {i+len(batch_rows)}")
                print(f"Added rows {i+1} to {i+len(batch_rows)}")
            except Exception as e:
                logger.error(f"Error adding batch {i//ADD_BATCH_SIZE + 1}: {e}")
                logger.error(f"Batch size: {len(batch_rows)} rows")
                raise

        logger.info("Smartsheet update complete")
        print("Smartsheet update complete.")
        
    except Exception as e:
        logger.error(f"Fatal error in update_smartsheet: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise


