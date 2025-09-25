import time
import requests
from cryptography.fernet import Fernet
import pandas as pd
from typing import List
import os
import smartsheet
import sys

from local_secrets import PASSWORD_FILE_PATH, ENCRYPTED_KEY_PATH

def blue_gradient_bar(progress, total, bar_len=100):
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
    
    for row in sheet.rows:
        row_data = {'_row_id': row.id}  # Add Smartsheet row ID
        for cell in row.cells:
            if cell.column_id in [col.id for col in sheet.columns]:
                col_index = [col.id for col in sheet.columns].index(cell.column_id)
                row_data[columns[col_index]] = cell.value
        data.append(row_data)
    
    # Add '_row_id' to columns for DataFrame
    return pd.DataFrame(data, columns=['_row_id'] + columns)

def load_assembly_job_data(network_dir: str) -> pd.DataFrame:
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
    from defines import bar_len
    data = []
    mpn_by_wo = []  # List of dicts: { 'WO#': ..., 'MPN': ... }
    dir_names = [entry_name for entry_name in os.listdir(network_dir) if os.path.isdir(os.path.join(network_dir, entry_name))]
    total_dirs = len(dir_names)
    found_files = 0

    for idx, entry_name in enumerate(dir_names):
        entry_path = os.path.join(network_dir, entry_name)
        camreadme_path = os.path.join(entry_path, "camReadme.txt")
        if os.path.isfile(camreadme_path):
            found_files += 1
            try:
                with open(camreadme_path, "r", encoding="utf-8", errors="ignore") as f:
                    entry = {}
                    lines = f.readlines()
                    for line in lines:
                        if '|' in line:
                            key, value = line.strip().split('|', 1)
                            entry[key.strip()] = value.strip()
                    entry['__file_path__'] = os.path.normpath(camreadme_path)

                # Remove trailing '|' from all string values in entry
                for k, v in entry.items():
                    if isinstance(v, str):
                        entry[k] = v.rstrip('|')

                data.append(entry)
                    
            except Exception as e:
                print(f"Error reading {camreadme_path}: {e}")

        # Print color gradient progress bar
        blue_gradient_bar(idx + 1, total_dirs, bar_len)
    # Newline after progress bar
    print()

    # Return all camData file information
    return pd.DataFrame(data), pd.DataFrame(mpn_by_wo)

