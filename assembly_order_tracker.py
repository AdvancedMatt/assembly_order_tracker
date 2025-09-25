"""
Assembly Job Tracker Script
==========================

Description:
    This script automates the end-to-end process of synchronizing assembly job tracking data between
    ETHAR and a Smartsheet sheet. It performs the following tasks:

    - 


    - Extracts job data from 'camReadme.txt' files and other sources in each job folder.
    - Cleans and filters the data, including removing old records, cleaning up delimiters, 
      and filtering by date, status, and quantity.
    - Calculates and adds derived fields such as shipped quantity, original due date 
      (excluding weekends and holidays), and job released to the floor date (from log files).
    - Merges in additional data from Smartsheet sources, such as PCB recommit dates.
    - Maps local data columns to Smartsheet columns using a configurable mapping.
    - Deletes all rows from the target Smartsheet and uploads the new data in bulk, with 
      safety checks to prevent accidental updates.
    - Provides debug output for data validation and troubleshooting, including sample 
      log lines if expected data is missing.

    The script is designed to be run as a scheduled or manual update to keep the Smartsheet 
    in sync with the latest job information from ETHAR, and is extensible for future enhancements.

Features:
    - Loads and parses job data from a network directory structure.
    - Cleans, filters, and transforms data for business logic and reporting needs.
    - Calculates business dates, handles US holidays, and extracts key events from log files.
    - Merges and maps data from multiple Smartsheet sources.
    - Bulk updates Smartsheet with robust error handling and debug output.
    - Designed for maintainability and easy extension.

Usage:
    - 
"""

import time
import smartsheet
import pandas as pd
import numpy as np
from pandas.tseries.offsets import CustomBusinessDay
from datetime import datetime, timedelta
import re
import sys
import os
from glob import glob

from defines import *
from functions import *
from local_secrets import *

# Track script start time for performance/debug
script_start = time.time()
print(f"Script started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

save_dir = os.path.join(os.path.dirname(__file__), "SaveFiles")
log_camData_path = os.path.join(save_dir, "log_camData.json")

#-------------------------------------------------------------------#
#            Get smartsheet and convert to dataframe
#-------------------------------------------------------------------#
t_convert_start = time.time()

try:
    ACCESS_TOKEN = get_api_key_file()
    smartsheet_client = smartsheet.Smartsheet(ACCESS_TOKEN)
    smartsheet_client.errors_as_exceptions(True)

    try:
        smartsheet_sheet = smartsheet_client.Sheets.get_sheet(assembly_part_tracking_id)
        smartsheet_part_tracking_df = convert_sheet_to_dataframe(smartsheet_sheet)
        
        if debug_output:
            print("Smartsheet DataFrame columns:", smartsheet_part_tracking_df.columns.tolist())
            print("First few rows of Smartsheet DataFrame:")
            print(smartsheet_part_tracking_df.head())
            if '_row_id' in smartsheet_part_tracking_df.columns:
                print("_row_id sample:", smartsheet_part_tracking_df['_row_id'].head().tolist())

    except smartsheet.exceptions.ApiError as e:
        print(f"Error: {e}")  
        t_convert = None

    if debug_output:
        print("Loaded smartsheet job tracking data:", smartsheet_part_tracking_df.shape)
except Exception as e:
    print(f"Error converting Smartsheet data: {e}")

t_convert_end = time.time() - t_convert_start

#-------------------------------------------------------------------#
#             Get camData and convert to dataframe
#-------------------------------------------------------------------#
t_camData_start = time.time()

try:
    # Handle missing or empty JSON file
    if not os.path.isfile(log_camData_path) or os.path.getsize(log_camData_path) == 0:
        old_data = []
    else:
        try:
            with open(log_camData_path, "r") as f:
                old_data = json.load(f)
        except Exception:
            old_data = []

    # Load assembly job tracking data from camReadme.txt files
    assembly_job_tracking_df = load_assembly_job_data(ASSEMBLY_ACTIVE_DIRECTORY, log_camData_path)
except Exception as e:
    print(f"Error loading assembly job tracking data: {e}")

try:    
    # Function returns a tuple (df, _), use only the first
    if isinstance(assembly_job_tracking_df, tuple):
        assembly_job_tracking_df = assembly_job_tracking_df[0]

    # Add 'internal_status' column, blank for all rows
    assembly_job_tracking_df['internal_status'] = ""

    # Ensure SaveFiles directory exists
    os.makedirs(save_dir, exist_ok=True)

    # Save to JSON
    assembly_job_tracking_df.to_json(log_camData_path, orient="records", indent=2)

except Exception as e:
    print(f"Error loading assembly job tracking data: {e}")

t_camData_end = time.time() - t_camData_start

#-------------------------------------------------------------------#
#                         Print timing data
#-------------------------------------------------------------------#
script_end = time.time()

print(f"Loaded assembly job tracking data (smartsheet): {t_convert_end:.2f} seconds")
print(f"Loaded assembly job tracking data (camReadme.txt): {t_camData_end:.2f} seconds")
print(" ")
print(f"Script ended at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total script runtime: {script_end - script_start:.2f} seconds")