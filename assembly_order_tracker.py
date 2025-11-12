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
from pandas.tseries.offsets import CustomBusinessDay
from datetime import datetime, timedelta
import re
import sys
import os
import json
from glob import glob

from defines import *
from functions import *
from local_secrets import *

# Track script start time for performance/debug
script_start = time.time()
print(f"Script started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

#-------------------------------------------------------------------#
#            Get smartsheet and convert to dataframe
#-------------------------------------------------------------------#
t_convert_start = time.time()

try:
    ACCESS_TOKEN = get_api_key_file()
    smartsheet_client = smartsheet.Smartsheet(ACCESS_TOKEN)
    smartsheet_client.errors_as_exceptions(True)

    smartsheet_sheet = smartsheet_client.Sheets.get_sheet(assembly_part_tracking_id)
    smartsheet_part_tracking_df = convert_sheet_to_dataframe(smartsheet_sheet)
    
    if debug_output:
        print("Smartsheet DataFrame columns:", smartsheet_part_tracking_df.columns.tolist())
        print("First few rows of Smartsheet DataFrame:")
        print(smartsheet_part_tracking_df.head())
        if '_row_id' in smartsheet_part_tracking_df.columns:
            print("_row_id sample:", smartsheet_part_tracking_df['_row_id'].head().tolist())

except Exception as e:
    print(f"Error converting Smartsheet data: {e}")

t_convert_end = time.time() - t_convert_start

#-------------------------------------------------------------------#
#            Store smartsheet user entered infomration 
#-------------------------------------------------------------------#
t_smartsheet_data_start = time.time()

store_smartsheet_user_data(smartsheet_part_tracking_df)

t_smartsheet_data_end = time.time() - t_smartsheet_data_start

#-------------------------------------------------------------------#
#          Get camData (ETHAR) and convert to dataframe
#-------------------------------------------------------------------#
t_camData_start = time.time()

try:
    # Handle missing or empty JSON file
    if not os.path.isfile(LOG_CAM_DATA) or os.path.getsize(LOG_CAM_DATA) == 0:
        old_data = []
    else:
        try:
            with open(LOG_CAM_DATA, "r") as f:
                old_data = json.load(f)
        except Exception:
            old_data = []

    # Load assembly job tracking data from camReadme.txt files
    assembly_job_tracking_df = load_assembly_job_data(ASSEMBLY_ACTIVE_DIRECTORY, LOG_CAM_DATA)
   
    # Function returns a tuple (df, _), use only the first
    if isinstance(assembly_job_tracking_df, tuple):
        assembly_job_tracking_df = assembly_job_tracking_df[0]

    # Add 'internal_status' column, blank for all rows
    assembly_job_tracking_df['internal_status'] = ""

    # Save updated data back to JSON file
    assembly_job_tracking_df.to_json(LOG_CAM_DATA, orient="records", indent=2)

except Exception as e:
    print(f"Error loading assembly job tracking data: {e}")

t_camData_end = time.time() - t_camData_start

#-------------------------------------------------------------------#
#         Build active assembly jobs and credit hold files
#-------------------------------------------------------------------#
t_active_jobs_file_start = time.time()

try:
    # Load the source data from log_camData.json
    cam_data = load_json_file(LOG_CAM_DATA, default_value=None)

    # Load existing credit hold data to check for releases
    existing_credit_holds = set()

    existing_data = load_json_file(LOG_CREDIT_HOLD, default_value=None)

    if existing_data:
        # Extract WO# from existing credit hold records
        existing_credit_holds = {record.get('WO#') for record in existing_data if record.get('WO#')}

    active_jobs, credit_hold_jobs, credit_hold_released = build_active_credithold_files(cam_data, existing_credit_holds)

    # Write active jobs to log_active_jobs.json
    if save_json_file(active_jobs, LOG_ACTIVE_JOBS, create_dir=True):
        print(f"✓ Active jobs saved successfully: {len(active_jobs)} records")

    # Write credit hold jobs to log_credit_hold.json
    if save_json_file(credit_hold_jobs, LOG_CREDIT_HOLD, create_dir=True):
        print(f"✓ Credit hold jobs saved successfully: {len(credit_hold_jobs)} records")

    # Write credit hold jobs to log_credit_released.json
    if save_json_file(credit_hold_released, LOG_CREDIT_RELEASED, create_dir=True):
        print(f"✓ Credit hold released jobs saved successfully: {len(credit_hold_released)} records")

except Exception as e:
    print(f"Error building active assembly jobs file: {e}")

t_active_jobs_file_time = time.time() - t_active_jobs_file_start

#-------------------------------------------------------------------#
#                    Build master BOM dataframe
#-------------------------------------------------------------------#
t_master_BOM_start = time.time()

try:
    master_bom_df = build_master_bom(active_jobs, ASSEMBLY_ACTIVE_DIRECTORY, debug_output)

except Exception as e:
    print(f"Error building master BOM dataframe: {e}")
    master_bom_df = pd.DataFrame()

t_master_BOM_end = time.time() - t_master_BOM_start

#-------------------------------------------------------------------#
#                Add overage to master parts file
#-------------------------------------------------------------------#
t_reqd_parts_file_start = time.time()

try:
    master_bom_df = add_overage_to_master_bom(master_bom_df, QUOTE_DIR, False)

except Exception as e:
    print(f"Error adding purchasing overage to master BOM: {e}")

t_reqd_parts_file_end = time.time() - t_reqd_parts_file_start

#-------------------------------------------------------------------#
#                Build missing purchase parts file
#-------------------------------------------------------------------#
t_purchase_parts_file_start = time.time()

try:
    missing_purchase_parts_file(master_bom_df, LOG_MISSING_PURCH_PARTS, debug_output)

except Exception as e:
    print(f"Error building missing purchase parts file: {e}")

t_purchase_parts_file_end = time.time() - t_purchase_parts_file_start

#-------------------------------------------------------------------#
#           Build missing purchase parts designator file
#-------------------------------------------------------------------#
t_purchase_parts_designator_file_start = time.time()

try:
    missing_purchase_parts_designator_file(master_bom_df, LOG_PURCH_DESIGNATOR, debug_output)

except Exception as e:
    print(f"Error building purchase parts designator file: {e}")

t_purchase_parts_designator_file_end = time.time() - t_purchase_parts_designator_file_start

#-------------------------------------------------------------------#
#                Build missing customer parts file
#-------------------------------------------------------------------#
t_customer_parts_file_start = time.time()

try:
    missing_cust_parts_file(master_bom_df, LOG_MISSING_CUST_PARTS, debug_output)
    
except Exception as e:
    print(f"Error building missing customer parts file: {e}")

t_customer_parts_file_end = time.time() - t_customer_parts_file_start

#-------------------------------------------------------------------#
#           Build missing customer parts designator file
#-------------------------------------------------------------------#
t_customer_parts_designator_file_start = time.time()

try:
    missing_cust_parts_designator_file(master_bom_df, LOG_CUSTOMER_DESIGNATORS, debug_output)
    
except Exception as e:
    print(f"Error building customer parts designator file: {e}")

t_customer_parts_designator_file_end = time.time() - t_customer_parts_designator_file_start

#-------------------------------------------------------------------#
#                      Build missing PCB file
#-------------------------------------------------------------------#
t_pcb_file_start = time.time()

try:
    missing_pcb_file(master_bom_df, LOG_PCB_STATUS, debug_output)

except Exception as e:
    print(f"Error building PCB status file: {e}")

t_pcb_file_end = time.time() - t_pcb_file_start

#-------------------------------------------------------------------#
#                    Build missing stencil file
#-------------------------------------------------------------------#
t_stencil_file_start = time.time()

try:
    missing_stencil_file(master_bom_df, LOG_STENCIL_STATUS, debug_output)

except Exception as e:
    print(f"Error building stencil status file: {e}")

t_stencil_file_end = time.time() - t_stencil_file_start

#-------------------------------------------------------------------#
#                       Build parts PO file
#-------------------------------------------------------------------#
t_parts_po_file_start = time.time()

try:
    parts_po_file(active_jobs, ASSEMBLY_ACTIVE_DIRECTORY, LOG_PO_NUMBERS, debug_output)

except Exception as e:
    print(f"Error building PO numbers file: {e}")

t_parts_po_file_end = time.time() - t_parts_po_file_start

#-------------------------------------------------------------------#
#                     Refine active jobs list
#-------------------------------------------------------------------#
t_refine_active_jobs_start = time.time()

try:
    refine_active_jobs(
        LOG_ACTIVE_JOBS,
        LOG_MISSING_CUST_PARTS,
        LOG_MISSING_PURCH_PARTS,
        LOG_PCB_STATUS,
        LOG_STENCIL_STATUS
    )

except Exception as e:
    print(f"Error refining active jobs: {e}")

t_refine_active_jobs_end = time.time() - t_refine_active_jobs_start

#-------------------------------------------------------------------#
#                    Build job statistics file
#-------------------------------------------------------------------#
t_stats_file_start = time.time()

generate_statistics_file(cam_data, active_jobs, credit_hold_jobs)

t_stats_file_end = time.time() - t_stats_file_start

#-------------------------------------------------------------------#
#                Build DataFrame for Smartsheet update
#-------------------------------------------------------------------#
t_smartsheet_df_start = time.time()

try:
    smartsheet_update_df = build_smartsheet_upload_df(
                                LOG_ACTIVE_JOBS,
                                LOG_USER_ENTERED_DATA,
                                LOG_PO_NUMBERS,
                                LOG_PURCH_DESIGNATOR,
                                LOG_CUSTOMER_DESIGNATORS,
                                LOG_MISSING_PURCH_PARTS,
                                LOG_MISSING_CUST_PARTS,
                                LOG_PCB_STATUS,
                                LOG_STENCIL_STATUS
    )

except Exception as e:
    print(f"Error building Smartsheet update DataFrame: {e}")

t_smartsheet_df_end = time.time() - t_smartsheet_df_start

#-------------------------------------------------------------------#
#                         Update smartsheet
#-------------------------------------------------------------------#
t_smartsheet_update_start = time.time()

try:
    update_smartsheet(
        smartsheet_update_df,
        smartsheet_client,
        assembly_part_tracking_id,
        smartsheet_part_tracking_df,
        smartsheet_sheet
    )

except Exception as e:
    print(f"Error updating Smartsheet: {e}")

t_smartsheet_update_end = time.time() - t_smartsheet_update_start

#-------------------------------------------------------------------#
#                         Print timing data
#-------------------------------------------------------------------#
script_end = time.time()

print(f"Loaded assembly job tracker (smartsheet): {t_convert_end:.2f} seconds")
print(f"Stored smartsheet user data: {t_smartsheet_data_end:.2f} seconds")
print(f"Loaded assembly job tracking data (camReadme.txt): {t_camData_end:.2f} seconds")
print(f"Built active assembly jobs file: {t_active_jobs_file_time:.2f} seconds")
print(f"Built master BOM dataframe: {t_master_BOM_end:.2f} seconds")
print(f"Added overage to master BOM: {t_reqd_parts_file_end:.2f} seconds")
print(f"Built missing purchase parts file: {t_purchase_parts_file_end:.2f} seconds")
print(f"Built missing purchase parts designator file: {t_purchase_parts_designator_file_end:.2f} seconds")
print(f"Built missing customer parts file: {t_customer_parts_file_end:.2f} seconds")
print(f"Built missing customer parts designator file: {t_customer_parts_designator_file_end:.2f} seconds")
print(f"Built PCB status file: {t_pcb_file_end:.2f} seconds")
print(f"Built stencil status file: {t_stencil_file_end:.2f} seconds")
print(f"Built parts PO file: {t_parts_po_file_end:.2f} seconds")
print(f"Built job statistics file: {t_stats_file_end:.2f} seconds")
print(" ")
print(f"Script ended at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total script runtime: {script_end - script_start:.2f} seconds")
print(f"✓ Processing complete!")