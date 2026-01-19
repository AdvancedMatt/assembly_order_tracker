"""
███╗   ███╗ █████╗ ██████╗ 
████╗ ████║██╔══██╗██╔══██╗
██╔████╔██║███████║██████╔╝
██║╚██╔╝██║██╔══██║██╔═══╝ 
██║ ╚═╝ ██║██║  ██║██║     
╚═╝     ╚═╝╚═╝  ╚═╝╚═╝     

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
import traceback

from defines import *
from functions import *
from local_secrets import *

# Required imports for database connection
from database_utils import execute_custom_query
from config_manager import validate_config
import logging

# Configure logging based on DEBUG flag
log_level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)

# Create SaveFiles directory if it doesn't exist
os.makedirs('SaveFiles', exist_ok=True)

# Setup logging to both file and console
log_handlers = [
    logging.FileHandler(ERROR_LOG_PATH, mode='a'),
    logging.StreamHandler(sys.stdout)
]

if DEBUG:
    # Add debug log file handler when DEBUG is True
    log_handlers.append(logging.FileHandler(DEBUG_LOG_PATH, mode='a'))

logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=log_handlers
)

logger = logging.getLogger(__name__)

# Track script start time for performance/debug
script_start = time.time()
logger.info(f"{'='*70}")
logger.info(f"Script started - DEBUG Mode: {DEBUG}")
logger.info(f"{'='*70}")
print(f"Script started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
if DEBUG:
    print(f"DEBUG MODE ENABLED - Detailed logs will be written to {DEBUG_LOG_PATH}")
print(f"Error logs will be written to {ERROR_LOG_PATH}")

#-------------------------------------------------------------------#
#            Get smartsheet and convert to dataframe
#-------------------------------------------------------------------#
t_convert_start = time.time()

try:
    logger.info("Attempting to connect to Smartsheet API...")
    ACCESS_TOKEN = get_api_key_file()
    logger.debug(f"API token retrieved successfully (length: {len(ACCESS_TOKEN)})")
    
    smartsheet_client = smartsheet.Smartsheet(ACCESS_TOKEN)
    smartsheet_client.errors_as_exceptions(True)
    logger.info("Smartsheet client initialized")

    logger.info(f"Fetching Smartsheet ID: {assembly_part_tracking_id}")
    smartsheet_sheet = smartsheet_client.Sheets.get_sheet(assembly_part_tracking_id)
    logger.info(f"Smartsheet retrieved - Rows: {len(smartsheet_sheet.rows)}, Columns: {len(smartsheet_sheet.columns)}")
    
    smartsheet_part_tracking_df = convert_sheet_to_dataframe(smartsheet_sheet)
    logger.info(f"Smartsheet converted to DataFrame - Shape: {smartsheet_part_tracking_df.shape}")
    
    if DEBUG or debug_output:
        logger.debug(f"Smartsheet DataFrame columns: {smartsheet_part_tracking_df.columns.tolist()}")
        logger.debug(f"First few rows of Smartsheet DataFrame:\n{smartsheet_part_tracking_df.head()}")
        print("Smartsheet DataFrame columns:", smartsheet_part_tracking_df.columns.tolist())
        print("First few rows of Smartsheet DataFrame:")
        print(smartsheet_part_tracking_df.head())
        if '_row_id' in smartsheet_part_tracking_df.columns:
            logger.debug(f"_row_id sample: {smartsheet_part_tracking_df['_row_id'].head().tolist()}")
            print("_row_id sample:", smartsheet_part_tracking_df['_row_id'].head().tolist())
    
    if smartsheet_part_tracking_df.empty:
        logger.warning("WARNING: Smartsheet DataFrame is EMPTY! This might be expected if starting fresh.")
        print("⚠ WARNING: Smartsheet is empty. Continuing with empty DataFrame...")

except FileNotFoundError as e:
    logger.error(f"File not found error accessing API key: {e}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    print(f"ERROR: Cannot find API key file. Check file paths in local_secrets.py")
    sys.exit(1)
except smartsheet.exceptions.ApiError as e:
    logger.error(f"Smartsheet API error: {e}")
    logger.error(f"Error code: {e.error.result.error_code if hasattr(e.error, 'result') else 'Unknown'}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    print(f"ERROR: Smartsheet API error - {e}")
    print(f"This could be due to: invalid token, no network access, or incorrect sheet ID")
    sys.exit(1)
except Exception as e:
    logger.error(f"Unexpected error converting Smartsheet data: {e}")
    logger.error(f"Error type: {type(e).__name__}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    print(f"ERROR: Failed to load Smartsheet - {e}")
    sys.exit(1)

t_convert_end = time.time() - t_convert_start

#-------------------------------------------------------------------#
#            Store smartsheet user entered infomration 
#-------------------------------------------------------------------#
t_smartsheet_data_start = time.time()

try:
    logger.info("Storing Smartsheet user-entered data...")
    store_smartsheet_user_data(smartsheet_part_tracking_df, USE_COLOR_PROGRESS_BAR)
    logger.info("User-entered data stored successfully")
except Exception as e:
    logger.error(f"Error storing Smartsheet user data: {e}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    print(f"ERROR: Failed to store user-entered data - {e}")

t_smartsheet_data_end = time.time() - t_smartsheet_data_start

#-------------------------------------------------------------------#
#          Get camData (ETHAR) and convert to dataframe
#-------------------------------------------------------------------#
t_camData_start = time.time()

try:
    logger.info("Loading assembly job tracking data from network directory...")
    logger.info(f"Network directory: {ASSEMBLY_ACTIVE_DIRECTORY}")
    
    # Verify network directory exists
    if not os.path.exists(ASSEMBLY_ACTIVE_DIRECTORY):
        logger.error(f"Network directory does not exist: {ASSEMBLY_ACTIVE_DIRECTORY}")
        raise FileNotFoundError(f"Cannot access network directory: {ASSEMBLY_ACTIVE_DIRECTORY}")
    
    if not os.path.isdir(ASSEMBLY_ACTIVE_DIRECTORY):
        logger.error(f"Path is not a directory: {ASSEMBLY_ACTIVE_DIRECTORY}")
        raise NotADirectoryError(f"Path is not a directory: {ASSEMBLY_ACTIVE_DIRECTORY}")
    
    # Check directory permissions
    try:
        test_list = os.listdir(ASSEMBLY_ACTIVE_DIRECTORY)
        logger.debug(f"Found {len(test_list)} entries in network directory")
    except PermissionError as pe:
        logger.error(f"Permission denied accessing network directory: {pe}")
        raise
    
    # Handle missing or empty JSON file
    if not os.path.isfile(LOG_CAM_DATA) or os.path.getsize(LOG_CAM_DATA) == 0:
        logger.info(f"No existing cam data log found at {LOG_CAM_DATA}, starting fresh")
        old_data = []
    else:
        try:
            with open(LOG_CAM_DATA, "r") as f:
                old_data = json.load(f)
            logger.debug(f"Loaded {len(old_data)} existing cam data records")
        except json.JSONDecodeError as je:
            logger.warning(f"JSON decode error in {LOG_CAM_DATA}: {je}. Starting fresh.")
            old_data = []
        except Exception as read_err:
            logger.warning(f"Error reading {LOG_CAM_DATA}: {read_err}. Starting fresh.")
            old_data = []

    # Load assembly job tracking data from camReadme.txt files
    logger.info("Scanning camReadme.txt files...")
    assembly_job_tracking_df = load_assembly_job_data(ASSEMBLY_ACTIVE_DIRECTORY, LOG_CAM_DATA)
   
    # Function returns a tuple (df, _), use only the first
    if isinstance(assembly_job_tracking_df, tuple):
        assembly_job_tracking_df = assembly_job_tracking_df[0]
        logger.debug("Extracted DataFrame from tuple return")

    logger.info(f"Loaded {len(assembly_job_tracking_df)} job records from camReadme files")
    
    if assembly_job_tracking_df.empty:
        logger.warning("WARNING: No job data found in camReadme.txt files!")
        print("⚠ WARNING: No assembly jobs found. Check if network directory has job folders.")
    
    # Sanitize data - fix invalid numeric and date fields
    logger.info("Sanitizing data fields...")
    assembly_job_tracking_df, data_corrections = sanitize_cam_data(assembly_job_tracking_df)
    
    if data_corrections:
        # Save corrections report
        corrections_file = 'SaveFiles/data_corrections.json'
        try:
            with open(corrections_file, 'w') as f:
                json.dump(data_corrections, f, indent=2)
            logger.info(f"Data corrections report saved to {corrections_file}")
        except Exception as e:
            logger.warning(f"Could not save corrections report: {e}")

    # Add 'internal_status' column, blank for all rows
    assembly_job_tracking_df['internal_status'] = ""

    # Save updated data back to JSON file
    assembly_job_tracking_df.to_json(LOG_CAM_DATA, orient="records", indent=2)
    logger.info(f"Saved cam data to {LOG_CAM_DATA}")

except FileNotFoundError as e:
    logger.error(f"File/Directory not found: {e}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    print(f"ERROR: Cannot access required files/directories - {e}")
    print(f"Please verify network connection and directory paths.")
    sys.exit(1)
except PermissionError as e:
    logger.error(f"Permission denied: {e}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    print(f"ERROR: Permission denied accessing network directory - {e}")
    print(f"Please check your network permissions.")
    sys.exit(1)
except Exception as e:
    logger.error(f"Error loading assembly job tracking data: {e}")
    logger.error(f"Error type: {type(e).__name__}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    print(f"ERROR: Failed to load assembly job data - {e}")
    # Initialize empty DataFrame to allow script to continue
    assembly_job_tracking_df = pd.DataFrame()
    assembly_job_tracking_df['internal_status'] = ""

t_camData_end = time.time() - t_camData_start

#-------------------------------------------------------------------#
#         Build active assembly jobs and credit hold files
#-------------------------------------------------------------------#
t_active_jobs_file_start = time.time()

try:
    logger.info("Building active assembly jobs and credit hold files...")
    # Load the source data from log_camData.json
    cam_data = load_json_file(LOG_CAM_DATA, default_value=[])
    logger.debug(f"Loaded {len(cam_data)} records from cam data")

    # Load existing credit hold data to check for releases
    existing_credit_holds = set()

    existing_data = load_json_file(LOG_CREDIT_HOLD, default_value=[])

    if existing_data:
        # Extract WO# from existing credit hold records
        existing_credit_holds = {record.get('WO#') for record in existing_data if record.get('WO#')}
        logger.debug(f"Found {len(existing_credit_holds)} existing credit holds")

    active_jobs, credit_hold_jobs, credit_hold_released = build_active_credithold_files(cam_data, existing_credit_holds)
    logger.info(f"Built active jobs: {len(active_jobs)}, credit holds: {len(credit_hold_jobs)}, released: {len(credit_hold_released)}")

    # Write active jobs to log_active_jobs.json
    if save_json_file(active_jobs, LOG_ACTIVE_JOBS, create_dir=True):
        logger.info(f"Active jobs saved successfully: {len(active_jobs)} records")
        print(f"✓ Active jobs saved successfully: {len(active_jobs)} records")

    # Write credit hold jobs to log_credit_hold.json
    if save_json_file(credit_hold_jobs, LOG_CREDIT_HOLD, create_dir=True):
        logger.info(f"Credit hold jobs saved successfully: {len(credit_hold_jobs)} records")
        print(f"✓ Credit hold jobs saved successfully: {len(credit_hold_jobs)} records")

    # Write credit hold jobs to log_credit_released.json
    if save_json_file(credit_hold_released, LOG_CREDIT_RELEASED, create_dir=True):
        logger.info(f"Credit hold released jobs saved successfully: {len(credit_hold_released)} records")
        print(f"✓ Credit hold released jobs saved successfully: {len(credit_hold_released)} records")

except Exception as e:
    logger.error(f"Error building active assembly jobs file: {e}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    print(f"ERROR: Failed to build active jobs - {e}")
    # Initialize variables to prevent NameError in subsequent code
    active_jobs = []
    credit_hold_jobs = []
    credit_hold_released = []
    cam_data = []

t_active_jobs_file_time = time.time() - t_active_jobs_file_start

#-------------------------------------------------------------------#
#                    Build master BOM dataframe
#-------------------------------------------------------------------#
t_master_BOM_start = time.time()

try:
    logger.info("Building master BOM dataframe...")
    master_bom_df = build_master_bom(active_jobs, ASSEMBLY_ACTIVE_DIRECTORY, debug_output)
    logger.info(f"Master BOM built with {len(master_bom_df)} records")
    if master_bom_df.empty:
        logger.warning("WARNING: Master BOM is empty!")
        print("⚠ WARNING: Master BOM is empty. Check if job folders contain BOM files.")
except Exception as e:
    logger.error(f"Error building master BOM dataframe: {e}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    print(f"ERROR: Failed to build master BOM - {e}")
    master_bom_df = pd.DataFrame()

t_master_BOM_end = time.time() - t_master_BOM_start

#-------------------------------------------------------------------#
#                Add overage to master parts file
#-------------------------------------------------------------------#
t_reqd_parts_file_start = time.time()

try:
    logger.info("Adding purchasing overage to master BOM...")
    master_bom_df = add_overage_to_master_bom(master_bom_df, QUOTE_DIR, False)
    logger.info("Overage added successfully")
except Exception as e:
    logger.error(f"Error adding purchasing overage to master BOM: {e}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    print(f"ERROR: Failed to add overage - {e}")

t_reqd_parts_file_end = time.time() - t_reqd_parts_file_start

#-------------------------------------------------------------------#
#                Build missing purchase parts file
#-------------------------------------------------------------------#
t_purchase_parts_file_start = time.time()

try:
    logger.info("Building missing purchase parts file...")
    missing_purchase_parts_file(master_bom_df, LOG_MISSING_PURCH_PARTS, debug_output)
    logger.info("Missing purchase parts file created")
except Exception as e:
    logger.error(f"Error building missing purchase parts file: {e}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    print(f"ERROR: Failed to build missing purchase parts - {e}")

t_purchase_parts_file_end = time.time() - t_purchase_parts_file_start

#-------------------------------------------------------------------#
#           Build missing purchase parts designator file
#-------------------------------------------------------------------#
t_purchase_parts_designator_file_start = time.time()

try:
    logger.info("Building purchase parts designator file...")
    missing_purchase_parts_designator_file(master_bom_df, LOG_PURCH_DESIGNATOR, debug_output)
    logger.info("Purchase parts designator file created")
except Exception as e:
    logger.error(f"Error building purchase parts designator file: {e}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    print(f"ERROR: Failed to build purchase parts designator - {e}")

t_purchase_parts_designator_file_end = time.time() - t_purchase_parts_designator_file_start

#-------------------------------------------------------------------#
#                Build missing customer parts file
#-------------------------------------------------------------------#
t_customer_parts_file_start = time.time()

try:
    logger.info("Building missing customer parts file...")
    missing_cust_parts_file(master_bom_df, LOG_MISSING_CUST_PARTS, debug_output)
    logger.info("Missing customer parts file created")
except Exception as e:
    logger.error(f"Error building missing customer parts file: {e}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    print(f"ERROR: Failed to build missing customer parts - {e}")

t_customer_parts_file_end = time.time() - t_customer_parts_file_start

#-------------------------------------------------------------------#
#           Build missing customer parts designator file
#-------------------------------------------------------------------#
t_customer_parts_designator_file_start = time.time()

try:
    logger.info("Building customer parts designator file...")
    missing_cust_parts_designator_file(master_bom_df, LOG_CUSTOMER_DESIGNATORS, debug_output)
    logger.info("Customer parts designator file created")
except Exception as e:
    logger.error(f"Error building customer parts designator file: {e}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    print(f"ERROR: Failed to build customer parts designator - {e}")

t_customer_parts_designator_file_end = time.time() - t_customer_parts_designator_file_start

#-------------------------------------------------------------------#
#                      Build missing PCB file
#-------------------------------------------------------------------#
t_pcb_file_start = time.time()

try:
    logger.info("Building PCB status file...")
    missing_pcb_file(master_bom_df, LOG_PCB_STATUS, debug_output)
    logger.info("PCB status file created")
except Exception as e:
    logger.error(f"Error building PCB status file: {e}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    print(f"ERROR: Failed to build PCB status - {e}")

t_pcb_file_end = time.time() - t_pcb_file_start

#-------------------------------------------------------------------#
#                    Build missing stencil file
#-------------------------------------------------------------------#
t_stencil_file_start = time.time()

try:
    logger.info("Building stencil status file...")
    missing_stencil_file(master_bom_df, LOG_STENCIL_STATUS, debug_output)
    logger.info("Stencil status file created")
except Exception as e:
    logger.error(f"Error building stencil status file: {e}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    print(f"ERROR: Failed to build stencil status - {e}")

t_stencil_file_end = time.time() - t_stencil_file_start

#-------------------------------------------------------------------#
#                       Build parts PO file
#-------------------------------------------------------------------#
t_parts_po_file_start = time.time()

try:
    logger.info("Building PO numbers file...")
    parts_po_file(active_jobs, ASSEMBLY_ACTIVE_DIRECTORY, LOG_PO_NUMBERS, debug_output)
    logger.info("PO numbers file created")
except Exception as e:
    logger.error(f"Error building PO numbers file: {e}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    print(f"ERROR: Failed to build PO numbers - {e}")

t_parts_po_file_end = time.time() - t_parts_po_file_start

#-------------------------------------------------------------------#
#                     Refine active jobs list
#-------------------------------------------------------------------#
t_refine_active_jobs_start = time.time()

try:
    logger.info("Refining active jobs list...")
    refine_active_jobs(
        LOG_ACTIVE_JOBS,
        LOG_MISSING_CUST_PARTS,
        LOG_MISSING_PURCH_PARTS,
        LOG_PCB_STATUS,
        LOG_STENCIL_STATUS
    )
    logger.info("Active jobs refined successfully")
except Exception as e:
    logger.error(f"Error refining active jobs: {e}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    print(f"ERROR: Failed to refine active jobs - {e}")

t_refine_active_jobs_end = time.time() - t_refine_active_jobs_start

#-------------------------------------------------------------------#
#                    Build job statistics file
#-------------------------------------------------------------------#
t_stats_file_start = time.time()

try:
    logger.info("Generating statistics file...")
    generate_statistics_file(cam_data, active_jobs, credit_hold_jobs)
    logger.info("Statistics file generated")
except Exception as e:
    logger.error(f"Error generating statistics file: {e}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    print(f"WARNING: Failed to generate statistics - {e}")

t_stats_file_end = time.time() - t_stats_file_start

#-------------------------------------------------------------------#
#                Build DataFrame for Smartsheet update
#-------------------------------------------------------------------#
t_smartsheet_df_start = time.time()

try:
    logger.info("Building Smartsheet upload DataFrame...")
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
    logger.info(f"Smartsheet DataFrame built with {len(smartsheet_update_df)} rows")
    
    if smartsheet_update_df.empty:
        logger.warning("WARNING: Smartsheet update DataFrame is EMPTY!")
        print("⚠ WARNING: Nothing to upload to Smartsheet (empty DataFrame)")
    
    if DEBUG:
        logger.debug(f"Smartsheet update columns: {smartsheet_update_df.columns.tolist()}")
        logger.debug(f"Sample data:\n{smartsheet_update_df.head()}")

except Exception as e:
    logger.error(f"Error building Smartsheet update DataFrame: {e}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    print(f"ERROR: Failed to build Smartsheet DataFrame - {e}")
    smartsheet_update_df = pd.DataFrame()

t_smartsheet_df_end = time.time() - t_smartsheet_df_start

#-------------------------------------------------------------------#
#                         Update smartsheet
#-------------------------------------------------------------------#
t_smartsheet_update_start = time.time()

try:
    logger.info("Updating Smartsheet...")
    if smartsheet_update_df.empty:
        logger.warning("Skipping Smartsheet update - no data to upload")
        print("⚠ Skipping Smartsheet update (no data)")
    else:
        update_smartsheet(
            smartsheet_update_df,
            smartsheet_client,
            assembly_part_tracking_id,
            smartsheet_part_tracking_df,
            smartsheet_sheet
        )
        logger.info("Smartsheet update completed successfully")
        print("✓ Smartsheet updated successfully")

except smartsheet.exceptions.ApiError as e:
    logger.error(f"Smartsheet API error during update: {e}")
    logger.error(f"Error code: {e.error.result.error_code if hasattr(e.error, 'result') else 'Unknown'}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    print(f"ERROR: Smartsheet API error during update - {e}")
    print(f"Data may not have been uploaded correctly. Check the error log.")
except Exception as e:
    logger.error(f"Error updating Smartsheet: {e}")
    logger.error(f"Error type: {type(e).__name__}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    print(f"ERROR: Failed to update Smartsheet - {e}")

t_smartsheet_update_end = time.time() - t_smartsheet_update_start

#-------------------------------------------------------------------#
#                         Print timing data
#-------------------------------------------------------------------#
script_end = time.time()

logger.info(f"{'='*70}")
logger.info("Script execution summary:")
logger.info(f"Loaded assembly job tracker (smartsheet): {t_convert_end:.2f} seconds")
logger.info(f"Stored smartsheet user data: {t_smartsheet_data_end:.2f} seconds")
logger.info(f"Loaded assembly job tracking data (camReadme.txt): {t_camData_end:.2f} seconds")
logger.info(f"Built active assembly jobs file: {t_active_jobs_file_time:.2f} seconds")
logger.info(f"Built master BOM dataframe: {t_master_BOM_end:.2f} seconds")
logger.info(f"Total script runtime: {script_end - script_start:.2f} seconds")
logger.info(f"{'='*70}")

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
print(f"\nLogs written to: {ERROR_LOG_PATH}")
if DEBUG:
    print(f"Debug logs written to: {DEBUG_LOG_PATH}")