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
import json
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
   
    # Function returns a tuple (df, _), use only the first
    if isinstance(assembly_job_tracking_df, tuple):
        assembly_job_tracking_df = assembly_job_tracking_df[0]

    # Add 'internal_status' column, blank for all rows
    assembly_job_tracking_df['internal_status'] = ""

    # Ensure SaveFiles directory exists and save
    os.makedirs(save_dir, exist_ok=True)
    assembly_job_tracking_df.to_json(log_camData_path, orient="records", indent=2)

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
    master_bom_df = add_overage_to_master_bom(master_bom_df, QUOTE_DIR)

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
    # Create empty file on error
    save_json_file([], LOG_MISSING_PURCH_PARTS)

t_purchase_parts_file_end = time.time() - t_purchase_parts_file_start

#-------------------------------------------------------------------#
#           Build missing purchase parts designator file
#-------------------------------------------------------------------#
t_purchase_parts_designator_file_start = time.time()

try:
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
        designator_path = 'SaveFiles/log_purch_designator.json'
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
        save_json_file([], 'SaveFiles/log_purch_designator.json')

except Exception as e:
    print(f"Error building purchase parts designator file: {e}")
    # Create empty file on error
    save_json_file([], 'SaveFiles/log_purch_designator.json')

t_purchase_parts_designator_file_end = time.time() - t_purchase_parts_designator_file_start

#-------------------------------------------------------------------#
#                Build missing customer parts file
#-------------------------------------------------------------------#
t_customer_parts_file_start = time.time()

try:
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
        missing_customer_parts_path = 'SaveFiles/log_missing_cust_parts.json'
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
        save_json_file([], 'SaveFiles/log_missing_cust_parts.json')

except Exception as e:
    print(f"Error building missing customer parts file: {e}")
    # Create empty file on error
    save_json_file([], 'SaveFiles/log_missing_cust_parts.json')

t_customer_parts_file_end = time.time() - t_customer_parts_file_start

#-------------------------------------------------------------------#
#           Build missing customer parts designator file
#-------------------------------------------------------------------#
t_customer_parts_designator_file_start = time.time()

try:
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
        customer_designator_path = 'SaveFiles/log_cust_designator.json'
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
        save_json_file([], 'SaveFiles/log_cust_designator.json')

except Exception as e:
    print(f"Error building customer parts designator file: {e}")
    # Create empty file on error
    save_json_file([], 'SaveFiles/log_cust_designator.json')

t_customer_parts_designator_file_end = time.time() - t_customer_parts_designator_file_start

#-------------------------------------------------------------------#
#                      Build missing PCB file
#-------------------------------------------------------------------#
t_pcb_file_start = time.time()

try:
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
        pcb_status_path = 'SaveFiles/log_pcb_status.json'
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
        save_json_file([], 'SaveFiles/log_pcb_status.json')

except Exception as e:
    print(f"Error building PCB status file: {e}")
    # Create empty file on error
    save_json_file([], 'SaveFiles/log_pcb_status.json')

t_pcb_file_end = time.time() - t_pcb_file_start

#-------------------------------------------------------------------#
#                    Build missing stencil file
#-------------------------------------------------------------------#
t_stencil_file_start = time.time()

try:
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
        stencil_status_path = 'SaveFiles/log_stencil_status.json'
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
        save_json_file([], 'SaveFiles/log_stencil_status.json')

except Exception as e:
    print(f"Error building stencil status file: {e}")
    # Create empty file on error
    save_json_file([], 'SaveFiles/log_stencil_status.json')

t_stencil_file_end = time.time() - t_stencil_file_start

#-------------------------------------------------------------------#
#                       Build parts PO file
#-------------------------------------------------------------------#
t_parts_po_file_start = time.time()

try:
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
                blue_gradient_bar(idx + 1, total_active_jobs)
            
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
            po_numbers_path = 'SaveFiles/log_po_numbers.json'
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
            save_json_file([], 'SaveFiles/log_po_numbers.json')
            
    else:
        print("No active jobs data available to process PO numbers")
        # Create empty file
        save_json_file([], 'SaveFiles/log_po_numbers.json')

except Exception as e:
    print(f"Error building PO numbers file: {e}")
    # Create empty file on error
    save_json_file([], 'SaveFiles/log_po_numbers.json')

t_parts_po_file_end = time.time() - t_parts_po_file_start

#-------------------------------------------------------------------#
#                     Refine active jobs list
#-------------------------------------------------------------------#
t_refine_active_jobs_start = time.time()

try:
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

    # Save refined active jobs as Excel file for review
    refined_active_jobs_excel_path = os.path.join(save_dir, "refined_active_jobs.xlsx")
    pd.DataFrame(refined_active_jobs).to_excel(refined_active_jobs_excel_path, index=False)
    print(f"Refined active jobs Excel saved: {refined_active_jobs_excel_path}")

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

    # Define Smartsheet headers
    smartsheet_headers = [
        "Sales Order Date", "Due Date", "Turn", "WO#", "Quote #", "Customer", 
        "Date and Action", "spacer", "Pur Part", "Cus Part", "PCB", "Stencil", "spacer2",   
        "Purchase Order",  "Complete", "Purch Des", "Cust Des", "Released Date", "Internal Status"
    ]

    # Start with log_active_jobs.json as base
    update_rows = []
    for job in active_jobs_data:
        wo_number = job.get("WO#", "")
        user_data = wo_user_data.get(wo_number, {})
        pur_part_val = "P" if wo_number in wo_missing_purch else ""
        cus_part_val = "C" if wo_number in wo_missing_cust else ""
        pcb_val = "PCB" if pcb_status_dict.get(wo_number, "") != "Complete" else ""
        stencil_val = "ST" if stencil_status_dict.get(wo_number, "") != "Complete" else ""

        #TODO add turn time
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
            "Complete": user_data.get("Complete", ""),
            "Released Date": job.get("Action", ""),
            "Internal Status": job.get("internal_status", "")
        }
        update_rows.append(row)

    # Create DataFrame
    smartsheet_update_df = pd.DataFrame(update_rows, columns=smartsheet_headers)

    # Show sample for review
    print("Sample Smartsheet update DataFrame:")
    print(smartsheet_update_df.head())

except Exception as e:
    print(f"Error building Smartsheet update DataFrame: {e}")

t_smartsheet_df_end = time.time() - t_smartsheet_df_start

#-------------------------------------------------------------------#
#                         Update smartsheet
#-------------------------------------------------------------------#
t_smartsheet_update_start = time.time()

try:
    DELETE_BATCH_SIZE = 100
    ADD_BATCH_SIZE = 100

    # Get all row IDs from the current Smartsheet
    if '_row_id' in smartsheet_part_tracking_df.columns:
        all_row_ids = smartsheet_part_tracking_df['_row_id'].tolist()
    else:
        all_row_ids = []

    # Delete all rows in batches
    print(f"Deleting {len(all_row_ids)} rows from Smartsheet in batches of {DELETE_BATCH_SIZE}...")
    for i in range(0, len(all_row_ids), DELETE_BATCH_SIZE):
        batch_ids = all_row_ids[i:i+DELETE_BATCH_SIZE]
        if batch_ids:
            smartsheet_client.Sheets.delete_rows(assembly_part_tracking_id, batch_ids)
            print(f"Deleted rows {i+1} to {i+len(batch_ids)}")

    # Replace NaN values with empty strings before uploading to Smartsheet
    smartsheet_update_df = smartsheet_update_df.replace({np.nan: ""})

    # Prepare new rows for Smartsheet
    new_rows = []
    format_part_columns = ['Pur Part', 'Cus Part', 'PCB', 'Stencil']
    format_turn_column = 'Turn'
    format_due_column = 'Due Date'

    for idx, row in smartsheet_update_df.iterrows():
        cells = []
        for col in smartsheet_update_df.columns:
            col_id = smartsheet_sheet.columns[smartsheet_update_df.columns.get_loc(col)].id
            cell = smartsheet.models.Cell()
            cell.column_id = col_id
            cell.value = row[col]
            # Apply formatting for specified columns
            if col in format_part_columns:
                if row[col] == '':
                    cell.format = ",,,,,,,,14,14,,0,,,,0,"
                else:
                    cell.format = ",,,,,,,,2,19,,0,,,,0,"

            if col == format_turn_column:
                try:
                    turn_val = float(row[col])
                    if turn_val <= 5:
                        cell.format = ",,,,,,,,,9,,0,,,,0,"
                except (ValueError, TypeError):
                    pass

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
                    print(f"DEBUG: Exception in due date formatting: {ex}")
                    
            cells.append(cell)
        new_row = smartsheet.models.Row()
        new_row.to_top = True
        new_row.cells = cells
        new_rows.append(new_row)

    # Add new rows in batches
    print(f"Adding {len(new_rows)} rows to Smartsheet in batches of {ADD_BATCH_SIZE}...")
    for i in range(0, len(new_rows), ADD_BATCH_SIZE):
        batch_rows = new_rows[i:i+ADD_BATCH_SIZE]
        smartsheet_client.Sheets.add_rows(assembly_part_tracking_id, batch_rows)
        print(f"Added rows {i+1} to {i+len(batch_rows)}")

    print("Smartsheet update complete.")

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