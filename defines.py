# Debug and logging configuration
debug_output = False
DEBUG = False  # Set to True to enable detailed debug logging
LOG_LEVEL = "WARNING"  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
ERROR_LOG_PATH = "SaveFiles/error_log.txt"
DEBUG_LOG_PATH = "SaveFiles/debug_log.txt"

# Smartsheet columns to store - used to restor smartsheet during update
user_entered_columns = [
    'Date and Action', 'Additional Notes'
]

# Filter out records where 'Status' in the excluded statuses list
excluded_statuses = [
    'Closed', 'Shipped', 'Closed Short', 'Cancelled'
]

# excluded_statuses = [
#     'Closed', 'Shipped', 'Closed Short',
#     'Prog-Q', 'Prog-Done', 'Outside Hold', 'SMT-Setup', 'Hold-Floor', 'Floor',
#     'Ship-Partial', 'Close Short', 'CAM Hold', 'Packaging', 'Thruhole',
#     'Outside-Prog', 'Programming', 'QC Inspection', 'Selective Solder',
#     'SMT-Done', 'FA-Thruhole', 'Cancelled'
# ]

# Define Smartsheet parameters
smartsheet_headers = [
    "Sales Order Date", "Due Date", "Turn", "WO#", "Quote #", "Customer", 
    "Date and Action", "spacer", "Pur Part", "Cus Part", "PCB", "Stencil", "spacer2",   
    "Purchase Order", "Purch Des", "Cust Des", "Additional Notes", "Refresh Date", "Refresh Time"
]

DELETE_BATCH_SIZE = 100
ADD_BATCH_SIZE = 100

# Progress bar settings
USE_COLOR_PROGRESS_BAR = True  # Set to False for basic ASCII progress bar without colors

bar_len = 100 # Progress bar length

delete_batch_size = 240  # Delete smartsheet batch size
insert_batch_size = 450  # Insert smartsheet batch size

color_options = [
            (139, 0, 0),    # Dark Red
            (0, 100, 0),    # Dark Green  
            (0, 0, 139),    # Dark Blue (original)
            (128, 0, 128),  # Purple
            (255, 140, 0),  # Dark Orange
            (220, 20, 60),  # Crimson
            (0, 139, 139),  # Dark Cyan
            (139, 69, 19),  # Saddle Brown
            (75, 0, 130),   # Indigo
            (0, 128, 0),    # Green
            (184, 134, 11), # Dark Goldenrod
            (128, 128, 0),  # Olive
        ]

# Data validation configuration
# Fields that should be numeric (int or float)
NUMERIC_FIELDS = [
    'Line Items', 'SMT', 'FP', 'TH', 'BGA', 'Qty', 'Qty Shipped',
    'Turn', 'Quote #', 'Mfg Qty'
]

# Fields that should be dates
DATE_FIELDS = [
    'Sales Order Date', 'Due Date', 'Date Released', 'Ship Date'
]

# Default values for invalid data
DEFAULT_NUMERIC_VALUE = 0
DEFAULT_DATE_VALUE = '1980-01-01'