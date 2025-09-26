debug_output = False

# Filter out records where 'Status' in the excluded statuses list
excluded_statuses = [
    'Closed', 'Shipped', 'Closed Short',
    'Prog-Q', 'Prog-Done', 'Outside Hold', 'SMT-Setup', 'Hold-Floor', 'Floor',
    'Ship-Partial', 'Close Short', 'CAM Hold', 'Packaging', 'Thruhole',
    'Outside-Prog', 'Programming', 'QC Inspection', 'Selective Solder',
    'SMT-Done', 'FA-Thruhole', 'Cancelled'
]

bar_len = 100 # Progress bar length

delete_batch_size = 240  # Delete smartsheet batch size
insert_batch_size = 450  # Insert smartsheet batch size

column_mapping = {
    'Order Date': 'Sales Order Date',
    'WO#': 'WO#',
    'Quote#': 'Quote #',
    'Customer': 'Customer',
    'Ref Des': 'Ref Des',
    'Purchase Parts Rec Complete': 'Pur Part',
    'Customer Supplied Parts Rec Complete': 'Cus Part'
}