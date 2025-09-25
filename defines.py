debug_output = False

num_days_ago = 4 # Days ago to consider for filtering 
num_days_future = 4 # Days in the future to consider for filtering

bar_len = 40 # Progress bar length

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