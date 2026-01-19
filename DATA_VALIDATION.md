# Data Validation & Sanitization

## Overview

The script now automatically detects and corrects invalid data in camReadme.txt files. This handles cases where users enter text values (like "Re-make", "CAS") in fields that should contain numbers or dates.

## What Gets Validated

### Numeric Fields
These fields should contain numbers (integers or floats):
- `Line Items`
- `SMT`
- `FP`
- `TH`
- `BGA`
- `Qty`
- `Qty Shipped`
- `Turn`
- `Quote #`
- `Mfg Qty`

**Invalid Values**: Any text like "Re-make", "CAS", "N/A", etc.
**Correction**: Set to `0`

### Date Fields
These fields should contain valid dates:
- `Sales Order Date`
- `Due Date`
- `Date Released`
- `Ship Date`

**Invalid Values**: Text, malformed dates, impossible dates
**Correction**: Set to `1980-01-01`

## How It Works

1. **Data is loaded** from camReadme.txt files
2. **Validation runs automatically** checking each field
3. **Invalid data is corrected** using default values
4. **Corrections are logged** with full details
5. **A report is generated** showing all changes made

## Where to Find Correction Information

### Console Output
When the script runs, you'll see:
```
⚠ WARNING: Found 15 invalid data entries that were corrected:
  WO# 57311_22 | Peregrine Avionics - 5 field(s) corrected
  WO# 63373 | Siemens Industry Software Inc. - 5 field(s) corrected
  See SaveFiles/error_log.txt for details
```

### Log Files
**SaveFiles/error_log.txt** contains detailed correction information:
```
2026-01-19 10:30:45 - WARNING - Corrected numeric field: Record 202 | WO# 57311_22 | 
    Field 'Line Items': 'CAS' → 0
2026-01-19 10:30:45 - WARNING - Corrected numeric field: Record 202 | WO# 57311_22 | 
    Field 'SMT': 'CAS' → 0
```

**SaveFiles/data_corrections.json** contains structured correction data:
```json
[
  {
    "Record": 202,
    "WO#": "57311_22",
    "Customer": "Peregrine Avionics",
    "Field": "Line Items",
    "Original_Value": "CAS",
    "Corrected_Value": 0,
    "Type": "Numeric"
  }
]
```

## Example Corrections

### Before (Invalid):
```
WO#: 57311_22
Line Items: CAS
SMT: CAS
FP: CAS
TH: CAS
BGA: CAS
```

### After (Corrected):
```
WO#: 57311_22
Line Items: 0
SMT: 0
FP: 0
TH: 0
BGA: 0
```

## Impact on Processing

### For Numeric Fields:
- **Setting to 0** means these jobs won't count toward totals
- **This prevents calculation errors** that would crash the script
- **Values are still tracked** so you know something needs attention

### For Date Fields:
- **Setting to 1980-01-01** makes them obviously invalid
- **Date calculations still work** without crashing
- **Old date stands out** in reports for easy identification

## User Training Recommendations

### What Users Should Enter

**✅ CORRECT - Numeric Fields:**
- `5` (integer)
- `10.5` (decimal)
- Leave blank if not applicable

**❌ INCORRECT - Numeric Fields:**
- `Re-make` (text)
- `CAS` (text)
- `N/A` (text)
- `TBD` (text)

**✅ CORRECT - Date Fields:**
- `2026-01-19` (ISO format)
- `01/19/2026` (US format)
- `1/19/26` (short format)
- Leave blank if not applicable

**❌ INCORRECT - Date Fields:**
- `Re-make` (text)
- `TBD` (text)
- `Pending` (text)

### Alternative: Use Comment Fields

If users need to track special statuses like "Re-make" or "CAS":
1. **Use a designated comment/note field** (not shown in your examples)
2. **Enter proper numeric/date values** in the data fields
3. **Or create custom status fields** that expect text values

## Monitoring Corrections

### Daily Review
Check `SaveFiles/data_corrections.json` regularly to:
- Identify users who need training
- Find systemic data entry issues
- Update user documentation

### Automated Alerts (Future Enhancement)
Could add:
- Email notification when corrections are made
- Dashboard showing correction trends
- User-specific correction reports

## Configuration

To change default correction values, edit [defines.py](defines.py):

```python
# Default values for invalid data
DEFAULT_NUMERIC_VALUE = 0      # Change to different number if needed
DEFAULT_DATE_VALUE = '1980-01-01'  # Change to different date if needed
```

To add more fields to validation lists:

```python
# Fields that should be numeric (int or float)
NUMERIC_FIELDS = [
    'Line Items', 'SMT', 'FP', 'TH', 'BGA', 'Qty',
    'Your_New_Field_Here'  # Add here
]

# Fields that should be dates
DATE_FIELDS = [
    'Sales Order Date', 'Due Date',
    'Your_New_Date_Field'  # Add here
]
```

## Technical Details

### Validation Function
Location: [functions.py](functions.py) - `sanitize_cam_data()`

The function:
1. Iterates through each field in the validation lists
2. Attempts to convert values to appropriate types
3. Catches conversion errors
4. Replaces invalid values with defaults
5. Logs all changes made
6. Returns both corrected data and correction report

### Safe Conversion Utilities
Two helper functions prevent crashes during calculations:
- `safe_float(value, default=0.0)` - Safely converts to float
- `safe_int(value, default=0)` - Safely converts to integer

These can be used anywhere in code where numeric conversion might fail:
```python
# Instead of:
quantity = float(row['Qty'])  # Could crash with "Re-make"

# Use:
quantity = safe_float(row['Qty'], default=0)  # Always safe
```

## Benefits

### ✅ Script Reliability
- No more crashes from invalid data
- Calculations complete successfully
- Smartsheet updates work properly

### ✅ Data Quality Visibility
- All corrections are logged
- Easy to identify problem areas
- Can track correction trends over time

### ✅ User Friendly
- Script continues running despite bad data
- Clear warnings about what was corrected
- Data still usable (with default values)

### ✅ Maintainable
- Easy to add new fields to validate
- Configurable default values
- Detailed logging for troubleshooting

## Summary

The data validation system:
- 🛡️ **Protects** the script from crashing due to invalid user input
- 📝 **Documents** all corrections for review
- ⚙️ **Automatically fixes** common data entry mistakes
- 📊 **Maintains** data processing integrity
- 🎓 **Helps identify** training opportunities

Users can continue their current workflow while you gradually improve data entry practices!
