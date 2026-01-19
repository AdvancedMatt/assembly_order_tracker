# Debug Mode Guide

## Overview
This application now includes comprehensive debug logging to help troubleshoot issues when running on the VM or in any environment.

## How to Enable Debug Mode

### Option 1: Edit defines.py
Open `defines.py` and change:
```python
DEBUG = False  # Change this to True
```

### Option 2: Set Log Level
In `defines.py`, you can also adjust the logging level:
```python
LOG_LEVEL = "DEBUG"  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
```

## What Debug Mode Does

When `DEBUG = True`:
1. **Detailed Logging**: Every major operation is logged with timestamps
2. **Debug Log File**: Creates `SaveFiles/debug_log.txt` with verbose information
3. **Error Tracebacks**: Full stack traces for all errors
4. **Network Operations**: Logs file access attempts, directory scans, etc.
5. **Database Queries**: Logs query execution details
6. **Smartsheet Operations**: Logs API calls, row counts, column mapping

## Log Files Location

All logs are stored in the `SaveFiles` directory:

- **`SaveFiles/error_log.txt`**: Always created, contains all errors and important info
- **`SaveFiles/debug_log.txt`**: Only created when DEBUG=True, contains verbose debugging info

Both files append new runs, so you have a history of executions.

## Reading the Logs

Log format:
```
2026-01-19 10:30:45 - INFO - [assembly_order_tracker.py:95] - Script started - DEBUG Mode: True
```

Components:
- **Timestamp**: When the log entry was created
- **Level**: INFO, DEBUG, WARNING, ERROR, CRITICAL
- **Source**: File and line number where log was generated
- **Message**: Descriptive information

## Common Issues and What to Look For

### 1. Network Directory Access Issues
**Error**: `Permission denied` or `Network directory does not exist`

**What to check in logs**:
```
ERROR - Network directory does not exist: \\\\server\\path
ERROR - Permission denied accessing network directory
```

**Solution**: 
- Verify VPN connection
- Check network path is correct
- Ensure you have read permissions

### 2. Smartsheet Connection Issues
**Error**: `Smartsheet API error`

**What to check in logs**:
```
ERROR - Smartsheet API error: API token invalid
ERROR - Error code: 1004
```

**Solutions**:
- Check API token is valid
- Verify network connectivity
- Confirm sheet ID is correct

### 3. Empty Smartsheet
**Warning**: Sheet loads but is empty

**What to check in logs**:
```
WARNING: Smartsheet DataFrame is EMPTY! This might be expected if starting fresh.
```

**Solutions**:
- This is normal for new/cleared sheets
- Check if data should exist
- Verify you're connecting to correct sheet

### 4. Missing camReadme.txt Files
**Error**: No job data found

**What to check in logs**:
```
WARNING: No job data found in camReadme.txt files!
INFO - Found 0 directories to scan
```

**Solutions**:
- Check network directory path
- Verify job folders contain camReadme.txt
- Check file permissions

### 5. Database Connection Failures
**Error**: Cannot connect to SQL Server

**What to check in logs**:
```
ERROR - Database operational error: Unable to connect to server
ERROR - This could mean: wrong server name, network issues, or authentication failure
```

**Solutions**:
- Verify database server is accessible
- Check credentials
- Ensure SQL Server is running

## Remote Troubleshooting Workflow

Since you're running this on a VM over slow VPN:

1. **Enable DEBUG mode** before compiling
2. **Run the EXE** on the VM
3. **Download log files** from VM:
   - `SaveFiles/error_log.txt`
   - `SaveFiles/debug_log.txt` (if DEBUG was enabled)
4. **Review logs locally** to identify the issue
5. **Fix the issue** and recompile
6. **Upload new EXE** and test

## Performance Considerations

Debug mode adds overhead:
- More disk I/O for logging
- Slightly slower execution
- Larger log files

**Recommendation**: Use DEBUG mode only when troubleshooting. Set to `False` for production runs.

## Quick Debug Checklist

Before compiling for VM deployment:

- [ ] Set `DEBUG = True` in defines.py if troubleshooting
- [ ] Verify all file paths are correct for VM environment
- [ ] Check network paths use correct server names
- [ ] Ensure SaveFiles directory will be writable on VM
- [ ] Compile the EXE
- [ ] Test locally if possible
- [ ] Deploy to VM
- [ ] Run and collect logs
- [ ] Review logs for errors

## Log File Management

Logs append to existing files, so they grow over time. To clean up:

1. Delete old log files manually
2. Or implement rotation (files get renamed/archived)
3. Current setup: Error logs persist indefinitely

**Tip**: Before each major troubleshooting session, delete old logs to start fresh.

## Getting Help

When reporting issues, always include:
1. The error message shown in console
2. Relevant sections from error_log.txt
3. Debug_log.txt if DEBUG was enabled
4. What operation was being performed when error occurred
5. Any recent changes to configuration

## Example Debug Session

```bash
# 1. Enable debug mode
# Edit defines.py: DEBUG = True

# 2. Compile
pyinstaller assembly_order_tracker.spec

# 3. Deploy to VM
# (Use FTP/copy method)

# 4. Run on VM
assembly_order_tracker.exe

# 5. Copy logs back
# Copy SaveFiles/*.txt to local machine

# 6. Review logs
# Open with text editor, search for "ERROR" or "WARNING"

# 7. Fix issues and repeat
```

## Advanced: Filtering Logs

To find specific issues in large log files:

**Windows (PowerShell)**:
```powershell
# Find all errors
Select-String -Path "SaveFiles\error_log.txt" -Pattern "ERROR"

# Find errors from a specific date
Select-String -Path "SaveFiles\error_log.txt" -Pattern "2026-01-19.*ERROR"

# Find database-related errors
Select-String -Path "SaveFiles\error_log.txt" -Pattern "database|SQL"
```

**Linux/Mac**:
```bash
# Find all errors
grep "ERROR" SaveFiles/error_log.txt

# Find errors from a specific date
grep "2026-01-19.*ERROR" SaveFiles/error_log.txt

# Find database-related errors
grep -i "database\|sql" SaveFiles/error_log.txt
```

## Summary

The enhanced debug system provides:
- ✅ Comprehensive error tracking
- ✅ Detailed operation logging
- ✅ File-based logs for remote troubleshooting
- ✅ Configurable verbosity
- ✅ Better error messages with context
- ✅ Stack traces for all failures

This should significantly reduce troubleshooting time when issues occur on the VM!
