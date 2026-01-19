# Debug Enhancement Summary

## Changes Made to Assembly Order Tracker

### Date: January 19, 2026

This document summarizes all changes made to add comprehensive debugging and error handling capabilities.

---

## 1. Configuration Changes (defines.py)

### Added:
- `DEBUG = False` - Master debug flag
- `LOG_LEVEL = "INFO"` - Configurable logging level
- `ERROR_LOG_PATH = "SaveFiles/error_log.txt"` - Error log file location
- `DEBUG_LOG_PATH = "SaveFiles/debug_log.txt"` - Debug log file location

### Usage:
Set `DEBUG = True` to enable verbose debug logging before compiling for the VM.

---

## 2. Main Script Changes (assembly_order_tracker.py)

### Added:
- **Comprehensive logging setup** at script start
- **File and console logging handlers**
- **Timestamped log entries** with file/line information
- **Import of traceback module** for detailed error reporting

### Enhanced Error Handling For:

#### Smartsheet Operations:
- API connection failures
- Authentication errors
- Empty sheet warnings
- Row/column parsing errors
- Specific error types (ApiError vs general exceptions)

#### Network/File Operations:
- Network directory access verification
- Permission errors
- File not found errors
- JSON parsing errors
- Directory listing failures

#### Data Processing:
- Empty DataFrame warnings
- Missing data alerts
- Processing failures at each stage

### Each Section Now Has:
- Try-except blocks with specific error types
- Logger calls for INFO, DEBUG, WARNING, ERROR levels
- Full traceback logging for all exceptions
- User-friendly console messages
- Graceful degradation (continues when possible)

---

## 3. Functions Module Changes (functions.py)

### Added:
- Logger import and initialization
- Traceback module import

### Enhanced Functions:

#### `get_api_key_file()`:
- Detailed logging of file read operations
- Specific error messages for decryption failures
- File path logging

#### `convert_sheet_to_dataframe()`:
- Row and column count logging
- Progress tracking
- Column name verification
- Error handling with context

#### `load_assembly_job_data()`:
- Network directory validation
- Permission checking before access
- File-by-file error handling
- Statistics tracking (files read, cached, errors)
- Detailed error messages for each failure
- Cache hit/miss logging

#### `update_smartsheet()`:
- Batch operation logging
- Row deletion tracking
- Row addition tracking
- Cell formatting error handling
- Detailed failure context
- Progress updates

### Error Handling Improvements:
- Each file operation wrapped in try-except
- OS errors caught specifically (PermissionError, FileNotFoundError)
- JSON decode errors handled gracefully
- Detailed error context in logs

---

## 4. Database Module Changes (database_utils.py)

### Enhanced:

#### `connect()`:
- Specific pyodbc error types caught
- Interface errors (driver issues)
- Operational errors (connection issues)
- Helpful error messages for each type

#### `execute_query()`:
- Query logging (first 100 chars)
- Parameter logging
- Row count reporting
- Programming errors (SQL syntax)
- Data errors (type mismatches)
- Full query logging on error

---

## 5. New Features Added

### 1. Debug Mode
- Toggle verbose logging with single flag
- Separate debug log file when enabled
- Console and file output

### 2. Error Log File
- Always created, even in non-debug mode
- Append mode - keeps history
- Timestamped entries
- Searchable format

### 3. Enhanced Error Messages
- Context about what was being attempted
- File paths involved
- Network locations
- Data counts and states
- Suggestions for common issues

### 4. Graceful Degradation
- Script continues when possible
- Initializes empty data structures on failure
- Allows partial completion
- Clear indication of what failed

### 5. Performance Tracking
- Each section logs completion time
- File operation counts
- Row processing statistics

---

## 6. Benefits for Remote Troubleshooting

### Before:
- Generic error messages
- No log files
- Hard to diagnose remote issues
- Needed to guess at failure points

### After:
- Detailed log files to download
- Exact line numbers where errors occur
- Full context of what was being processed
- Stack traces for all failures
- Can diagnose issues without rerunning

---

## 7. Code Quality Improvements

### Added Safety:
- ✅ Better null checks
- ✅ Type verification
- ✅ File existence checks
- ✅ Permission verification
- ✅ Empty data handling
- ✅ Network connectivity checks

### Added Visibility:
- ✅ What operation is running
- ✅ How much data is being processed
- ✅ When errors occur
- ✅ What data was involved
- ✅ Why operations failed

### Added Robustness:
- ✅ Specific exception handling
- ✅ Resource cleanup
- ✅ Graceful failures
- ✅ Continuation where possible
- ✅ Clear error reporting

---

## 8. Usage Instructions

### For Development:
1. Set `DEBUG = True` in defines.py
2. Run locally to see detailed output
3. Review debug_log.txt for all operations

### For VM Deployment:
1. Set `DEBUG = True` for troubleshooting runs
2. Set `DEBUG = False` for production runs
3. Compile with PyInstaller
4. Deploy to VM
5. Run application
6. Download log files from SaveFiles/
7. Review for errors
8. Fix issues and redeploy

### For Normal Operations:
1. Keep `DEBUG = False`
2. Monitor error_log.txt periodically
3. Review for WARNING or ERROR entries
4. Enable DEBUG if issues arise

---

## 9. Files Modified

1. **defines.py** - Added debug configuration
2. **assembly_order_tracker.py** - Added logging setup and error handling
3. **functions.py** - Enhanced all major functions with logging
4. **database_utils.py** - Improved database error handling

## 10. Files Created

1. **DEBUG_GUIDE.md** - Comprehensive guide for using debug features
2. **DEBUG_CHANGES.md** - This summary document

---

## 11. Testing Recommendations

### Before Deploying to VM:

1. **Test with DEBUG = True locally**
   - Verify logs are created
   - Check log formatting
   - Ensure all operations log correctly

2. **Test error scenarios**:
   - Wrong network path
   - Invalid Smartsheet credentials
   - Empty data directories
   - Database connection failure

3. **Test with DEBUG = False**:
   - Verify error_log still works
   - Check performance impact is minimal
   - Ensure user sees appropriate messages

4. **Compile and test EXE**:
   - Run compiled version locally
   - Verify logs are created in correct location
   - Test with real data

5. **Deploy to VM**:
   - Run once with DEBUG = True
   - Review logs
   - Switch to DEBUG = False if working

---

## 12. Maintenance Notes

### Log File Management:
- Logs append indefinitely
- Consider periodic cleanup
- Large logs may slow text editors
- Can be opened in chunks if needed

### Future Enhancements:
- [ ] Log rotation (automatic cleanup)
- [ ] Log file size limits
- [ ] Email notifications for errors
- [ ] Dashboard for log viewing
- [ ] Automatic error reporting

### Performance Impact:
- Minimal when DEBUG = False
- Moderate when DEBUG = True
- Log file I/O is main overhead
- No impact on algorithm performance

---

## 13. Quick Reference

### Enable Debug Mode:
```python
# In defines.py
DEBUG = True
```

### Log File Locations:
```
SaveFiles/error_log.txt  # Always created
SaveFiles/debug_log.txt  # Only when DEBUG = True
```

### Find Errors in Logs:
```powershell
# PowerShell
Select-String -Path "SaveFiles\error_log.txt" -Pattern "ERROR"
```

### Common Issues:
1. Network directory access → Check VPN, permissions
2. Smartsheet API errors → Check token, sheet ID
3. Empty data → Check source directories
4. Database errors → Check connection, credentials

---

## Summary

The application now has enterprise-grade error handling and logging:

- ✅ **Comprehensive**: Every major operation is logged
- ✅ **Configurable**: Toggle debug mode with one flag
- ✅ **Detailed**: Full context for all errors
- ✅ **Remote-Friendly**: Log files for offline analysis
- ✅ **User-Friendly**: Clear console messages
- ✅ **Maintainable**: Easy to add more logging
- ✅ **Production-Ready**: Minimal overhead when debug is off

This should significantly improve your ability to troubleshoot issues on the remote VM!
