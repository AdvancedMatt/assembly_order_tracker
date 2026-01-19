# Quick Start: Enable Debug Mode

## For Immediate Troubleshooting

### Step 1: Enable Debug Mode (30 seconds)

Open `defines.py` and change line 2:

```python
# Change this line:
DEBUG = False

# To this:
DEBUG = True
```

Save the file.

### Step 2: Compile the EXE (if needed)

```powershell
pyinstaller assembly_order_tracker.spec
```

The EXE will be in the `dist/` folder.

### Step 3: Run on VM

Transfer the EXE to the VM and run it. Watch the console output.

### Step 4: Get the Logs

After the script runs (or fails), download these files from the VM:

```
SaveFiles/error_log.txt
SaveFiles/debug_log.txt
```

### Step 5: Find the Problem

Open the log files and search for:
- `ERROR` - Critical failures
- `WARNING` - Potential issues
- The last few lines before script stopped

Common error patterns:

**Network Issues:**
```
ERROR - Network directory does not exist
ERROR - Permission denied accessing network directory
```
→ Check VPN connection and network paths

**Smartsheet Issues:**
```
ERROR - Smartsheet API error
ERROR - Error code: 1004
```
→ Check API token and sheet ID

**Database Issues:**
```
ERROR - Database operational error
ERROR - Unable to connect to server
```
→ Check database credentials and server access

**Empty Data:**
```
WARNING: No job data found in camReadme.txt files!
WARNING: Smartsheet DataFrame is EMPTY!
```
→ Check if data exists in source locations

### Step 6: Fix and Redeploy

1. Fix the identified issue
2. Optionally set `DEBUG = False` for production
3. Recompile if needed
4. Deploy and test

---

## What Each Log Level Shows

**DEBUG = False (Production Mode):**
- Errors and critical warnings only
- Basic operation status
- Minimal file output
- Faster execution

**DEBUG = True (Troubleshooting Mode):**
- Every operation logged
- File access attempts
- Data counts and structures
- Function entry/exit
- More detailed errors
- Slower execution (due to extra logging)

---

## Quick Troubleshooting Checklist

If the script fails:

- [ ] Check `SaveFiles/error_log.txt` for the error message
- [ ] Note what operation was happening when it failed
- [ ] Check if network directories are accessible
- [ ] Verify Smartsheet credentials are valid
- [ ] Ensure database server is reachable
- [ ] Check if source data exists

If you need more detail:

- [ ] Enable DEBUG mode
- [ ] Rerun the script
- [ ] Check `SaveFiles/debug_log.txt`
- [ ] Look for the exact operation that failed
- [ ] Review the data it was trying to process

---

## Emergency: Script Won't Run at All

If the EXE won't start or crashes immediately:

1. Enable DEBUG mode
2. Run from PowerShell to see error messages:
   ```powershell
   cd "path\to\exe"
   .\assembly_order_tracker.exe
   ```
3. Note the error in PowerShell
4. Check if SaveFiles directory was created
5. Check if any log file was written
6. Look for Python runtime errors

---

## Tips

- **Always enable DEBUG for first run on VM** - Catches setup issues
- **Disable DEBUG for scheduled/automated runs** - Saves time and disk space
- **Keep old log files for a few days** - Helps track recurring issues
- **Search logs for ERROR first** - Most critical issues
- **Then check for WARNING** - Potential problems

---

## Contact/Support

When asking for help, provide:

1. The console output (what you see when running)
2. The `error_log.txt` file
3. The `debug_log.txt` file (if DEBUG was enabled)
4. What you were trying to do
5. What error message you saw

This gives complete context to diagnose the issue!

---

## Remember

✅ **DEBUG = True** → Troubleshooting  
✅ **DEBUG = False** → Production  
✅ Log files are in **SaveFiles/**  
✅ Logs **append**, they don't overwrite  
✅ **Download logs from VM** to review locally
