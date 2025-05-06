import os
import subprocess
import pandas as pd
from datetime import datetime, timedelta
import sys

# --- Configuration ---
SCRIPT_DIR = r"C:\Scripts\hulft_connect" # Base directory for all scripts/logs
POWERSHELL_EXE = "powershell.exe"       # Path to PowerShell executable

# Paths to the scripts and files
# !!  !!
ps_script_path = os.path.join(SCRIPT_DIR, "hulcon.ps1") 
csv_log_path = os.path.join(SCRIPT_DIR, "NetworkDiagnostics_Log.csv")
# !! UPDATE this path if your plotter script has a different name !!
plotter_script_path = os.path.join(SCRIPT_DIR, "NDP.py") # UPDATED FILENAME based on user context
# !! UPDATE this path if your VBScript has a different name !!
vbs_script_path = os.path.join(SCRIPT_DIR, "SendEmail2Admin_HTML.vbs") # UPDATED FILENAME based on user context
graph_output_path = os.path.join(SCRIPT_DIR, "network_status_graph.png") # Where to save the graph

# Email Configuration
EMAIL_SUBJECT_FAILURE = "HULFT Network Alert: RDP or Outbound Connectivity Issues Detected" # Updated Subject
EMAIL_BODY_FAILURE = "Critical network checks failed (RDP Listen or Outbound).|See attached graph for details.|Timestamp: {timestamp}" # Use '|' for newlines in VBScript

# --- Helper Functions ---

def check_file_exists(file_path, file_description):
    """Checks if a file exists and prints an error if not."""
    if not os.path.exists(file_path):
        print(f"CRITICAL ERROR: {file_description} not found at '{file_path}'. Please check the path.", file=sys.stderr)
        return False
    return True

def run_powershell_script(script_path):
    """Executes the PowerShell network check script."""
    print(f"Running PowerShell script: {script_path}...")
    # ** PERMISSION NOTE **: This script needs permission to write to '{csv_log_path}'
    # Consider running the orchestrator as Administrator or adjusting folder permissions.
    try:
        command = [POWERSHELL_EXE, "-ExecutionPolicy", "Bypass", "-NoProfile", "-File", script_path]
        result = subprocess.run(command, capture_output=True, text=True, encoding='utf-8', errors='ignore')

        print("--- PowerShell Script Output ---")
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            # Print stderr output regardless of exit code
            print(f"PowerShell Errors/Warnings:\n{result.stderr}", file=sys.stderr)
        print("--- End PowerShell Output ---")

        # --- Improved Error Checking ---
        success = True # Assume success initially
        if result.returncode != 0:
            print(f"Error: PowerShell script exited with non-zero code {result.returncode}", file=sys.stderr)
            success = False # Non-zero exit code indicates failure

        # Check stderr for known critical errors even if exit code is 0
        # Modify the string below if your PowerShell script's FATAL error message changes
        if "FATAL: Failed to write results to CSV" in result.stderr or "Access to the path" in result.stderr and "is denied" in result.stderr:
             print("Critical Error detected in PowerShell stderr: Failed to write CSV (likely permissions issue).", file=sys.stderr)
             success = False # Treat CSV write failure as critical

        if not success:
             print("PowerShell script execution failed or encountered critical errors.", file=sys.stderr)
        else:
             print("PowerShell script execution completed (but check output for warnings).")

        return success # Return True only if no critical errors detected

    except FileNotFoundError:
        print(f"Error: PowerShell executable not found at '{POWERSHELL_EXE}'", file=sys.stderr)
        return False
    except Exception as e:
        print(f"An error occurred while running the PowerShell script: {e}", file=sys.stderr)
        return False

def analyze_results(csv_path):
    """
    Analyzes the latest results in the CSV log file.
    Determines if specific critical checks (RDP Listen or Outbound) failed.
    

    Returns:
        tuple: (send_alert, latest_timestamp_str)
               send_alert (bool): True if critical checks failed, False otherwise.
               latest_timestamp_str (str): Timestamp of the latest check, or current time if no data/error.
    """
    print(f"Analyzing results from: {csv_path}...")
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    stale_data_threshold_minutes = 5 # How old data can be before warning
    send_alert = False # Default to False

    try:
        # --- Check File Existence and Modification Time ---
        if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
            print("Warning: CSV log file not found or is empty. Cannot determine status accurately.", file=sys.stderr)
            # 
            return False, now_str

        try:
            file_mod_time_unix = os.path.getmtime(csv_path)
            file_mod_time = datetime.fromtimestamp(file_mod_time_unix)
            time_diff = datetime.now() - file_mod_time
            if time_diff > timedelta(minutes=stale_data_threshold_minutes):
                print(f"Warning: CSV file was last modified at {file_mod_time}, which is more than {stale_data_threshold_minutes} minutes ago.", file=sys.stderr)
                print("Analysis might be based on stale data due to potential upstream errors (like CSV write failure).", file=sys.stderr)
                # 
        except Exception as e:
            print(f"Warning: Could not check CSV modification time: {e}", file=sys.stderr)
        # --- End Modification Time Check ---

        df = pd.read_csv(csv_path)
        if df.empty:
             print("Warning: CSV log file is empty after loading. Cannot determine status.", file=sys.stderr)
             return False, now_str

        # Convert Timestamp and Success columns, handling potential errors
        try:
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])
            # Convert TargetPort safely to numeric, coercing errors to NaN
            df['TargetPort'] = pd.to_numeric(df['TargetPort'], errors='coerce')
        except Exception as e:
            print(f"Error converting CSV columns ('Timestamp', 'TargetPort'): {e}. Cannot reliably determine status.", file=sys.stderr)
            return False, now_str # Treat data conversion errors as unable to determine status

        if df['Success'].dtype == 'object':
             df['Success'] = df['Success'].astype(str).str.lower().map({'true': True, 'false': False, 'yes': True, 'no': False})
        # Use nullable boolean type, fill parse errors/NaNs as False (failure)
        df['Success'] = df['Success'].astype('boolean').fillna(False)

        # Find the latest timestamp in the log
        if df['Timestamp'].isna().all():
             print("Error: All 'Timestamp' values are invalid after conversion.", file=sys.stderr)
             return False, now_str
        latest_timestamp = df['Timestamp'].max()
        latest_timestamp_str = latest_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        print(f"Latest check timestamp found in log: {latest_timestamp_str}")

        # Filter results for the latest timestamp
        latest_results = df[df['Timestamp'] == latest_timestamp]

        if latest_results.empty:
            print(f"Warning: No results found matching the latest timestamp ({latest_timestamp}). Cannot determine status.", file=sys.stderr)
            return False, latest_timestamp_str # Treat as unable to determine

        # --- Apply Specific Failure Logic ---
        # Find ALL failures in the latest run first
        all_failures = latest_results[latest_results['Success'] == False]

        if all_failures.empty:
            print(f"Network Status OK: All checks in the latest run ({latest_timestamp_str}) were successful.")
            send_alert = False
        else:
            # Check if any of these failures meet the CRITICAL criteria for alerting
            # Criterion 1: RDP Listen Check Failure (Inbound, Port 3389)
            is_rdp_failure = not all_failures[
                (all_failures['CheckType'] == 'Inbound Listen Check') &
                (all_failures['TargetPort'] == 3389.0) # Ensure float comparison if TargetPort was coerced
            ].empty

            # Criterion 2: Any Outbound Check Failure (TCP or ICMP)
            is_outbound_failure = not all_failures[
                all_failures['CheckType'].isin(["Outbound TCP", "Outbound ICMP"])
            ].empty

            # Determine if an alert should be sent
            send_alert = is_rdp_failure or is_outbound_failure

            if send_alert:
                print(f"ALERT Condition Met: Critical check(s) failed in the latest run ({latest_timestamp_str}):")
                # Filter all_failures to show only the ones meeting the criteria
                critical_failures = all_failures[
                    ((all_failures['CheckType'] == 'Inbound Listen Check') & (all_failures['TargetPort'] == 3389.0)) |
                    (all_failures['CheckType'].isin(["Outbound TCP", "Outbound ICMP"]))
                ]
                # Print details for the critical failures causing the alert
                for index, row in critical_failures.head(10).iterrows(): # Limit printing details
                     port_str = f":{int(row['TargetPort'])}" if pd.notna(row['TargetPort']) else ""
                     details = f"Check: {row.get('CheckName', 'N/A')}, Target: {row.get('TargetHost', 'N/A')}{port_str}"
                     if pd.notna(row.get('Details')):
                         details += f", Details: {row.get('Details')}"
                     print(f"  - {details}")
                if len(critical_failures) > 10:
                    print(f"  ... and {len(critical_failures)-10} more critical failures.")
            else:
                # Failures occurred, but none were the specific critical ones
                print(f"Network Warning: Non-critical check(s) failed in the latest run ({latest_timestamp_str}). No alert triggered.")
                # Optionally print details of non-critical failures here
                non_critical_failures = all_failures[
                    ~(((all_failures['CheckType'] == 'Inbound Listen Check') & (all_failures['TargetPort'] == 3389.0)) |
                      (all_failures['CheckType'].isin(["Outbound TCP", "Outbound ICMP"])))
                ]
                for index, row in non_critical_failures.head(5).iterrows(): # Limit printing details
                     port_str = f":{int(row['TargetPort'])}" if pd.notna(row['TargetPort']) else ""
                     details = f"Check: {row.get('CheckName', 'N/A')}, Target: {row.get('TargetHost', 'N/A')}{port_str}"
                     if pd.notna(row.get('Details')):
                         details += f", Details: {row.get('Details')}"
                     print(f"  - [Non-Alerting Failure] {details}")
                if len(non_critical_failures) > 5:
                    print(f"  ... and {len(non_critical_failures)-5} more non-critical failures.")

        return send_alert, latest_timestamp_str

    except pd.errors.EmptyDataError:
        print("Warning: CSV log file is empty or contains no data after loading.", file=sys.stderr)
        return False, now_str # Cannot determine status
    except KeyError as e:
        print(f"Error: Missing expected column in CSV for analysis: {e}", file=sys.stderr)
        return False, now_str # Cannot determine status
    except Exception as e:
        print(f"Error analyzing CSV file '{csv_path}': {e}", file=sys.stderr)
        return False, now_str # Treat analysis errors as unable to determine status reliably

def generate_graph(plotter_script, csv_log, output_path, days_to_display=None): # Added days_to_display
    """Runs the Python plotting script to save the graph."""
    print(f"Generating graph using: {plotter_script}...")
    try:
        if not os.path.exists(plotter_script):
             print(f"Error: Plotter script not found at '{plotter_script}'", file=sys.stderr)
             return False

        command = [sys.executable, plotter_script, csv_log, "-o", output_path]
        # <--- ADD DAYS ARGUMENT TO COMMAND --->
        if days_to_display is not None:
            command.extend(["-d", str(days_to_display)])
        # <--- END OF ADDITION --->

        result = subprocess.run(command, capture_output=True, text=True, check=False, encoding='utf-8', errors='ignore')

        print("--- Plotter Script Output ---")
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(f"Plotter Errors/Warnings:\n{result.stderr}", file=sys.stderr)
        print("--- End Plotter Output ---")

        if result.returncode != 0:
            print(f"Warning: Plotter script exited with code {result.returncode}. Graph might not be generated correctly.", file=sys.stderr)

        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
             print(f"Graph saved successfully to {output_path}")
             return True
        else:
             print(f"Error: Graph file '{output_path}' was not created or is empty.", file=sys.stderr)
             return False

    except FileNotFoundError:
        print(f"Error: Python executable '{sys.executable}' not found.", file=sys.stderr)
        return False
    except Exception as e:
        print(f"An error occurred while running the plotter script: {e}", file=sys.stderr)
        return False


def send_email_notification(vbs_script, subject, body, attachment_path):
    """Calls the VBScript to send an email with attachment."""
    print("Sending email notification...")
    # Check attachment exists *before* calling VBS
    if not os.path.exists(attachment_path):
        print(f"Error: Attachment file not found at '{attachment_path}'. Cannot send email.", file=sys.stderr)
        return False

    try:
        # Ensure VBS script exists
        if not os.path.exists(vbs_script):
             print(f"Error: VBScript email sender not found at '{vbs_script}'", file=sys.stderr)
             return False

        command = ["cscript.exe", "//Nologo", vbs_script, subject, attachment_path, body]
        # Set a timeout (e.g., 60 seconds) in case email sending hangs
        result = subprocess.run(command, capture_output=True, text=True, timeout=60)

        print("--- VBScript Email Output ---")
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(f"VBScript Errors/Warnings:\n{result.stderr}", file=sys.stderr)
        print("--- End VBScript Output ---")

        # Check VBScript output/return code for success
        # Assuming VBScript prints "SUCCESS" on success and exits with 0
        if result.returncode == 0 and "SUCCESS" in result.stdout:
            print("Email sent successfully.")
            return True
        else:
            print(f"Error: VBScript email sending failed. Exit code: {result.returncode}", file=sys.stderr)
            return False

    except FileNotFoundError:
         print(f"Error: 'cscript.exe' not found. Is it in the system PATH?", file=sys.stderr)
         return False
    except subprocess.TimeoutExpired:
         print(f"Error: VBScript email sending timed out after 60 seconds.", file=sys.stderr)
         return False
    except Exception as e:
        print(f"An error occurred while running the VBScript email sender: {e}", file=sys.stderr)
        return False

# --- Main Execution ---
if __name__ == "__main__":
    start_time = datetime.now()
    print(f"--- Network Monitor Started: {start_time} ---")

    # --- Initial Sanity Checks ---
    print("Performing initial checks...")
    abort = False
    if not check_file_exists(ps_script_path, "PowerShell Script"): abort = True
    if not check_file_exists(plotter_script_path, "Python Plotter Script"): abort = True
    if not check_file_exists(vbs_script_path, "VBScript Email Script"): abort = True
    if abort:
        print("One or more required script files are missing. Aborting.", file=sys.stderr)
        sys.exit(1)
    print("Required script files found.")
    # --- End Initial Checks ---

    # 1. Run the PowerShell network check
    ps_success = run_powershell_script(ps_script_path)
    if not ps_success:
        print("Warning: PowerShell script execution failed or had critical errors. Analysis/Graphing might be based on old data.", file=sys.stderr)
        # Decide whether to continue or exit if PS fails. Let's continue for now.

    # 2. Analyze the results from the CSV using the new specific logic
    send_alert_based_on_criteria, timestamp_str = analyze_results(csv_log_path)

    # 3. Generate the graph (always attempt if CSV exists, provides context)
    graph_generated = False # Default to false
    if os.path.exists(csv_log_path): # Only try to graph if log exists
        graph_generated = generate_graph(plotter_script_path, csv_log_path, graph_output_path, days_to_display=3) # MODIFIED
    else:
        print("Skipping graph generation because CSV log file does not exist.")


    # 4. Send email only if the specific alert criteria were met AND the graph was generated
    if send_alert_based_on_criteria:
        # Note: Message changed in Configuration section
        print("ALERT condition met based on specific criteria (RDP Listen or Outbound failure). Preparing email notification.")
        if graph_generated:
            email_body = EMAIL_BODY_FAILURE.format(timestamp=timestamp_str)
            email_success = send_email_notification(vbs_script_path, EMAIL_SUBJECT_FAILURE, email_body, graph_output_path)
            if not email_success:
                print("Email notification failed to send.", file=sys.stderr)
        else:
            print("Skipping email notification because graph generation failed or was skipped.", file=sys.stderr)
            # Optionally, send an email without the graph to report the failure?
            # email_body_no_graph = f"Critical network checks failed (RDP Listen or Outbound) at {timestamp_str}. Graph generation failed or was skipped."
            # send_email_notification(vbs_script_path, EMAIL_SUBJECT_FAILURE + " (No Graph)", email_body_no_graph, "") # Pass empty attachment path? Check VBScript handling.
    else:
        # This means either all checks passed, or only non-critical checks failed.
        print("No critical alert conditions met (RDP Listen / Outbound). No email notification needed.")

    end_time = datetime.now()
    print(f"--- Network Monitor Finished: {end_time} (Duration: {end_time - start_time}) ---")
