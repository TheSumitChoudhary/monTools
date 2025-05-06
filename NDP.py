#network diagnostic plotter
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import argparse
import os
from datetime import datetime, timedelta # Import timedelta

def plot_network_diagnostics(csv_path, output_file=None, days_to_display=None): # Added days_to_display
    """
    Reads network diagnostic data from a CSV file and generates timeline plots.
    Can either display the plot or save it to a file.

    Args:
        csv_path (str): The path to the input CSV file.
        output_file (str, optional): Path to save the plot image.
                                     If None, displays the plot instead. Defaults to None.
        days_to_display (int, optional): Number of past days to display data for.
                                         If None, all data is displayed. Defaults to None.
    Returns:
        bool: True if plotting (or saving) was successful, False otherwise.
    """
    # --- 1. Load and Prepare Data ---
    try:
        if not os.path.exists(csv_path):
            print(f"Error: CSV file not found at '{csv_path}'")
            return False
        if os.path.getsize(csv_path) == 0:
            print(f"Error: CSV file '{csv_path}' is empty.")
            return False

        df = pd.read_csv(csv_path)
        print(f"Successfully loaded {len(df)} records from '{csv_path}'")

        expected_cols = ['Timestamp', 'CheckType', 'CheckName', 'Success', 'ResponseTimeMs']
        if not all(col in df.columns for col in expected_cols):
            print(f"Error: CSV file is missing one or more expected columns: {expected_cols}")
            return False

    except pd.errors.EmptyDataError:
        print(f"Error: CSV file '{csv_path}' is empty or contains no data.")
        return False
    except Exception as e:
        print(f"Error loading or parsing CSV file '{csv_path}': {e}")
        return False

    try:
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    except Exception as e:
        print(f"Error converting 'Timestamp' column to datetime: {e}")
        return False

    # <--- ADD THIS SECTION FOR DATE FILTERING --->
    if days_to_display is not None and isinstance(days_to_display, int) and days_to_display > 0:
        print(f"Filtering data for the last {days_to_display} days.")
        cutoff_date = datetime.now() - timedelta(days=days_to_display)
        # Ensure cutoff_date is timezone-naive if df['Timestamp'] is, or make them compatible
        # If df['Timestamp'] is timezone-aware, you might need to make cutoff_date timezone-aware too.
        # Assuming df['Timestamp'] is timezone-naive based on typical CSV output.
        df = df[df['Timestamp'] >= cutoff_date]
        if df.empty:
            print(f"No data found within the last {days_to_display} days.")
            # You might want to still generate an empty plot or return False
            # For now, let's allow it to try and plot, which will show empty plots.
    # <--- END OF DATE FILTERING SECTION --->


    if df['Success'].dtype == 'object':
        df['Success'] = df['Success'].astype(str).str.lower().map({'true': True, 'false': False, 'yes': True, 'no': False})
    df['Success'] = df['Success'].astype('boolean')
    df['Success'] = df['Success'].fillna(False)


    df['ResponseTimeMs'] = pd.to_numeric(df['ResponseTimeMs'], errors='coerce')
    df = df.sort_values(by='Timestamp')

    # --- 2. Separate Data for Plotting ---
    outbound_checks = df[df['CheckType'].isin(['Outbound TCP', 'Outbound ICMP'])]
    inbound_checks = df[df['CheckType'] == 'Inbound Listen Check']
    ping_speed_checks = df[(df['CheckType'] == 'Outbound ICMP') & (df['Success'] == True) & (df['ResponseTimeMs'].notna())]

    if outbound_checks.empty and inbound_checks.empty and ping_speed_checks.empty:
        print("No data found for any plot type after filtering.")
        print("Skipping plot generation as no relevant data was found.")
        # If an output file is requested, create an empty placeholder or indicate no plot.
        if output_file:
            # Option 1: Create an empty plot with a message
            fig, ax = plt.subplots(figsize=(10, 2))
            ax.text(0.5, 0.5, f"No network data available for the selected period.",
                    horizontalalignment='center', verticalalignment='center',
                    transform=ax.transAxes, fontsize=12)
            ax.set_xticks([])
            ax.set_yticks([])
            try:
                plt.savefig(output_file, bbox_inches='tight')
                print(f"Empty plot with message saved to '{output_file}'")
                plt.close(fig)
                return True
            except Exception as e:
                print(f"Error saving empty plot: {e}")
                plt.close(fig)
                return False
            # Option 2: Return True but don't create a file (current behavior)
            # Option 3: Return False
        return True # Or False if an empty plot isn't desired for saving


    # --- 3. Create Plots ---
    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(3, 1, figsize=(15, 15), sharex=True)
    fig.suptitle('Network Diagnostics Timeline', fontsize=16, y=0.99)

    # Plot 1: Outbound Status
    if not outbound_checks.empty:
        print(f"Plotting Outbound Status ({len(outbound_checks)} points)...")
        sns.scatterplot(data=outbound_checks, x='Timestamp', y='CheckName', hue='Success',
                palette={True: 'green', False: 'red'}, ax=axes[0], s=30, legend='full') # s changed from 60 to 30
        axes[0].set_title('Outbound Check Status (TCP/ICMP)')
        axes[0].set_ylabel('Check Name')
        axes[0].tick_params(axis='y', labelsize=8)
        axes[0].legend(title='Success', loc='center left', bbox_to_anchor=(1, 0.5))
    else:
        axes[0].text(0.5, 0.5, 'No Outbound Check data available', ha='center', va='center', transform=axes[0].transAxes)
        axes[0].set_title('Outbound Check Status (TCP/ICMP)')
        axes[0].set_ylabel('Check Name')

    # Plot 2: Inbound Listen Status
    if not inbound_checks.empty:
        print(f"Plotting Inbound Listen Status ({len(inbound_checks)} points)...")
        sns.scatterplot(data=inbound_checks, x='Timestamp', y='CheckName', hue='Success',
                palette={True: 'blue', False: 'orange'}, ax=axes[1], s=30, legend='full') # s changed from 60 to 30
        axes[1].set_title('Inbound Port Listen Status')
        axes[1].set_ylabel('Local Port Check')
        axes[1].tick_params(axis='y', labelsize=8)
        handles, labels = axes[1].get_legend_handles_labels()
        label_map = {True: 'Listening', False: 'Not Listening'}
        new_labels = []
        valid_handles = []
        for i, l in enumerate(labels):
            try:
                # Handle if labels are already boolean or string 'True'/'False'
                actual_label = eval(l) if isinstance(l, str) else l
                if actual_label in label_map:
                    new_labels.append(label_map[actual_label])
                    valid_handles.append(handles[i])
            except NameError: # If eval fails because it's not 'True' or 'False'
                print(f"Warning: Could not map legend label '{l}'")
                new_labels.append(l) # Keep original label
                valid_handles.append(handles[i])

        if valid_handles: # Only show legend if there's something to show
             axes[1].legend(valid_handles, new_labels, title='Status', loc='center left', bbox_to_anchor=(1, 0.5))
    else:
        axes[1].text(0.5, 0.5, 'No Inbound Listen Check data available', ha='center', va='center', transform=axes[1].transAxes)
        axes[1].set_title('Inbound Port Listen Status')
        axes[1].set_ylabel('Local Port Check')

    # Plot 3: Ping Speed
    if not ping_speed_checks.empty:
        print(f"Plotting Ping Speed ({len(ping_speed_checks)} points)...")
        sns.lineplot(data=ping_speed_checks, x='Timestamp', y='ResponseTimeMs', hue='CheckName',
                     marker='o', ax=axes[2], legend='full')
        axes[2].set_title('Ping Speed (Successful ICMP Checks)')
        axes[2].set_ylabel('Average RTT (ms)')
        axes[2].set_ylim(bottom=0) # Ensure RTT doesn't go below 0
        # Ensure legend is only added if there are lines to label
        if axes[2].has_data():
            axes[2].legend(title='Ping Target', loc='center left', bbox_to_anchor=(1, 0.5), fontsize=8)
    else:
        axes[2].text(0.5, 0.5, 'No successful Ping data available', ha='center', va='center', transform=axes[2].transAxes)
        axes[2].set_title('Ping Speed (Successful ICMP Checks)')
        axes[2].set_ylabel('Average RTT (ms)')


    # --- 4. Final Touches & Output ---
    axes[2].set_xlabel('Timestamp')
    try:
        locator = mdates.AutoDateLocator(minticks=5, maxticks=10) # You can adjust minticks/maxticks
        formatter = mdates.ConciseDateFormatter(locator)
        axes[2].xaxis.set_major_locator(locator)
        axes[2].xaxis.set_major_formatter(formatter)
        plt.setp(axes[2].xaxis.get_majorticklabels(), rotation=30, ha='right')
    except Exception as e:
        print(f"Warning: Could not apply advanced date formatting: {e}")
        plt.xticks(rotation=30, ha='right')

    plt.tight_layout(rect=[0, 0.03, 0.9, 0.97])

    if output_file:
        try:
            output_dir = os.path.dirname(output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
                print(f"Created directory: {output_dir}")
            plt.savefig(output_file, bbox_inches='tight')
            print(f"Plot successfully saved to '{output_file}'")
            plt.close(fig)
            return True
        except Exception as e:
            print(f"Error saving plot to '{output_file}': {e}")
            plt.close(fig)
            return False
    else:
        print("Displaying plot...")
        plt.show()
        return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate timeline plots from network diagnostics CSV log.")
    parser.add_argument(
        "csv_file",
        nargs='?',
        default="NetworkDiagnostics_Log.csv",
        help="Path to the network diagnostics CSV file (default: NetworkDiagnostics_Log.csv)"
    )
    parser.add_argument(
        "-o", "--output-file",
        help="Path to save the plot image (e.g., plot.png) instead of displaying it."
    )
    # <--- ADD ARGUMENT FOR DAYS --->
    parser.add_argument(
        "-d", "--days",
        type=int,
        help="Number of past days of data to display on the graph (e.g., 3 for last 3 days)."
    )
    args = parser.parse_args()

    # Pass the days argument to the plotting function
    plot_network_diagnostics(args.csv_file, args.output_file, args.days)
