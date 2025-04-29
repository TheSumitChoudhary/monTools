#!/usr/bin/env python3
"""
Enhanced Service Monitoring Insights Engine
-------------------------------------------
Analyzes service status changes from a CSV file and generates
a visually appealing heatmap of activity for a user-specified month.

Improvements:
- Prompts user to select a month (1-12) for visualization.
- Focuses visualization *only* on the selected month's activity heatmap.
- Uses Seaborn for significantly improved aesthetics ("gorgeous" plots).
- Implements a Day of Month vs. Hour heatmap using a 'YlOrRd' colormap.
- Uses a clean 'white' style (no background grid).
- Adds detailed comments and uses type hinting.
- Improves overall code structure and readability.
- Uses modern Pandas features.
- Ensures all hour labels (0-23) are displayed on the x-axis.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from datetime import datetime, date
from typing import Dict, Optional, Tuple, List
import logging
import sys
import traceback
import calendar

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('service_insights_heatmap.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ServiceInsightsEngine:
    """
    Analyzes service status changes and generates insights and a heatmap visualization
    for a specified month.
    """
    def __init__(self, csv_path: str):
        """
        Initializes the engine by loading and preprocessing data.

        Args:
            csv_path (str): The path to the CSV file containing service status data.
        """
        self.df: Optional[pd.DataFrame] = self._load_and_preprocess_data(csv_path)
        self.insights: Dict = {}
        # Set theme: Use 'white' style for no background grid, keep the requested palette
        sns.set_theme(style="white", palette="YlOrRd", font_scale=1.1)
        self.default_figsize: Tuple[int, int] = (14, 8)

    def _load_and_preprocess_data(self, path: str) -> Optional[pd.DataFrame]:
        """
        Loads data from the CSV file, validates, preprocesses, and adds time features.

        Args:
            path (str): The path to the CSV file.

        Returns:
            Optional[pd.DataFrame]: The preprocessed DataFrame, or None if loading fails.
        """
        logger.info(f"Attempting to load data from: {path}")
        try:
            df = pd.read_csv(path)
            logger.info(f"Successfully loaded {len(df)} rows.")

            required_cols = {'Timestamp', 'ServiceName', 'PreviousStatus', 'CurrentStatus'}
            if not required_cols.issubset(df.columns):
                missing = required_cols - set(df.columns)
                logger.error(f"Critical Error: Missing required columns: {missing}. Aborting.")
                return None

            df = df.rename(columns={
                'Timestamp': 'timestamp',
                'ServiceName': 'service_name',
                'PreviousStatus': 'previous_status',
                'CurrentStatus': 'current_status'
            })
            logger.info("Renamed columns for internal consistency.")

            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            original_len = len(df)
            df.dropna(subset=['timestamp'], inplace=True)
            if len(df) < original_len:
                logger.warning(f"Dropped {original_len - len(df)} rows due to invalid timestamps.")

            if df.empty:
                 logger.error("Critical Error: No valid data remaining after timestamp parsing. Aborting.")
                 return None

            df['date'] = df['timestamp'].dt.date
            df['hour'] = df['timestamp'].dt.hour
            df['day_of_week'] = df['timestamp'].dt.day_name()
            df['day_of_month'] = df['timestamp'].dt.day
            df['month'] = df['timestamp'].dt.month
            df['year'] = df['timestamp'].dt.year
            logger.info("Added time-based features (date, hour, day_of_week, day_of_month, month, year).")

            df.sort_values(by='timestamp', inplace=True)
            logger.info("Data sorted by timestamp.")

            logger.info("Data loading and preprocessing completed successfully.")
            return df

        except FileNotFoundError:
            logger.error(f"Critical Error: File not found at '{path}'. Aborting.")
            return None
        except pd.errors.EmptyDataError:
            logger.error(f"Critical Error: File at '{path}' is empty. Aborting.")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred during data loading: {e}", exc_info=True)
            return None

    # --- Methods for calculate_stability_metrics and analyze_correlations remain unchanged ---
    def calculate_stability_metrics(self) -> None:
        """ Calculates key stability metrics (kept for potential future use). """
        if self.df is None: return
        logger.info("Calculating stability metrics...")
        self.insights['total_changes_per_service'] = self.df['service_name'].value_counts().to_dict()
        self.df['time_diff_seconds'] = self.df.groupby('service_name')['timestamp'].diff().dt.total_seconds()
        self.df['duration_in_previous_state_seconds'] = self.df.groupby('service_name')['time_diff_seconds'].shift(-1).fillna(0)
        time_in_state = self.df.groupby(['service_name', 'previous_status'])['duration_in_previous_state_seconds'].sum()
        time_in_state_hours = (time_in_state / 3600).unstack(fill_value=0)
        self.insights['time_in_state_hours_per_service'] = time_in_state_hours.to_dict('index')
        logger.info("Stability metrics calculated.")

    def analyze_correlations(self) -> None:
        """ Analyzes basic correlations (kept for potential future use). """
        if self.df is None: return
        logger.info("Analyzing correlations (simultaneous changes)...")
        simultaneous_changes_df = self.df[self.df.duplicated(subset=['timestamp'], keep=False)]
        simultaneous_events = simultaneous_changes_df.groupby('timestamp').apply(
            lambda x: x[['service_name', 'previous_status', 'current_status']].to_dict('records')
        ).reset_index(name='changes')
        self.insights['simultaneous_change_events'] = simultaneous_events.to_dict('records')
        logger.info(f"Found {len(simultaneous_events)} timestamps with simultaneous changes.")

    def _filter_for_specific_month(self, year: int, month: int) -> Optional[pd.DataFrame]:
        """
        Filters the main DataFrame for records within the specified calendar month and year.

        Args:
            year (int): The year to filter by.
            month (int): The month number (1-12) to filter by.

        Returns:
            Optional[pd.DataFrame]: Filtered DataFrame or None if no data.
        """
        if self.df is None:
            logger.warning("DataFrame not loaded. Cannot filter.")
            return None

        try:
            # Validate month input just in case
            if not 1 <= month <= 12:
                 logger.error(f"Invalid month provided to filter function: {month}. Must be 1-12.")
                 return None

            # Calculate the start and end timestamps for the given month and year
            start_of_month = datetime(year, month, 1)
            # Get the number of days in the month
            _, days_in_month = calendar.monthrange(year, month)
            # End of month is the last day at 23:59:59.999999
            end_of_month = datetime(year, month, days_in_month, 23, 59, 59, 999999)

            # Filter the DataFrame
            month_df = self.df[
                (self.df['timestamp'] >= start_of_month) &
                (self.df['timestamp'] <= end_of_month)
            ].copy()

            month_name = start_of_month.strftime('%B') # Get month name for logging
            if month_df.empty:
                logger.warning(f"No data found for the specified period: {month_name} {year}.")
                return None

            logger.info(f"Filtered data for {month_name} {year} ({start_of_month.strftime('%Y-%m-%d')} to {end_of_month.strftime('%Y-%m-%d')}). Found {len(month_df)} records.")
            return month_df

        except Exception as e:
            logger.error(f"Error during filtering for {year}-{month}: {e}", exc_info=True)
            return None


    def visualize_heatmap_for_month(self, year: int, month: int) -> None:
        """
        Generates and displays the heatmap visualization for the specified month and year.
        Ensures all hour labels (0-23) are shown on the x-axis. Uses 'white' style.

        Args:
            year (int): The year to visualize.
            month (int): The month number (1-12) to visualize.
        """
        month_name = datetime(year, month, 1).strftime('%B') # Get month name for titles
        logger.info(f"Generating heatmap visualization for {month_name} {year}...")

        df_month = self._filter_for_specific_month(year=year, month=month)

        if df_month is None or df_month.empty:
            logger.warning(f"No data available for {month_name} {year} to visualize.")
            # Display a message plot
            fig, ax = plt.subplots(figsize=(10, 2))
            ax.text(0.5, 0.5, f'No data available for {month_name} {year}.',
                    horizontalalignment='center', verticalalignment='center',
                    fontsize=14, color='red')
            ax.axis('off')
            plt.suptitle(f"Service Status Changes: {month_name} {year}", fontsize=16, y=0.95)
            plt.tight_layout(rect=[0, 0.03, 1, 0.95])
            plt.show()
            return

        # Get the number of days in the specified month
        _, days_in_month = calendar.monthrange(year, month)

        # Create a single figure and axes for the heatmap
        fig, ax = plt.subplots(figsize=self.default_figsize)
        # Set the main title for the figure
        fig.suptitle(f"Service Activity by Hour and Day: {month_name} {year}", fontsize=18, y=0.98) # y adjusts vertical position

        # --- Generate Heatmap Data ---
        try:
            heatmap_data = df_month.groupby(['day_of_month', 'hour']).size().unstack(fill_value=0)
            # Ensure all days and hours are present
            all_days = range(1, days_in_month + 1)
            all_hours = range(24)
            heatmap_data = heatmap_data.reindex(index=all_days, columns=all_hours, fill_value=0)

            # --- Plot Heatmap ---
            sns.heatmap(heatmap_data,
                        cmap='YlOrRd', # Use the specified colormap
                        ax=ax,
                        linewidths=.5,      # Keep lines between cells
                        linecolor='lightgray', # Color for the lines
                        cbar_kws={'label': 'Number of Changes'}) # Label for the color bar

            # Set title specific to the axes/plot itself (optional, can rely on suptitle)
            # ax.set_title('Activity Frequency: Day of Month vs. Hour') # Can be commented out if suptitle is enough
            ax.set_xlabel('Hour of Day')
            ax.set_ylabel('Day of Month')

            # --- Explicitly set X-axis ticks and labels ---
            ax.set_xticks([i + 0.5 for i in all_hours]) # Center ticks in cells
            ax.set_xticklabels(all_hours)              # Label with hour numbers
            ax.tick_params(axis='x', rotation=0)       # Keep labels horizontal

            # --- Configure Y-axis ---
            ax.set_yticks([i + 0.5 for i in range(days_in_month)]) # Center ticks in cells
            ax.set_yticklabels(all_days)                           # Label with day numbers
            ax.tick_params(axis='y', rotation=0)                   # Keep labels horizontal
            ax.invert_yaxis() # Invert y-axis (day 1 at the top)


        except Exception as e:
             logger.error(f"Failed to generate heatmap for {month_name} {year}: {e}", exc_info=True)
             ax.text(0.5, 0.5, 'Error generating heatmap.', ha='center', va='center', color='red', fontsize=12)
             ax.set_title('Heatmap Generation Error')

        # --- Final Touches ---
        plt.tight_layout(rect=[0, 0.03, 1, 0.95]) # Adjust layout to prevent title overlap
        plt.show()
        logger.info(f"Heatmap visualization for {month_name} {year} generated and displayed.")


# --- Main Execution Block ---
if __name__ == "__main__":
    logger.info("="*50)
    logger.info("Starting Service Monitoring Insights Engine (Heatmap Only - Select Month)")
    logger.info("="*50)

    csv_file_name = 'ServiceStatusChanges.csv'

    # --- Get User Input for Month ---
    selected_month = None
    while selected_month is None:
        try:
            month_input = input("Enter the month number (1-12) to visualize: ")
            month_int = int(month_input)
            if 1 <= month_int <= 12:
                selected_month = month_int
            else:
                print("Invalid input. Please enter a number between 1 and 12.")
        except ValueError:
            print("Invalid input. Please enter a number.")
        except EOFError: # Handle case where input stream is closed (e.g., piping)
             logger.error("Input stream closed unexpectedly. Exiting.")
             sys.exit(1)

    # Assume the current year for the visualization
    current_year = datetime.now().year
    logger.info(f"User selected month: {selected_month}, Year: {current_year}")


    try:
        # --- Initialization ---
        engine = ServiceInsightsEngine(csv_file_name)

        if engine.df is None:
            logger.error("Data loading failed. Exiting.")
            sys.exit(1)

        # --- Analysis (Optional) ---
        # engine.calculate_stability_metrics()
        # engine.analyze_correlations()

        # --- Visualization ---
        logger.info(f"--- Generating Heatmap Visualization for Month {selected_month}/{current_year} ---")
        # Call the visualization function with the selected month and current year
        engine.visualize_heatmap_for_month(year=current_year, month=selected_month)

        logger.info("="*50)
        logger.info("Insights Engine Finished Successfully")
        logger.info("="*50)

    except Exception as e:
        logger.error(f"A critical unexpected error occurred during execution: {e}", exc_info=True)
        traceback.print_exc()
        sys.exit(1)
