"""
Server Alert Dashboard
A streamlined, practical visualization tool for server CPU alerts.
"""

# Import statements first (these don't use Streamlit commands)
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
import os
import sys

# IMPORTANT: st.set_page_config() must be the first Streamlit command
st.set_page_config(
    page_title="HULFT01 Server Alert Dashboard",
    page_icon="📊",
    layout="wide"
)

# Now you can print debug information using standard print (not st.write)
print("Python version:", sys.version)
print("Pandas version:", pd.__version__)
print("Working directory:", os.getcwd())

# Custom styling (this is fine after set_page_config)
st.markdown("""
<style>
    .main {
        background-color: #f5f7f9;
    }
    .metric-container {
        background-color: white;
        padding: 15px;
        border-radius: 5px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .chart-container {
        background-color: white;
        padding: 15px;
        border-radius: 5px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-top: 20px;
    }
    h1, h2, h3 {
        color: #1e3a8a;
    }
</style>
""", unsafe_allow_html=True)

# Title
st.title("📊 HULFT01 Performance Monitoring")

# 

# Load data - modified for better compatibility
def load_data():
    try:
        csv_path = 'server_alerts1.csv'
        st.write(f"Attempting to load CSV from: {os.path.abspath(csv_path)}")
        
        if not os.path.exists(csv_path):
            st.error(f"CSV file not found at: {os.path.abspath(csv_path)}")
            return pd.DataFrame()
            
        df = pd.read_csv(csv_path)
        st.write(f"Successfully loaded {len(df)} records")
        
        # Convert timestamp with error handling
        try:
            df['received_time'] = pd.to_datetime(df['received_time'])
            df['date'] = df['received_time'].dt.date
        except Exception as e:
            st.error(f"Error converting timestamps: {str(e)}")
            
        # Add helpful derived columns
        if 'cpu_usage' in df.columns:
            df['is_critical'] = df['cpu_usage'] >= 80
            
        return df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return pd.DataFrame()

# Load data without caching for troubleshooting
df = load_data()

if df.empty:
    st.warning("No data available. Please ensure your CSV file exists and contains valid data.")
    st.stop()

# Sidebar filters
st.sidebar.header("Filters")

# Date range selector with error handling
try:
    min_date = df['date'].min()
    max_date = df['date'].max()
    default_start = max(min_date, max_date - timedelta(days=7))

    start_date = st.sidebar.date_input("Start Date", default_start, min_value=min_date, max_value=max_date)
    end_date = st.sidebar.date_input("End Date", max_date, min_value=min_date, max_value=max_date)
except Exception as e:
    st.error(f"Error setting up date filters: {str(e)}")
    start_date = datetime.now().date() - timedelta(days=7)
    end_date = datetime.now().date()

# Server selector
try:
    servers = df['server_name'].unique()
    selected_server = st.sidebar.selectbox("Select Server", ["All Servers"] + list(servers))
except Exception as e:
    st.error(f"Error setting up server filter: {str(e)}")
    selected_server = "All Servers"

# Process selector
try:
    processes = df['top_process'].unique()
    selected_process = st.sidebar.selectbox("Select Process", ["All Processes"] + list(processes))
except Exception as e:
    st.error(f"Error setting up process filter: {str(e)}")
    selected_process = "All Processes"

# Apply filters with error handling
try:
    filtered_df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]

    if selected_server != "All Servers":
        filtered_df = filtered_df[filtered_df['server_name'] == selected_server]
        
    if selected_process != "All Processes":
        filtered_df = filtered_df[filtered_df['top_process'] == selected_process]
except Exception as e:
    st.error(f"Error applying filters: {str(e)}")
    filtered_df = df.copy()

# Key metrics
st.header("Key Metrics")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Total Alerts", 
        len(filtered_df),
        help="Total number of CPU alerts in the selected period"
    )

with col2:
    try:
        avg_cpu = filtered_df['cpu_usage'].mean() if 'cpu_usage' in filtered_df else 0
        st.metric(
            "Average CPU", 
            f"{avg_cpu:.1f}%",
            help="Average CPU usage across all alerts"
        )
    except Exception as e:
        st.error(f"Error calculating average CPU: {str(e)}")
        st.metric("Average CPU", "Error")

with col3:
    try:
        max_cpu = filtered_df['cpu_usage'].max() if 'cpu_usage' in filtered_df and not filtered_df.empty else 0
        st.metric(
            "Peak CPU", 
            f"{max_cpu:.1f}%",
            help="Maximum CPU usage recorded"
        )
    except Exception as e:
        st.error(f"Error calculating peak CPU: {str(e)}")
        st.metric("Peak CPU", "Error")

with col4:
    try:
        if 'is_critical' in filtered_df.columns:
            critical_count = filtered_df['is_critical'].sum()
            critical_pct = (critical_count / len(filtered_df)) * 100 if len(filtered_df) > 0 else 0
            st.metric(
                "Critical Alerts", 
                f"{critical_count} ({critical_pct:.1f}%)",
                help="Alerts with CPU usage ≥ 80%"
            )
        else:
            st.metric("Critical Alerts", "N/A")
    except Exception as e:
        st.error(f"Error calculating critical alerts: {str(e)}")
        st.metric("Critical Alerts", "Error")

# CPU Trend Chart
st.header("CPU Usage Trend")

try:
    if len(filtered_df) > 0 and 'cpu_usage' in filtered_df.columns:
        # Sort by time for proper trend display
        trend_df = filtered_df.sort_values('received_time')
        
        fig = px.line(
            trend_df, 
            x='received_time', 
            y='cpu_usage',
            labels={'received_time': 'Time', 'cpu_usage': 'CPU Usage (%)'},
            template='plotly_white'
        )
        
        # Add threshold line
        if 'threshold' in filtered_df.columns and not filtered_df['threshold'].isnull().all():
            threshold = filtered_df['threshold'].iloc[0]
            fig.add_hline(
                y=threshold, 
                line_dash="dash", 
                line_color="red",
                annotation_text=f"Threshold ({threshold}%)",
                annotation_position="top right"
            )
        
        fig.update_layout(
            height=400,
            xaxis_title="Time",
            yaxis_title="CPU Usage (%)",
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data available for CPU trend visualization.")
except Exception as e:
    st.error(f"Error generating CPU trend chart: {str(e)}")

# Process Analysis
st.header("Process Analysis")
col1, col2 = st.columns(2)

with col1:
    st.subheader("Top CPU-Consuming Processes")
    
    try:
        if len(filtered_df) > 0 and 'top_process' in filtered_df.columns and 'cpu_seconds' in filtered_df.columns:
            # Group by process and calculate average CPU seconds
            process_df = filtered_df.groupby('top_process')['cpu_seconds'].mean().reset_index()
            process_df = process_df.sort_values('cpu_seconds', ascending=False).head(10)
            
            fig = px.bar(
                process_df,
                x='top_process',
                y='cpu_seconds',
                labels={'top_process': 'Process', 'cpu_seconds': 'Avg CPU Seconds'},
                template='plotly_white',
                color='cpu_seconds',
                color_continuous_scale='Viridis'
            )
            
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No process data available.")
    except Exception as e:
        st.error(f"Error generating process chart: {str(e)}")

with col2:
    st.subheader("Process Distribution")
    
    try:
        if len(filtered_df) > 0 and 'top_process' in filtered_df.columns:
            process_counts = filtered_df['top_process'].value_counts()
            
            fig = px.pie(
                values=process_counts.values,
                names=process_counts.index,
                hole=0.4,
                template='plotly_white'
            )
            
            fig.update_layout(height=400)
            fig.update_traces(textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No process distribution data available.")
    except Exception as e:
        st.error(f"Error generating pie chart: {str(e)}")

# Time Pattern Analysis
st.header("Time Pattern Analysis")

try:
    if len(filtered_df) > 0 and 'received_time' in filtered_df.columns and 'cpu_usage' in filtered_df.columns:
        # Create hour of day column
        pattern_df = filtered_df.copy()
        pattern_df['hour'] = pattern_df['received_time'].dt.hour
        
        # Group by hour and calculate average CPU
        hourly_avg = pattern_df.groupby('hour')['cpu_usage'].mean().reset_index()
        
        fig = px.bar(
            hourly_avg,
            x='hour',
            y='cpu_usage',
            labels={'hour': 'Hour of Day', 'cpu_usage': 'Avg CPU Usage (%)'},
            template='plotly_white'
        )
        
        fig.update_layout(
            height=350,
            xaxis=dict(
                tickmode='array',
                tickvals=list(range(0, 24)),
                ticktext=[f"{h:02d}:00" for h in range(0, 24)]
            )
        )
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No time pattern data available.")
except Exception as e:
    st.error(f"Error generating time pattern chart: {str(e)}")

# Recent Alerts Table
st.header("Recent Alerts")

try:
    if len(filtered_df) > 0:
        # Sort by time, most recent first
        recent_df = filtered_df.sort_values('received_time', ascending=False).head(10)
        
        # Select and rename columns for display
        display_cols = ['received_time', 'server_name', 'cpu_usage', 'top_process', 'cpu_seconds']
        rename_map = {
            'received_time': 'Time',
            'server_name': 'Server',
            'cpu_usage': 'CPU Usage (%)',
            'top_process': 'Process',
            'cpu_seconds': 'CPU Seconds'
        }
        
        # Filter to available columns and rename
        available_cols = [col for col in display_cols if col in recent_df.columns]
        table_df = recent_df[available_cols].rename(columns={col: rename_map.get(col, col) for col in available_cols})
        
        # Format datetime column
        if 'Time' in table_df.columns:
            table_df['Time'] = table_df['Time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Display the table
        st.dataframe(table_df, use_container_width=True)
    else:
        st.info("No alert data available.")
except Exception as e:
    st.error(f"Error displaying recent alerts: {str(e)}")

# Download option
st.sidebar.header("Export Data")

try:
    if len(filtered_df) > 0:
        csv = filtered_df.to_csv(index=False)
        server_name = "all_servers" if selected_server == "All Servers" else selected_server
        
        st.sidebar.download_button(
            label="Download Filtered Data",
            data=csv,
            file_name=f"server_alerts_{server_name}_{start_date}_{end_date}.csv",
            mime='text/csv',
        )
except Exception as e:
    st.sidebar.error(f"Error setting up download: {str(e)}")

# Footer
st.sidebar.markdown("---")
st.sidebar.caption(f"Data last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
