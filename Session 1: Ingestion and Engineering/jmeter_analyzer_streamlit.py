"""
***************************************************************************************************
| A | M | S |   | L | A | B | S |   | C | U | S | T | O | M | E | R |   | D | E | M | O |

Demo:         AMS Labs JMeter Performance Analysis Tool
Create Date:  2025-06-15
Purpose:      Streamlit application for analyzing JMeter performance test results and query patterns
Data Source:  JMeter log files (CSV format)
Customer:     Performance Testing and Database Optimization
***************************************************************************************************

----------------------------------------------------------------------------------
This Streamlit application analyzes JMeter performance test results to provide
insights into query execution patterns, throughput metrics, and system performance.
It processes JMeter CSV log files and creates interactive visualizations showing
query rates over time with statistical analysis capabilities.

Key Concepts:
  • Performance Analysis: Statistical analysis of query execution patterns
  • Time-series Visualization: Query rate trends over testing periods
  • Log File Processing: Automated parsing and analysis of JMeter CSV outputs
  • Interactive Charts: Real-time visualization with Plotly integration
  • Load Testing Metrics: Throughput analysis and performance benchmarking
----------------------------------------------------------------------------------
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import io

def analyze_jmeter_log(df):
    """
    Analyze JMeter log data and return processed data for visualization
    """
    
    # Convert timestamp from milliseconds to datetime
    df['datetime'] = pd.to_datetime(df['timeStamp'], unit='ms')
    
    # Sort by timestamp to ensure chronological order
    df = df.sort_values('datetime')
    
    # Get the time range
    start_time = df['datetime'].min()
    end_time = df['datetime'].max()
    
    # Create 5-second intervals
    # Round down start time to nearest 5 seconds
    start_rounded = start_time.floor('5s')
    
    # Create time bins for 5-second intervals
    time_bins = pd.date_range(start=start_rounded, end=end_time, freq='5s')
    
    # Group queries by 5-second intervals and count them
    df['time_bin'] = pd.cut(df['datetime'], bins=time_bins, right=False, include_lowest=True)
    query_counts = df.groupby('time_bin').size()
    
    # Create a complete time series (fill missing intervals with 0)
    all_intervals = pd.DataFrame(index=pd.date_range(start=start_rounded, end=end_time, freq='5s'))
    all_intervals['query_count'] = 0
    all_intervals['time_label'] = all_intervals.index.strftime('%H:%M:%S')
    
    # Map the actual counts to the intervals
    for interval, count in query_counts.items():
        if pd.notna(interval):
            interval_start = interval.left
            if interval_start in all_intervals.index:
                all_intervals.loc[interval_start, 'query_count'] = count
    
    return all_intervals, start_time, end_time, len(df)

def create_bar_chart(data):
    """
    Create a bar chart using Plotly
    """
    
    # Calculate statistics
    max_queries = data['query_count'].max()
    avg_queries = data['query_count'].mean()
    
    # Create bar chart
    fig = go.Figure(data=[
        go.Bar(
            x=data.index,
            y=data['query_count'],
            text=data['query_count'].apply(lambda x: str(int(x)) if x > 0 else ''),
            textposition='outside',
            textfont=dict(size=10, color='black'),
            marker=dict(
                color='#2E86AB',
                opacity=0.8,
                line=dict(color='#1C5F7A', width=1)
            ),
            hovertemplate='<b>Time:</b> %{x|%H:%M:%S}<br><b>Queries:</b> %{y}<extra></extra>'
        )
    ])
    
    # Update layout
    fig.update_layout(
        title={
            'text': 'JMeter Query Execution Rate<br><sub>Number of Queries per 5-Second Interval</sub>',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 18}
        },
        xaxis_title='Time',
        yaxis_title='Number of Queries',
        xaxis=dict(
            tickformat='%H:%M:%S',
            tickangle=45
        ),
        yaxis=dict(
            gridcolor='lightgray',
            gridwidth=0.5
        ),
        plot_bgcolor='white',
        height=600,
        margin=dict(t=100, b=100, l=50, r=50)
    )
    
    return fig, max_queries, avg_queries

def main():
    st.set_page_config(
        page_title="JMeter Log Analyzer",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("📊 JMeter Log Analyzer")
    st.markdown("Upload your JMeter CSV log file to visualize query execution patterns")
    
    # File uploader
    uploaded_file = st.file_uploader(
        "Choose a JMeter CSV log file",
        type=['csv'],
        help="Upload your JMeter CSV log file containing timestamp and query data"
    )
    
    if uploaded_file is not None:
        try:
            # Read the uploaded file
            df = pd.read_csv(uploaded_file)
            
            # Validate required columns
            required_columns = ['timeStamp']
            if not all(col in df.columns for col in required_columns):
                st.error(f"Missing required columns. Expected: {required_columns}")
                st.info("Please make sure your CSV file contains the 'timeStamp' column")
                return
            
            st.success(f"✅ File uploaded successfully! Found {len(df)} records")
            
            # Analyze the data
            with st.spinner("Analyzing JMeter log data..."):
                intervals_data, start_time, end_time, total_queries = analyze_jmeter_log(df)
            
            # Display summary statistics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Queries", f"{total_queries:,}")
            
            with col2:
                duration = end_time - start_time
                # Format duration as HH:MM:SS
                total_seconds = int(duration.total_seconds())
                hours = total_seconds // 3600
                minutes = (total_seconds % 3600) // 60
                seconds = total_seconds % 60
                duration_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                st.metric("Test Duration", duration_str)
            
            with col3:
                max_queries = intervals_data['query_count'].max()
                st.metric("Peak Queries/5s", f"{max_queries}")
            
            with col4:
                avg_queries = intervals_data['query_count'].mean()
                st.metric("Avg Queries/5s", f"{avg_queries:.1f}")
            
            # Create and display the chart
            st.subheader("📈 Query Execution Pattern")
            
            fig, max_val, avg_val = create_bar_chart(intervals_data)
            st.plotly_chart(fig, use_container_width=True)
            
            # Additional insights
            st.subheader("📋 Detailed Statistics")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Time Range:**")
                st.write(f"• Start: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
                st.write(f"• End: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
                st.write(f"• Duration: {duration_str}")
            
            with col2:
                st.write("**Query Distribution:**")
                st.write(f"• Total 5-second intervals: {len(intervals_data)}")
                st.write(f"• Intervals with queries: {(intervals_data['query_count'] > 0).sum()}")
                st.write(f"• Empty intervals: {(intervals_data['query_count'] == 0).sum()}")
            
            # Show raw data table (optional)
            if st.checkbox("Show raw interval data"):
                st.subheader("📊 Raw Interval Data")
                display_data = intervals_data.copy()
                display_data.index = display_data.index.strftime('%H:%M:%S')
                st.dataframe(
                    display_data,
                    use_container_width=True,
                    column_config={
                        "query_count": st.column_config.NumberColumn(
                            "Query Count",
                            help="Number of queries in this 5-second interval",
                            format="%d"
                        )
                    }
                )
            
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
            st.info("Please make sure your file is a valid JMeter CSV log with the correct format")
    
    else:
        # Instructions when no file is uploaded
        st.info("👆 Please upload a JMeter CSV log file to get started")
        
        st.markdown("### 📝 Expected File Format")
        st.markdown("""
        Your JMeter CSV log file should contain at least the following column:
        - **timeStamp**: Unix timestamp in milliseconds
        
        Example header:
        ```
        timeStamp,elapsed,label,responseCode,responseMessage,threadName,dataType,success,failureMessage,bytes,sentBytes,grpThreads,allThreads,URL,Latency,IdleTime,Connect
        ```
        """)
        
        st.markdown("### 🚀 How it works")
        st.markdown("""
        1. **Upload** your JMeter CSV log file
        2. **Analyze** - The app processes your data and groups queries into 5-second intervals
        3. **Visualize** - View a bar chart showing query execution patterns over time
        4. **Insights** - Get summary statistics and detailed breakdowns
        """)

if __name__ == "__main__":
    main() 