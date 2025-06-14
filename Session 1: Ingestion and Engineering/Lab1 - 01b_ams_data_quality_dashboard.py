# Lab 1 - 01b - ams_data_quality_dashboard.py
# Remember to include plotly 

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(
    page_title="AMS Labs Data Quality Dashboard",
    page_icon="📊",
    layout="wide"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
        margin-bottom: 1rem;
    }
    .alert-high {
        background-color: #ffebee;
        border-left: 4px solid #f44336;
    }
    .alert-medium {
        background-color: #fff3e0;
        border-left: 4px solid #ff9800;
    }
    .alert-low {
        background-color: #e8f5e8;
        border-left: 4px solid #4caf50;
    }
    .stSelectbox > div > div > select {
        background-color: #f0f2f6;
    }
</style>
""", unsafe_allow_html=True)

# Initialize Snowflake session
session = get_active_session()

# Database configurations
DATABASE = 'AMS_LABS'
SCHEMA = 'DATA_ENGINEERING'
TABLE_NAME = 'TA_APPLICATION_DATA_BRONZE'

def get_data_quality_metrics() -> pd.DataFrame:
    """Fetch data quality monitoring results"""
    try:
        query = f"""
        SELECT 
            change_commit_time,
            measurement_time,
            table_database,
            table_schema,
            table_name,
            metric_name,
            value
        FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
        WHERE table_database = '{DATABASE}'
            AND table_schema = '{SCHEMA}'
            AND table_name = '{TABLE_NAME}'
        ORDER BY change_commit_time DESC
        """
        result = session.sql(query).collect()
        if result:
            df = session.create_dataframe(result).to_pandas()
            if 'VALUE' in df.columns:
                df['VALUE'] = pd.to_numeric(df['VALUE'], errors='coerce').fillna(0)
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching data quality metrics: {e}")
        return pd.DataFrame()

def get_table_profile() -> dict:
    """Get basic table profiling information"""
    try:
        # Get row count
        row_count_query = f"SELECT COUNT(*) as count FROM {DATABASE}.{SCHEMA}.{TABLE_NAME}"
        row_count_result = session.sql(row_count_query).collect()
        row_count = row_count_result[0]['COUNT'] if row_count_result else 0
        
        # Get table info
        table_info_query = f"DESCRIBE TABLE {DATABASE}.{SCHEMA}.{TABLE_NAME}"
        table_info_result = session.sql(table_info_query).collect()
        table_info_df = session.create_dataframe(table_info_result).to_pandas() if table_info_result else pd.DataFrame()
        
        return {
            'row_count': row_count,
            'table_info': table_info_df
        }
    except Exception as e:
        st.error(f"Error fetching table profile: {e}")
        return {'row_count': 0, 'table_info': pd.DataFrame()}

def get_problematic_records(metric_name: str, limit: int = 50) -> tuple[pd.DataFrame, str]:
    """Get records that have issues based on the selected metric"""
    try:
        query = ""
        
        if metric_name == "ROW_COUNT":
            query = f"""
            SELECT * FROM {DATABASE}.{SCHEMA}.{TABLE_NAME}
            ORDER BY SNAPSHOT_DATE DESC
            LIMIT {limit}
            """
            
        elif metric_name == "TA_APPLICATIONS_ID_NULL_COUNT":
            query = f"""
            SELECT * FROM {DATABASE}.{SCHEMA}.{TABLE_NAME}
            WHERE TA_APPLICATIONS_ID IS NULL
            ORDER BY SNAPSHOT_DATE DESC
            LIMIT {limit}
            """
            
        elif metric_name == "JOBNUMBER_NULL_COUNT":
            query = f"""
            SELECT * FROM {DATABASE}.{SCHEMA}.{TABLE_NAME}
            WHERE JOBNUMBER IS NULL
            ORDER BY SNAPSHOT_DATE DESC
            LIMIT {limit}
            """
            
        elif metric_name == "TA_APPLICATIONS_UK_UNIQUE_COUNT":
            query = f"""
            SELECT TA_APPLICATIONS_UK, COUNT(*) as occurrence_count,
                   LISTAGG(TA_APPLICATIONS_ID, ', ') as sample_record_ids
            FROM {DATABASE}.{SCHEMA}.{TABLE_NAME}
            WHERE TA_APPLICATIONS_UK IS NOT NULL
            GROUP BY TA_APPLICATIONS_UK
            HAVING COUNT(*) > 1
            ORDER BY occurrence_count DESC
            LIMIT {limit}
            """
            
        elif metric_name == "TA_APPLICATIONS_UK_DUPLICATE_COUNT":
            query = f"""
            SELECT * FROM {DATABASE}.{SCHEMA}.{TABLE_NAME}
            WHERE TA_APPLICATIONS_UK IN (
                SELECT TA_APPLICATIONS_UK 
                FROM {DATABASE}.{SCHEMA}.{TABLE_NAME}
                WHERE TA_APPLICATIONS_UK IS NOT NULL
                GROUP BY TA_APPLICATIONS_UK
                HAVING COUNT(*) > 1
            )
            ORDER BY TA_APPLICATIONS_UK, SNAPSHOT_DATE DESC
            LIMIT {limit}
            """
            
        elif metric_name == "INVALID_JOBNUMBER_FORMAT_COUNT":
            query = f"""
            SELECT * FROM {DATABASE}.{SCHEMA}.{TABLE_NAME}
            WHERE JOBNUMBER IS NOT NULL 
              AND FALSE = (JOBNUMBER ILIKE 'AMS%')
            ORDER BY SNAPSHOT_DATE DESC
            LIMIT {limit}
            """
            
        elif metric_name == "INVALID_WORKDAYID_FORMAT_COUNT":
            query = f"""
            SELECT * FROM {DATABASE}.{SCHEMA}.{TABLE_NAME}
            WHERE WORKDAYID IS NOT NULL 
              AND FALSE = (WORKDAYID REGEXP '^JR-[0-9]{{5}}$')
            ORDER BY SNAPSHOT_DATE DESC
            LIMIT {limit}
            """
            
        elif metric_name == "INVALID_JOBSTATUS_COUNT":
            query = f"""
            SELECT * FROM {DATABASE}.{SCHEMA}.{TABLE_NAME}
            WHERE JOBSTATUS IS NOT NULL 
              AND JOBSTATUS NOT IN ('Closed/Filled', 'Closed', 'Offer Accepted', 'On Hold', 'Pending')
            ORDER BY SNAPSHOT_DATE DESC
            LIMIT {limit}
            """
            
        elif metric_name == "INVALID_CANDIDATESTATUS_COUNT":
            query = f"""
            SELECT * FROM {DATABASE}.{SCHEMA}.{TABLE_NAME}
            WHERE CANDIDATESTATUS IS NOT NULL 
              AND CANDIDATESTATUS NOT IN ('New', 'Active', 'Inactive', 'In Hiring', 'To be Archived')
            ORDER BY SNAPSHOT_DATE DESC
            LIMIT {limit}
            """
            
        else:
            st.warning(f"No specific drill-down logic defined for metric: {metric_name}")
            query = f"""
            SELECT * FROM {DATABASE}.{SCHEMA}.{TABLE_NAME}
            ORDER BY SNAPSHOT_DATE DESC
            LIMIT {limit}
            """
        
        # Execute the query
        result = session.sql(query).collect()
        if result:
            df = session.create_dataframe(result).to_pandas()
            return df, query
        else:
            return pd.DataFrame(), query
            
    except Exception as e:
        st.error(f"Error fetching problematic records for {metric_name}: {e}")
        return pd.DataFrame(), query if 'query' in locals() else ""

def get_alert_level(metric_name: str, value: float) -> tuple[str, str]:
    """Determine alert level and emoji for a metric"""
    if "NULL_COUNT" in metric_name or "DUPLICATE_COUNT" in metric_name or "INVALID" in metric_name:
        if value > 10:
            return "alert-high", "🔴"
        elif value > 0:
            return "alert-medium", "🟡"
        else:
            return "alert-low", "🟢"
    else:
        return "alert-low", "🟢"

def calculate_quality_score(df_metrics: pd.DataFrame, row_count: int) -> float:
    """Calculate overall data quality score"""
    if df_metrics.empty or row_count == 0:
        return 95.0
    
    df_metrics_copy = df_metrics.copy()
    df_metrics_copy['VALUE'] = pd.to_numeric(df_metrics_copy['VALUE'], errors='coerce').fillna(0)
    
    total_issues = df_metrics_copy[df_metrics_copy['VALUE'] > 0]['VALUE'].sum()
    quality_score = max(60, min(95, 100 - (total_issues / row_count * 100)))
    return quality_score

def display_overview_metrics(df_metrics: pd.DataFrame, table_profile: dict):
    """Display key overview metrics"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        row_count = table_profile.get('row_count', 0)
        st.metric("Total Records", f"{row_count:,}")
    
    with col2:
        active_metrics = len(df_metrics['METRIC_NAME'].unique()) if not df_metrics.empty else 0
        st.metric("Active Metrics", active_metrics)
    
    with col3:
        if not df_metrics.empty:
            last_update = df_metrics['MEASUREMENT_TIME'].max()
            st.metric("Last Updated", last_update.strftime("%Y-%m-%d %H:%M") if pd.notna(last_update) else "N/A")
        else:
            st.metric("Last Updated", "N/A")
    
    with col4:
        quality_score = calculate_quality_score(df_metrics, table_profile.get('row_count', 0))
        st.metric("Quality Score", f"{quality_score:.1f}%")

def display_metric_cards(latest_metrics: pd.DataFrame):
    """Display individual metric cards"""
    for _, row in latest_metrics.iterrows():
        metric_name = row['METRIC_NAME']
        value = row['VALUE']
        
        alert_class, status_emoji = get_alert_level(metric_name, value)
        
        st.markdown(f"""
        <div class="metric-card {alert_class}">
            <h4>{status_emoji} {metric_name.replace('_', ' ').title()}</h4>
            <h2>{value}</h2>
            <small>Last updated: {row['MEASUREMENT_TIME']}</small>
        </div>
        """, unsafe_allow_html=True)

def display_drill_down_analysis(latest_metrics: pd.DataFrame):
    """Display drill-down analysis section"""
    metric_options = ["Select a metric..."] + list(latest_metrics['METRIC_NAME'].unique())
    selected_metric = st.selectbox(
        "Select a data quality metric to investigate:",
        metric_options,
        key="metric_selector"
    )
    
    if selected_metric != "Select a metric...":
        current_value = latest_metrics[latest_metrics['METRIC_NAME'] == selected_metric]['VALUE'].iloc[0]
        st.markdown(f"**Current Issues Found:** {current_value}")
        
        if current_value > 0:
            with st.spinner("Fetching problematic records..."):
                problematic_records, query_used = get_problematic_records(selected_metric, 50)
            
            if not problematic_records.empty:
                # Create tabs for data and query
                tab1, tab2 = st.tabs(["📊 Data Table", "💻 SQL Query"])
                
                with tab1:
                    st.info(f"Found {len(problematic_records)} records with issues")
                    
                    st.dataframe(
                        problematic_records, 
                        use_container_width=True,
                        height=400
                    )
                    
                    # Download functionality
                    csv = problematic_records.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Problematic Records as CSV",
                        data=csv,
                        file_name=f"problematic_records_{selected_metric}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
                
                with tab2:
                    st.subheader("SQL Query Used")
                    st.code(query_used, language="sql")
                    
                    if st.button("📋 Copy Query to Clipboard"):
                        st.success("Query copied! (Note: Actual clipboard functionality requires additional setup)")
                        
            else:
                st.info("No problematic records found.")
        else:
            st.success("✅ No issues found for this metric!")
    else:
        st.info("👆 Select a metric above to see detailed analysis of problematic records.")

def display_trends_chart(df_metrics: pd.DataFrame):
    """Display data quality trends over time"""
    if len(df_metrics) > 1:
        fig = px.line(
            df_metrics,
            x='MEASUREMENT_TIME',
            y='VALUE',
            color='METRIC_NAME',
            title='Data Quality Metrics Over Time',
            labels={'VALUE': 'Metric Value', 'MEASUREMENT_TIME': 'Time'}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Not enough data points to show trends. Metrics will appear here as they are collected over time.")

def display_table_schema(table_profile: dict):
    """Display table schema information"""
    st.subheader("🗂️ Table Schema")
    
    table_info = table_profile.get('table_info', pd.DataFrame())
    if not table_info.empty:
        st.dataframe(table_info, use_container_width=True)
    else:
        st.info("Table schema information not available.")

def main():
    """Main application function"""
    st.markdown('<div class="main-header">📊 AMS Labs Data Quality Dashboard</div>', unsafe_allow_html=True)
    
    # Refresh button
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("🔄 Refresh Dashboard", type="primary", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    
    st.markdown("---")
    
    # Fetch data
    with st.spinner("Loading data quality metrics..."):
        df_metrics = get_data_quality_metrics()
        table_profile = get_table_profile()
    
    # Display overview metrics
    display_overview_metrics(df_metrics, table_profile)
    
    st.markdown("---")
    
    if df_metrics.empty:
        st.warning("⚠️ No data quality metrics found. Please run the SQL script first to set up data metric functions.")
        st.info("""
        **To get started:**
        1. Execute the `Lab1 - 01_data_quality.sql` script in Snowflake
        2. Wait a few minutes for the metrics to be collected
        3. Refresh this dashboard
        """)
        return
    
    # Main dashboard sections
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("🎯 Data Quality Metrics")
        
        # Get latest metrics for each type
        latest_metrics = df_metrics.groupby('METRIC_NAME').agg({
            'VALUE': 'last',
            'MEASUREMENT_TIME': 'max'
        }).reset_index()
        
        # Ensure VALUE column is numeric
        latest_metrics['VALUE'] = pd.to_numeric(latest_metrics['VALUE'], errors='coerce').fillna(0)
        
        # Display metric cards
        display_metric_cards(latest_metrics)
    
    with col2:
        st.subheader("🔍 Drill Down Analysis")
        display_drill_down_analysis(latest_metrics)
    
    # Trends section
    st.markdown("---")
    st.subheader("📈 Data Quality Trends")
    display_trends_chart(df_metrics)
    
    # Table schema section
    st.markdown("---")
    display_table_schema(table_profile)
    
    # Raw data section (collapsible)
    with st.expander("🔍 Raw Data Quality Results"):
        st.dataframe(df_metrics, use_container_width=True)

if __name__ == "__main__":
    main() 