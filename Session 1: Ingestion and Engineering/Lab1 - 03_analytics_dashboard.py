import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from snowflake.snowpark.context import get_active_session
from datetime import datetime, timedelta
import numpy as np

# Page configuration
st.set_page_config(
    page_title="AMS Labs Customer Analytics Dashboard",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for professional styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.8rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
    }
    .section-header {
        font-size: 1.8rem;
        font-weight: bold;
        color: #2c3e50;
        margin: 2rem 0 1rem 0;
        border-bottom: 3px solid #3498db;
        padding-bottom: 0.5rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 10px;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-bottom: 1rem;
    }
    .metric-value {
        font-size: 2.5rem;
        font-weight: bold;
        margin-bottom: 0.5rem;
    }
    .metric-label {
        font-size: 1rem;
        opacity: 0.9;
    }
    .filter-container {
        background-color: #f8f9fa;
        padding: 1.5rem;
        border-radius: 10px;
        border: 1px solid #e9ecef;
        margin-bottom: 2rem;
    }
    .insight-box {
        background-color: #e8f4fd;
        border-left: 4px solid #2196F3;
        padding: 1rem;
        margin: 1rem 0;
        border-radius: 0 5px 5px 0;
    }
    .warning-box {
        background-color: #fff3cd;
        border-left: 4px solid #ffc107;
        padding: 1rem;
        margin: 1rem 0;
        border-radius: 0 5px 5px 0;
    }
    .success-box {
        background-color: #d4edda;
        border-left: 4px solid #28a745;
        padding: 1rem;
        margin: 1rem 0;
        border-radius: 0 5px 5px 0;
    }
</style>
""", unsafe_allow_html=True)

# Initialize Snowflake session
@st.cache_resource
def init_session():
    return get_active_session()

session = init_session()

# Database configurations
DATABASE = 'AMS_LABS'
SCHEMA = 'DATA_ENGINEERING'

# Data loading functions with caching
@st.cache_data(ttl=300)
def load_hiring_metrics():
    """Load customer hiring metrics from Gold layer"""
    query = f"""
    SELECT 
        SNAPSHOT_YEAR,
        SNAPSHOT_QUARTER,
        SNAPSHOT_MONTH,
        GEOGRAPHIC_REGION,
        CANDIDATE_COUNTRY_CLEAN,
        JOB_FUNCTION_CATEGORY,
        APPLICATION_OUTCOME_CATEGORY,
        SOURCING_CHANNEL_TYPE_CLEAN,
        SOURCING_CHANNEL_NAME_CLEAN,
        HIRE_TIME_BUCKET,
        REJECTION_TIME_BUCKET,
        TOTAL_APPLICATIONS,
        UNIQUE_CANDIDATES,
        UNIQUE_JOBS,
        AVG_WORKING_DAYS_TO_HIRE,
        MEDIAN_WORKING_DAYS_TO_HIRE,
        MIN_WORKING_DAYS_TO_HIRE,
        MAX_WORKING_DAYS_TO_HIRE,
        AVG_WORKING_DAYS_TO_REJECTION,
        MEDIAN_WORKING_DAYS_TO_REJECTION,
        MIN_WORKING_DAYS_TO_REJECTION,
        MAX_WORKING_DAYS_TO_REJECTION,
        APPLIED_COUNT,
        IN_PROCESS_COUNT,
        OFFERED_COUNT,
        HIRED_COUNT,
        DECLINED_COUNT,
        WITHDRAWN_COUNT,
        OVERALL_SUCCESS_RATE
    FROM {DATABASE}.{SCHEMA}.GOLD_CUSTOMER_HIRING_METRICS
    WHERE SNAPSHOT_YEAR >= EXTRACT(YEAR FROM CURRENT_DATE()) - 1
    ORDER BY SNAPSHOT_YEAR DESC, SNAPSHOT_MONTH DESC
    """
    
    try:
        df = session.sql(query).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error loading hiring metrics: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_application_trends():
    """Load application trends from Gold layer"""
    query = f"""
    SELECT 
        SNAPSHOT_DATE,
        SNAPSHOT_YEAR,
        SNAPSHOT_MONTH,
        GEOGRAPHIC_REGION,
        DAILY_APPLICATIONS,
        DAILY_UNIQUE_CANDIDATES,
        ROLLING_30DAY_AVG_APPLICATIONS,
        DAILY_SUCCESS_RATE,
        TOP_SOURCING_CHANNEL,
        FRESH_APPLICATIONS,
        AGED_APPLICATIONS
    FROM {DATABASE}.{SCHEMA}.GOLD_APPLICATION_TRENDS
    WHERE SNAPSHOT_DATE >= CURRENT_DATE() - 365
    ORDER BY SNAPSHOT_DATE DESC
    """
    
    try:
        df = session.sql(query).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error loading application trends: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_detailed_applications():
    """Load detailed application data for drill-through analysis"""
    query = f"""
    SELECT 
        UNIQUE_APPLICATION_ID,
        JOB_TITLE_CLEAN,
        JOB_FUNCTION_CATEGORY,
        GEOGRAPHIC_REGION,
        CANDIDATE_COUNTRY_CLEAN,
        APPLICATION_STATUS_CLEAN,
        APPLICATION_OUTCOME_CATEGORY,
        SOURCING_CHANNEL_TYPE_CLEAN,
        SOURCING_CHANNEL_NAME_CLEAN,
        WORKING_DAYS_TO_HIRE,
        WORKING_DAYS_TO_REJECTION,
        HIRE_TIME_BUCKET,
        REJECTION_TIME_BUCKET,
        SNAPSHOT_DATE,
        SNAPSHOT_YEAR,
        SNAPSHOT_MONTH
    FROM {DATABASE}.{SCHEMA}.SILVER_APPLICATIONS_ENRICHED
    WHERE SNAPSHOT_DATE >= CURRENT_DATE() - 365
    ORDER BY SNAPSHOT_DATE DESC
    LIMIT 10000
    """
    
    try:
        df = session.sql(query).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error loading detailed applications: {str(e)}")
        return pd.DataFrame()

def create_metric_card(title, value, subtitle=""):
    """Create a styled metric card"""
    return f"""
    <div class="metric-card">
        <div class="metric-value">{value}</div>
        <div class="metric-label">{title}</div>
        {f'<div style="font-size: 0.8rem; margin-top: 0.5rem;">{subtitle}</div>' if subtitle else ''}
    </div>
    """

def main():
    """Main application function"""
    st.markdown('<div class="main-header">🎯 AMS Labs Customer Analytics Dashboard</div>', unsafe_allow_html=True)
    
    # Load data
    with st.spinner("Loading customer analytics data..."):
        hiring_metrics = load_hiring_metrics()
        trends_data = load_application_trends()
        detailed_data = load_detailed_applications()
    
    if hiring_metrics.empty and trends_data.empty:
        st.error("⚠️ No data available. Please ensure the Gold layer tables are populated.")
        st.info("Run the transformation pipeline: `Lab1 - 02_data_transformation.sql`")
        return
    
    # Sidebar filters
    st.sidebar.markdown("## 🔍 Interactive Filters")
    
    # Refresh button
    if st.sidebar.button("🔄 Refresh Dashboard", type="primary"):
        st.cache_data.clear()
        st.rerun()
    
    st.sidebar.markdown("---")
    
    # Region filter
    if not hiring_metrics.empty:
        regions = ['All Regions'] + list(hiring_metrics['GEOGRAPHIC_REGION'].dropna().unique())
        selected_region = st.sidebar.selectbox("🌍 Geographic Region", regions)
    
    # Job function filter
    if not hiring_metrics.empty:
        functions = ['All Functions'] + list(hiring_metrics['JOB_FUNCTION_CATEGORY'].dropna().unique())
        selected_function = st.sidebar.selectbox("💼 Job Function", functions)
    
    # Sourcing channel filter
    if not hiring_metrics.empty:
        channels = ['All Channels'] + list(hiring_metrics['SOURCING_CHANNEL_TYPE_CLEAN'].dropna().unique())
        selected_channel = st.sidebar.selectbox("📢 Sourcing Channel", channels)
    
    # Apply filters
    filtered_hiring = hiring_metrics.copy()
    filtered_trends = trends_data.copy()
    filtered_detailed = detailed_data.copy()
    
    if not hiring_metrics.empty:
        if selected_region != 'All Regions':
            filtered_hiring = filtered_hiring[filtered_hiring['GEOGRAPHIC_REGION'] == selected_region]
            if not trends_data.empty:
                filtered_trends = filtered_trends[filtered_trends['GEOGRAPHIC_REGION'] == selected_region]
            if not detailed_data.empty:
                filtered_detailed = filtered_detailed[filtered_detailed['GEOGRAPHIC_REGION'] == selected_region]
        
        if selected_function != 'All Functions':
            filtered_hiring = filtered_hiring[filtered_hiring['JOB_FUNCTION_CATEGORY'] == selected_function]
            if not detailed_data.empty:
                filtered_detailed = filtered_detailed[filtered_detailed['JOB_FUNCTION_CATEGORY'] == selected_function]
        
        if selected_channel != 'All Channels':
            filtered_hiring = filtered_hiring[filtered_hiring['SOURCING_CHANNEL_TYPE_CLEAN'] == selected_channel]
            if not detailed_data.empty:
                filtered_detailed = filtered_detailed[filtered_detailed['SOURCING_CHANNEL_TYPE_CLEAN'] == selected_channel]
    
    # Main dashboard tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Executive Overview", 
        "🔄 Application Funnel Flow",
        "⏱️ Time-to-Hire Analysis", 
        "❌ Rejection Analysis", 
        "📢 Sourcing Performance", 
        "🗺️ Geographic Distribution"
    ])
    
    with tab1:
        show_executive_overview(filtered_hiring, filtered_trends)
    
    with tab2:
        show_application_funnel_flow(filtered_hiring, filtered_detailed)
    
    with tab3:
        show_time_to_hire_analysis(filtered_hiring, filtered_trends, filtered_detailed)
    
    with tab4:
        show_rejection_analysis(filtered_hiring, filtered_detailed)
    
    with tab5:
        show_sourcing_performance(filtered_hiring, filtered_detailed)
    
    with tab6:
        show_geographic_distribution(filtered_hiring, filtered_detailed)

def show_executive_overview(hiring_metrics, trends_data):
    """Executive overview with key metrics"""
    st.markdown('<div class="section-header">📊 Executive Overview</div>', unsafe_allow_html=True)
    
    if not hiring_metrics.empty:
        # Key metrics row
        col1, col2, col3, col4 = st.columns(4)
        
        total_apps = hiring_metrics['TOTAL_APPLICATIONS'].sum()
        avg_success_rate = hiring_metrics['OVERALL_SUCCESS_RATE'].mean()
        avg_time_to_hire = hiring_metrics['AVG_WORKING_DAYS_TO_HIRE'].mean()
        unique_candidates = hiring_metrics['UNIQUE_CANDIDATES'].sum()
        
        with col1:
            st.markdown(create_metric_card("Total Applications", f"{total_apps:,}"), unsafe_allow_html=True)
        
        with col2:
            st.markdown(create_metric_card("Success Rate", f"{avg_success_rate:.1f}%"), unsafe_allow_html=True)
        
        with col3:
            st.markdown(create_metric_card("Avg Time to Hire", f"{avg_time_to_hire:.0f} days"), unsafe_allow_html=True)
        
        with col4:
            st.markdown(create_metric_card("Unique Candidates", f"{unique_candidates:,}"), unsafe_allow_html=True)
        
        # Performance insights
        st.markdown("### 🎯 Key Performance Insights")
        
        # Best performing region
        if len(hiring_metrics) > 1:
            best_region = hiring_metrics.groupby('GEOGRAPHIC_REGION')['OVERALL_SUCCESS_RATE'].mean().idxmax()
            best_rate = hiring_metrics.groupby('GEOGRAPHIC_REGION')['OVERALL_SUCCESS_RATE'].mean().max()
            
            st.markdown(f"""
            <div class="success-box">
                <strong>🏆 Top Performing Region:</strong> {best_region} with {best_rate:.1f}% success rate
            </div>
            """, unsafe_allow_html=True)
        
        # Hiring efficiency analysis
        fast_hires = hiring_metrics[hiring_metrics['AVG_WORKING_DAYS_TO_HIRE'] <= 30]['TOTAL_APPLICATIONS'].sum()
        total_hires = hiring_metrics['HIRED_COUNT'].sum()
        
        if total_hires > 0:
            efficiency_rate = (fast_hires / total_apps) * 100
            st.markdown(f"""
            <div class="insight-box">
                <strong>⚡ Hiring Efficiency:</strong> {efficiency_rate:.1f}% of applications processed within 30 days
            </div>
            """, unsafe_allow_html=True)
    
    # Trends overview
    if not trends_data.empty:
        st.markdown("### 📈 Application Volume Trends")
        
        # Create trend chart
        fig = px.line(
            trends_data,
            x='SNAPSHOT_DATE',
            y='DAILY_APPLICATIONS',
            color='GEOGRAPHIC_REGION',
            title="Daily Application Volume by Region",
            labels={'DAILY_APPLICATIONS': 'Applications', 'SNAPSHOT_DATE': 'Date'}
        )
        
        # Add rolling average
        fig.add_scatter(
            x=trends_data['SNAPSHOT_DATE'],
            y=trends_data['ROLLING_30DAY_AVG_APPLICATIONS'],
            mode='lines',
            name='30-Day Average',
            line=dict(dash='dash', color='red', width=2)
        )
        
        fig.update_layout(height=400, showlegend=True)
        st.plotly_chart(fig, use_container_width=True)

def show_application_funnel_flow(hiring_metrics, detailed_data):
    """Customer Requirement: Interactive Sankey Diagram showing application flow through recruitment stages"""
    st.markdown('<div class="section-header">🔄 Application Funnel Flow</div>', unsafe_allow_html=True)
    
    if not detailed_data.empty:
        # Calculate funnel flow data
        flow_data = calculate_funnel_flow_data(detailed_data)
        
        if flow_data:
            # Create Sankey diagram
            st.markdown("### 📊 Recruitment Process Flow")
            
            # Sankey diagram using Plotly
            # Create enhanced Sankey diagram
            fig = go.Figure(data=[go.Sankey(
                node=dict(
                    pad=20,
                    thickness=25,
                    line=dict(color="black", width=1),
                    label=flow_data['node_labels'],
                    color=flow_data['node_colors'],
                    hovertemplate='%{label}<br>%{value} applications<extra></extra>'
                ),
                link=dict(
                    source=flow_data['source_indices'],
                    target=flow_data['target_indices'], 
                    value=flow_data['values'],
                    color=flow_data['link_colors'],
                    hovertemplate='%{source.label} → %{target.label}<br>%{value} applications<extra></extra>'
                )
            )])
            
            fig.update_layout(
                title=dict(
                    text="Recruitment Process Flow - Application Journey",
                    x=0.5,
                    font=dict(size=16, color="black", family="Arial, sans-serif")
                ),
                font=dict(
                    color="black", 
                    size=12,
                    family="Arial, sans-serif"
                ),
                paper_bgcolor="white",
                plot_bgcolor="white",
                height=650,
                margin=dict(l=10, r=10, t=80, b=10)
            )

            
            st.plotly_chart(fig, use_container_width=True)
            
            # Add process flow insights below the diagram
            total_applications = detailed_data.shape[0]
            applied_count = detailed_data[detailed_data['APPLICATION_OUTCOME_CATEGORY'] == 'Applied'].shape[0]
            in_process_count = detailed_data[detailed_data['APPLICATION_OUTCOME_CATEGORY'] == 'In Process'].shape[0]
            offered_count = detailed_data[detailed_data['APPLICATION_OUTCOME_CATEGORY'] == 'Offered'].shape[0]
            hired_count = detailed_data[detailed_data['APPLICATION_OUTCOME_CATEGORY'] == 'Hired'].shape[0]
            declined_count = detailed_data[detailed_data['APPLICATION_OUTCOME_CATEGORY'] == 'Declined'].shape[0]
            withdrawn_count = detailed_data[detailed_data['APPLICATION_OUTCOME_CATEGORY'] == 'Withdrawn'].shape[0]
            
            st.markdown("### 🎯 Process Flow Insights")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # Initial screening success rate
                review_candidates = in_process_count + offered_count + hired_count + int(declined_count * 0.7)
                screening_rate = (review_candidates / total_applications * 100) if total_applications > 0 else 0
                st.markdown(f"""
                <div class="insight-box">
                    <strong>📈 Initial Screening:</strong><br>
                    {screening_rate:.1f}% pass initial screening<br>
                    ({review_candidates:,} from {total_applications:,} total)
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                # Interview to offer conversion
                reached_offers = offered_count + hired_count
                offer_rate = (reached_offers / review_candidates * 100) if review_candidates > 0 else 0
                st.markdown(f"""
                <div class="success-box">
                    <strong>🎯 Interview Success:</strong><br>
                    {offer_rate:.1f}% of interviewed candidates get offers<br>
                    ({reached_offers:,} from {review_candidates:,} interviewed)
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                # Offer acceptance rate
                offer_acceptance_rate = (hired_count / reached_offers * 100) if reached_offers > 0 else 0
                st.markdown(f"""
                <div class="success-box">
                    <strong>✅ Offer Acceptance:</strong><br>
                    {offer_acceptance_rate:.1f}% of offers are accepted<br>
                    ({hired_count:,} from {reached_offers:,} offers)
                </div>
                """, unsafe_allow_html=True)
            
            # Overall funnel efficiency
            col4, col5, col6 = st.columns(3)
            
            with col4:
                overall_conversion = (hired_count / total_applications * 100) if total_applications > 0 else 0
                st.markdown(f"""
                <div class="metric-card" style="background: linear-gradient(135deg, #4CAF50 0%, #45a049 100%);">
                    <div class="metric-value">{overall_conversion:.1f}%</div>
                    <div class="metric-label">End-to-End Conversion</div>
                    <div style="font-size: 0.8rem; margin-top: 0.5rem;">{hired_count:,} hired from {total_applications:,} total</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col5:
                rejection_rate = (declined_count / total_applications * 100) if total_applications > 0 else 0
                st.markdown(f"""
                <div class="warning-box">
                    <strong>❌ Rejection Rate:</strong><br>
                    {rejection_rate:.1f}% of all applications declined<br>
                    ({declined_count:,} declined applications)
                </div>
                """, unsafe_allow_html=True)
            
            with col6:
                withdrawal_rate = (withdrawn_count / total_applications * 100) if total_applications > 0 else 0
                st.markdown(f"""
                <div style="background-color: #f8f9fa; border-left: 4px solid #6c757d; padding: 1rem; margin: 1rem 0; border-radius: 0 5px 5px 0;">
                    <strong>🚪 Withdrawal Rate:</strong><br>
                    {withdrawal_rate:.1f}% of applications withdrawn<br>
                    ({withdrawn_count:,} withdrawn applications)
                </div>
                """, unsafe_allow_html=True)
            
            # Stage-by-stage breakdown
            st.markdown("### 📈 Stage-by-Stage Breakdown")
            
            stage_breakdown = detailed_data['APPLICATION_OUTCOME_CATEGORY'].value_counts().reset_index()
            stage_breakdown.columns = ['Stage', 'Count']
            stage_breakdown['Percentage'] = (stage_breakdown['Count'] / total_applications * 100).round(1)
            
            fig_breakdown = px.bar(
                stage_breakdown,
                x='Stage',
                y='Count',
                text='Percentage',
                title="Application Count by Stage",
                color='Count',
                color_continuous_scale='Blues'
            )
            
            # Add percentage labels on bars
            fig_breakdown.update_traces(texttemplate='%{text}%', textposition='outside')
            fig_breakdown.update_layout(height=400)
            
            st.plotly_chart(fig_breakdown, use_container_width=True)
            
        else:
            st.warning("Insufficient data to generate funnel flow diagram.")
    else:
        st.error("No detailed application data available for funnel analysis.")

def calculate_funnel_flow_data(detailed_data):
    """Calculate data for Sankey diagram showing realistic recruitment process flow"""
    try:
        # Get actual counts from data
        stage_counts = detailed_data['APPLICATION_OUTCOME_CATEGORY'].value_counts().to_dict()
        total_apps = len(detailed_data)
        
        # Final outcome counts
        applied_count = stage_counts.get('Applied', 0) 
        in_process_count = stage_counts.get('In Process', 0)
        offered_count = stage_counts.get('Offered', 0)
        hired_count = stage_counts.get('Hired', 0)
        declined_count = stage_counts.get('Declined', 0)
        withdrawn_count = stage_counts.get('Withdrawn', 0)
        
        # Calculate realistic funnel progression
        # For a proper funnel, we need to think about flow quantities that make sense
        
        # All applications start as "Applied" - this represents the initial entry point
        total_started = total_apps
        
        # Those who progressed past initial screening (In Process + Offered + Hired + some Declined)
        # Declined can happen at any stage, so let's assume 70% of declined happened after initial review
        progressed_to_review = in_process_count + offered_count + hired_count + int(declined_count * 0.7)
        
        # Those who reached offer stage (Offered + Hired + some who declined offers)
        reached_offers = offered_count + hired_count
        
        # Calculate early rejections (declined during initial screening)
        early_declined = declined_count - int(declined_count * 0.7)
        
        # Define nodes with realistic flow structure
        node_labels = [
            f"Total Applications\n{total_started:,}",
            f"Initial Review\n{progressed_to_review:,}",
            f"Offers Made\n{reached_offers:,}",
            f"Hired\n{hired_count:,}",
            f"Declined\n{declined_count:,}",
            f"Withdrawn\n{withdrawn_count:,}"
        ]
        
        # Color scheme for realistic process
        node_colors = [
            "#E3F2FD",  # Total Applications - very light blue
            "#FFE082",  # Initial Review - yellow (active review)
            "#C8E6C9",  # Offers Made - light green 
            "#4CAF50",  # Hired - dark green (success)
            "#FFCDD2",  # Declined - light red (negative outcome)
            "#F5F5F5"   # Withdrawn - gray (neutral)
        ]
        
        # Define flows
        source_indices = []
        target_indices = []
        values = []
        link_colors = []
        
        # 1. Total Applications -> Initial Review (successful initial screening)
        if progressed_to_review > 0:
            source_indices.append(0)  # Total Applications
            target_indices.append(1)  # Initial Review  
            values.append(progressed_to_review)
            link_colors.append("rgba(255, 224, 130, 0.5)")
        
        # 2. Total Applications -> Early Declined (failed initial screening)
        if early_declined > 0:
            source_indices.append(0)  # Total Applications
            target_indices.append(4)  # Declined
            values.append(early_declined)
            link_colors.append("rgba(255, 205, 210, 0.5)")
        
        # 3. Total Applications -> Withdrawn (early withdrawal)
        if withdrawn_count > 0:
            source_indices.append(0)  # Total Applications
            target_indices.append(5)  # Withdrawn
            values.append(withdrawn_count)
            link_colors.append("rgba(245, 245, 245, 0.5)")
        
        # 4. Initial Review -> Offers Made (successful through interviews)
        if reached_offers > 0:
            source_indices.append(1)  # Initial Review
            target_indices.append(2)  # Offers Made
            values.append(reached_offers)
            link_colors.append("rgba(200, 230, 201, 0.5)")
        
        # 5. Initial Review -> Later Declined (failed during interview process)
        later_declined = int(declined_count * 0.7)
        if later_declined > 0:
            source_indices.append(1)  # Initial Review
            target_indices.append(4)  # Declined
            values.append(later_declined)
            link_colors.append("rgba(255, 205, 210, 0.5)")
        
        # 6. Offers Made -> Hired (accepted offers)
        if hired_count > 0:
            source_indices.append(2)  # Offers Made
            target_indices.append(3)  # Hired
            values.append(hired_count)
            link_colors.append("rgba(76, 175, 80, 0.6)")
        
        # 7. Offers Made -> Declined (declined offers - if any)
        declined_offers = reached_offers - hired_count
        if declined_offers > 0:
            source_indices.append(2)  # Offers Made
            target_indices.append(4)  # Declined
            values.append(declined_offers)
            link_colors.append("rgba(255, 205, 210, 0.5)")
        
        return {
            'node_labels': node_labels,
            'node_colors': node_colors,
            'source_indices': source_indices,
            'target_indices': target_indices,
            'values': values,
            'link_colors': link_colors
        }
        
    except Exception as e:
        st.error(f"Error calculating funnel flow data: {str(e)}")
        return None


def show_time_to_hire_analysis(hiring_metrics, trends_data, detailed_data):
    """Customer Requirement: Time-to-Hire Analysis with 13-month trends"""
    st.markdown('<div class="section-header">⏱️ Time-to-Hire Analysis</div>', unsafe_allow_html=True)
    
    if not hiring_metrics.empty:
        # Time-to-hire metrics
        col1, col2, col3 = st.columns(3)
        
        avg_days = hiring_metrics['AVG_WORKING_DAYS_TO_HIRE'].mean()
        median_days = hiring_metrics['MEDIAN_WORKING_DAYS_TO_HIRE'].median()
        min_days = hiring_metrics['MIN_WORKING_DAYS_TO_HIRE'].min()
        
        with col1:
            st.markdown(create_metric_card("Average Days", f"{avg_days:.0f}", "Working days to hire"), unsafe_allow_html=True)
        
        with col2:
            st.markdown(create_metric_card("Median Days", f"{median_days:.0f}", "50th percentile"), unsafe_allow_html=True)
        
        with col3:
            st.markdown(create_metric_card("Fastest Hire", f"{min_days:.0f}", "Minimum days"), unsafe_allow_html=True)
        
        # 13-month trend analysis
        st.markdown("### 📊 13-Month Time-to-Hire Trends")
        
        # Aggregate by month for trend analysis
        monthly_trends = hiring_metrics.groupby(['SNAPSHOT_YEAR', 'SNAPSHOT_MONTH', 'GEOGRAPHIC_REGION']).agg({
            'AVG_WORKING_DAYS_TO_HIRE': 'mean',
            'MEDIAN_WORKING_DAYS_TO_HIRE': 'mean',
            'TOTAL_APPLICATIONS': 'sum'
        }).reset_index()
        
        # Create date column for plotting
        monthly_trends['DATE'] = pd.to_datetime({
            'year': monthly_trends['SNAPSHOT_YEAR'],
            'month': monthly_trends['SNAPSHOT_MONTH'],
            'day': 1
        })
        
        # Line chart for time-to-hire trends
        fig = px.line(
            monthly_trends,
            x='DATE',
            y='AVG_WORKING_DAYS_TO_HIRE',
            color='GEOGRAPHIC_REGION',
            title="Average Time-to-Hire Trends by Region (13 Months)",
            labels={'AVG_WORKING_DAYS_TO_HIRE': 'Average Working Days', 'DATE': 'Month'}
        )
        
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Time bucket distribution
        st.markdown("### ⏰ Hiring Time Distribution")
        
        time_buckets = hiring_metrics.groupby('HIRE_TIME_BUCKET')['TOTAL_APPLICATIONS'].sum().reset_index()
        
        fig_bucket = px.pie(
            time_buckets,
            values='TOTAL_APPLICATIONS',
            names='HIRE_TIME_BUCKET',
            title="Distribution of Hiring Times",
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        
        fig_bucket.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_bucket, use_container_width=True)
        
        # Performance benchmarking
        if not detailed_data.empty:
            st.markdown("### 🎯 Performance Benchmarking")
            
            # Fast vs slow hiring analysis
            fast_threshold = 14  # 2 weeks
            slow_threshold = 60  # 2 months
            
            fast_hires = detailed_data[detailed_data['WORKING_DAYS_TO_HIRE'] <= fast_threshold]
            slow_hires = detailed_data[detailed_data['WORKING_DAYS_TO_HIRE'] >= slow_threshold]
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### ⚡ Fast Hires (≤14 days)")
                if not fast_hires.empty:
                    fast_channels = fast_hires['SOURCING_CHANNEL_TYPE_CLEAN'].value_counts().head(5)
                    fig_fast = px.bar(
                        x=fast_channels.values,
                        y=fast_channels.index,
                        orientation='h',
                        title="Top Channels for Fast Hires",
                        color=fast_channels.values,
                        color_continuous_scale='Greens'
                    )
                    st.plotly_chart(fig_fast, use_container_width=True)
            
            with col2:
                st.markdown("#### 🐌 Slow Hires (≥60 days)")
                if not slow_hires.empty:
                    slow_channels = slow_hires['SOURCING_CHANNEL_TYPE_CLEAN'].value_counts().head(5)
                    fig_slow = px.bar(
                        x=slow_channels.values,
                        y=slow_channels.index,
                        orientation='h',
                        title="Channels with Slow Hires",
                        color=slow_channels.values,
                        color_continuous_scale='Reds'
                    )
                    st.plotly_chart(fig_slow, use_container_width=True)

def show_rejection_analysis(hiring_metrics, detailed_data):
    """Customer Requirement: Rejection Stage Analysis with pie charts"""
    st.markdown('<div class="section-header">❌ Rejection Analysis</div>', unsafe_allow_html=True)
    
    if not hiring_metrics.empty:
        # Rejection metrics
        col1, col2, col3 = st.columns(3)
        
        total_declined = hiring_metrics['DECLINED_COUNT'].sum()
        avg_rejection_days = hiring_metrics['AVG_WORKING_DAYS_TO_REJECTION'].mean()
        rejection_rate = (total_declined / hiring_metrics['TOTAL_APPLICATIONS'].sum()) * 100
        
        with col1:
            st.markdown(create_metric_card("Total Rejections", f"{total_declined:,}"), unsafe_allow_html=True)
        
        with col2:
            st.markdown(create_metric_card("Avg Days to Rejection", f"{avg_rejection_days:.0f}"), unsafe_allow_html=True)
        
        with col3:
            st.markdown(create_metric_card("Rejection Rate", f"{rejection_rate:.1f}%"), unsafe_allow_html=True)
        
        # Rejection time bucket analysis
        st.markdown("### ⏱️ Time-to-Rejection Distribution")
        
        rejection_buckets = hiring_metrics.groupby('REJECTION_TIME_BUCKET')['DECLINED_COUNT'].sum().reset_index()
        rejection_buckets = rejection_buckets[rejection_buckets['DECLINED_COUNT'] > 0]
        
        if not rejection_buckets.empty:
            fig_rejection_time = px.pie(
                rejection_buckets,
                values='DECLINED_COUNT',
                names='REJECTION_TIME_BUCKET',
                title="Rejection Timeline Distribution",
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            
            fig_rejection_time.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_rejection_time, use_container_width=True)
        
        # Regional rejection analysis
        st.markdown("### 🌍 Rejection Analysis by Region")
        
        regional_rejections = hiring_metrics.groupby('GEOGRAPHIC_REGION').agg({
            'DECLINED_COUNT': 'sum',
            'TOTAL_APPLICATIONS': 'sum'
        }).reset_index()
        
        regional_rejections['REJECTION_RATE'] = (
            regional_rejections['DECLINED_COUNT'] / regional_rejections['TOTAL_APPLICATIONS'] * 100
        )
        
        fig_regional_rejection = px.bar(
            regional_rejections,
            x='GEOGRAPHIC_REGION',
            y='REJECTION_RATE',
            title="Rejection Rate by Geographic Region",
            color='REJECTION_RATE',
            color_continuous_scale='Reds',
            labels={'REJECTION_RATE': 'Rejection Rate (%)', 'GEOGRAPHIC_REGION': 'Region'}
        )
        
        st.plotly_chart(fig_regional_rejection, use_container_width=True)
        
        # Detailed rejection analysis
        if not detailed_data.empty:
            st.markdown("### 🔍 Detailed Rejection Insights")
            
            declined_data = detailed_data[detailed_data['APPLICATION_OUTCOME_CATEGORY'] == 'Declined']
            
            if not declined_data.empty:
                col1, col2 = st.columns(2)
                
                with col1:
                    # Rejection by job function
                    function_rejections = declined_data['JOB_FUNCTION_CATEGORY'].value_counts()
                    fig_function = px.pie(
                        values=function_rejections.values,
                        names=function_rejections.index,
                        title="Rejections by Job Function",
                        color_discrete_sequence=px.colors.qualitative.Set2
                    )
                    st.plotly_chart(fig_function, use_container_width=True)
                
                with col2:
                    # Rejection by sourcing channel
                    channel_rejections = declined_data['SOURCING_CHANNEL_TYPE_CLEAN'].value_counts()
                    fig_channel = px.pie(
                        values=channel_rejections.values,
                        names=channel_rejections.index,
                        title="Rejections by Sourcing Channel",
                        color_discrete_sequence=px.colors.qualitative.Dark2
                    )
                    st.plotly_chart(fig_channel, use_container_width=True)

def show_sourcing_performance(hiring_metrics, detailed_data):
    """Customer Requirement: Sourcing Channel Performance with drill-through"""
    st.markdown('<div class="section-header">📢 Sourcing Channel Performance</div>', unsafe_allow_html=True)
    
    if not hiring_metrics.empty:
        # Channel performance overview
        channel_performance = hiring_metrics.groupby('SOURCING_CHANNEL_TYPE_CLEAN').agg({
            'TOTAL_APPLICATIONS': 'sum',
            'HIRED_COUNT': 'sum',
            'OVERALL_SUCCESS_RATE': 'mean',
            'AVG_WORKING_DAYS_TO_HIRE': 'mean'
        }).reset_index()
        
        channel_performance['SUCCESS_RATE'] = (
            channel_performance['HIRED_COUNT'] / channel_performance['TOTAL_APPLICATIONS'] * 100
        )
        
        # Top performing channels
        st.markdown("### 🏆 Top Performing Sourcing Channels")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Volume-based performance
            fig_volume = px.bar(
                channel_performance.nlargest(10, 'TOTAL_APPLICATIONS'),
                x='TOTAL_APPLICATIONS',
                y='SOURCING_CHANNEL_TYPE_CLEAN',
                orientation='h',
                title="Channels by Application Volume",
                color='TOTAL_APPLICATIONS',
                color_continuous_scale='Blues'
            )
            st.plotly_chart(fig_volume, use_container_width=True)
        
        with col2:
            # Success rate performance
            fig_success = px.bar(
                channel_performance.nlargest(10, 'SUCCESS_RATE'),
                x='SUCCESS_RATE',
                y='SOURCING_CHANNEL_TYPE_CLEAN',
                orientation='h',
                title="Channels by Success Rate",
                color='SUCCESS_RATE',
                color_continuous_scale='Greens'
            )
            st.plotly_chart(fig_success, use_container_width=True)
        
        # Channel efficiency analysis
        st.markdown("### ⚡ Channel Efficiency Analysis")
        
        # Scatter plot: Volume vs Success Rate
        fig_efficiency = px.scatter(
            channel_performance,
            x='TOTAL_APPLICATIONS',
            y='SUCCESS_RATE',
            size='HIRED_COUNT',
            color='AVG_WORKING_DAYS_TO_HIRE',
            hover_name='SOURCING_CHANNEL_TYPE_CLEAN',
            title="Channel Performance: Volume vs Success Rate",
            labels={
                'TOTAL_APPLICATIONS': 'Total Applications',
                'SUCCESS_RATE': 'Success Rate (%)',
                'AVG_WORKING_DAYS_TO_HIRE': 'Avg Days to Hire'
            },
            color_continuous_scale='RdYlGn_r'
        )
        
        st.plotly_chart(fig_efficiency, use_container_width=True)
        
        # Drill-through analysis
        st.markdown("### 🔍 Drill-Through Channel Analysis")
        
        # Channel selector for drill-through
        selected_channel_detail = st.selectbox(
            "Select a channel for detailed analysis:",
            ['Select a channel...'] + list(channel_performance['SOURCING_CHANNEL_TYPE_CLEAN'].unique())
        )
        
        if selected_channel_detail != 'Select a channel...' and not detailed_data.empty:
            channel_detail_data = detailed_data[
                detailed_data['SOURCING_CHANNEL_TYPE_CLEAN'] == selected_channel_detail
            ]
            
            if not channel_detail_data.empty:
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    # Outcome distribution
                    outcomes = channel_detail_data['APPLICATION_OUTCOME_CATEGORY'].value_counts()
                    fig_outcomes = px.pie(
                        values=outcomes.values,
                        names=outcomes.index,
                        title=f"{selected_channel_detail} - Outcomes"
                    )
                    st.plotly_chart(fig_outcomes, use_container_width=True)
                
                with col2:
                    # Geographic distribution
                    regions = channel_detail_data['GEOGRAPHIC_REGION'].value_counts()
                    fig_regions = px.pie(
                        values=regions.values,
                        names=regions.index,
                        title=f"{selected_channel_detail} - Regions"
                    )
                    st.plotly_chart(fig_regions, use_container_width=True)
                
                with col3:
                    # Job function distribution
                    functions = channel_detail_data['JOB_FUNCTION_CATEGORY'].value_counts()
                    fig_functions = px.pie(
                        values=functions.values,
                        names=functions.index,
                        title=f"{selected_channel_detail} - Functions"
                    )
                    st.plotly_chart(fig_functions, use_container_width=True)
                
                # Time-to-hire distribution for selected channel
                hired_data = channel_detail_data[
                    channel_detail_data['APPLICATION_OUTCOME_CATEGORY'] == 'Hired'
                ]
                
                if not hired_data.empty:
                    st.markdown(f"#### ⏱️ Time-to-Hire Distribution for {selected_channel_detail}")
                    
                    fig_time_dist = px.histogram(
                        hired_data,
                        x='WORKING_DAYS_TO_HIRE',
                        nbins=20,
                        title=f"Time-to-Hire Distribution - {selected_channel_detail}",
                        labels={'WORKING_DAYS_TO_HIRE': 'Working Days to Hire', 'count': 'Number of Hires'}
                    )
                    
                    st.plotly_chart(fig_time_dist, use_container_width=True)

def show_geographic_distribution(hiring_metrics, detailed_data):
    """Customer Requirement: Geographic Distribution with map visualization"""
    st.markdown('<div class="section-header">🗺️ Geographic Distribution</div>', unsafe_allow_html=True)
    
    if not hiring_metrics.empty:
        # Regional overview
        regional_summary = hiring_metrics.groupby('GEOGRAPHIC_REGION').agg({
            'TOTAL_APPLICATIONS': 'sum',
            'HIRED_COUNT': 'sum',
            'OVERALL_SUCCESS_RATE': 'mean',
            'AVG_WORKING_DAYS_TO_HIRE': 'mean'
        }).reset_index()
        
        regional_summary['SUCCESS_RATE'] = (
            regional_summary['HIRED_COUNT'] / regional_summary['TOTAL_APPLICATIONS'] * 100
        )
        
        # Regional performance metrics
        st.markdown("### 🌍 Regional Performance Overview")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Applications by region
            fig_regional_apps = px.pie(
                regional_summary,
                values='TOTAL_APPLICATIONS',
                names='GEOGRAPHIC_REGION',
                title="Applications by Region",
                color_discrete_sequence=px.colors.qualitative.Set1
            )
            st.plotly_chart(fig_regional_apps, use_container_width=True)
        
        with col2:
            # Success rate by region
            fig_regional_success = px.bar(
                regional_summary,
                x='GEOGRAPHIC_REGION',
                y='SUCCESS_RATE',
                title="Success Rate by Region",
                color='SUCCESS_RATE',
                color_continuous_scale='Greens'
            )
            st.plotly_chart(fig_regional_success, use_container_width=True)
        
        with col3:
            # Time to hire by region
            fig_regional_time = px.bar(
                regional_summary,
                x='GEOGRAPHIC_REGION',
                y='AVG_WORKING_DAYS_TO_HIRE',
                title="Avg Time to Hire by Region",
                color='AVG_WORKING_DAYS_TO_HIRE',
                color_continuous_scale='Reds'
            )
            st.plotly_chart(fig_regional_time, use_container_width=True)
        
        # Country-level analysis
        if not detailed_data.empty:
            st.markdown("### 🏳️ Country-Level Analysis")
            
            country_summary = detailed_data.groupby(['CANDIDATE_COUNTRY_CLEAN', 'GEOGRAPHIC_REGION']).agg({
                'UNIQUE_APPLICATION_ID': 'count',
                'WORKING_DAYS_TO_HIRE': 'mean'
            }).reset_index()
            
            country_summary.columns = ['COUNTRY', 'REGION', 'APPLICATIONS', 'AVG_DAYS_TO_HIRE']
            country_summary = country_summary[country_summary['APPLICATIONS'] >= 5]  # Filter for meaningful data
            
            # Top countries by application volume
            top_countries = country_summary.nlargest(15, 'APPLICATIONS')
            
            fig_countries = px.bar(
                top_countries,
                x='APPLICATIONS',
                y='COUNTRY',
                color='REGION',
                orientation='h',
                title="Top 15 Countries by Application Volume",
                labels={'APPLICATIONS': 'Number of Applications', 'COUNTRY': 'Country'}
            )
            
            st.plotly_chart(fig_countries, use_container_width=True)
            
            # Geographic performance heatmap
            st.markdown("### 🔥 Geographic Performance Heatmap")
            
            # Create a pivot table for heatmap
            heatmap_data = hiring_metrics.pivot_table(
                values='OVERALL_SUCCESS_RATE',
                index='GEOGRAPHIC_REGION',
                columns='JOB_FUNCTION_CATEGORY',
                aggfunc='mean',
                fill_value=0
            )
            
            fig_heatmap = px.imshow(
                heatmap_data.values,
                x=heatmap_data.columns,
                y=heatmap_data.index,
                color_continuous_scale='RdYlGn',
                title="Success Rate Heatmap: Region vs Job Function",
                labels={'color': 'Success Rate (%)'}
            )
            
            fig_heatmap.update_layout(height=400)
            st.plotly_chart(fig_heatmap, use_container_width=True)
            
            # Regional insights
            st.markdown("### 💡 Regional Insights")
            
            best_region = regional_summary.loc[regional_summary['SUCCESS_RATE'].idxmax()]
            worst_region = regional_summary.loc[regional_summary['SUCCESS_RATE'].idxmin()]
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown(f"""
                <div class="success-box">
                    <strong>🏆 Best Performing Region:</strong><br>
                    {best_region['GEOGRAPHIC_REGION']}<br>
                    Success Rate: {best_region['SUCCESS_RATE']:.1f}%<br>
                    Avg Time to Hire: {best_region['AVG_WORKING_DAYS_TO_HIRE']:.0f} days
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                st.markdown(f"""
                <div class="warning-box">
                    <strong>⚠️ Needs Improvement:</strong><br>
                    {worst_region['GEOGRAPHIC_REGION']}<br>
                    Success Rate: {worst_region['SUCCESS_RATE']:.1f}%<br>
                    Avg Time to Hire: {worst_region['AVG_WORKING_DAYS_TO_HIRE']:.0f} days
                </div>
                """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()