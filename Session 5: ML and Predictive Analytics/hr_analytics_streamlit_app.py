# HR Analytics Dashboard - Streamlit App
# All visualizations extracted from the main ML notebook

import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.functions as F

# Configure page
st.set_page_config(
    page_title="HR Analytics Dashboard",
    page_icon="👥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize Snowflake session
@st.cache_resource
def init_snowflake_session():
    return get_active_session()

session = init_snowflake_session()

# Load data
@st.cache_data
def load_data():
    """Load and cache the HR data"""
    raw_data = session.table("HR_EMPLOYEE_ATTRITION")
    
    # Basic cleaning
    problematic_cols = ['EMPLOYEE_COUNT', 'STANDARD_HOURS', 'OVER18', 'PERFORMANCE_RATING']
    cols_to_drop = []
    
    for col_name in problematic_cols:
        if col_name in raw_data.columns:
            unique_count = raw_data.select(col_name).distinct().count()
            if unique_count <= 1:
                cols_to_drop.append(col_name)
    
    if cols_to_drop:
        cleaned_data = raw_data.drop(*cols_to_drop)
    else:
        cleaned_data = raw_data
    
    # Remove income outliers
    income_stats = cleaned_data.select([
        F.expr("percentile_cont(0.25) within group (order by MONTHLY_INCOME)").alias("Q1"),
        F.expr("percentile_cont(0.75) within group (order by MONTHLY_INCOME)").alias("Q3")
    ]).collect()[0]
    
    Q1 = float(income_stats['Q1'])
    Q3 = float(income_stats['Q3'])
    IQR = Q3 - Q1
    lower_bound = max(0, Q1 - 1.5 * IQR)
    upper_bound = Q3 + 1.5 * IQR
    
    cleaned_data = cleaned_data.filter(
        (F.col("MONTHLY_INCOME") >= lower_bound) & (F.col("MONTHLY_INCOME") <= upper_bound)
    )
    
    return cleaned_data.to_pandas()

# Main dashboard
def main():
    st.title("🏢 HR Analytics Dashboard")
    st.markdown("*Comprehensive analysis of employee attrition patterns*")
    st.markdown("---")
    
    # Load data
    with st.spinner("Loading HR data..."):
        df = load_data()
    
    st.success(f"✅ Data loaded: {len(df):,} employee records")
    
    # Sidebar filters
    st.sidebar.header("🔧 Filters")
    
    # Department filter
    departments = sorted(df['DEPARTMENT'].unique())
    selected_departments = st.sidebar.multiselect(
        "Select Departments", 
        departments, 
        default=departments
    )
    
    # Gender filter
    genders = sorted(df['GENDER'].unique())
    selected_genders = st.sidebar.multiselect(
        "Select Genders", 
        genders, 
        default=genders
    )
    
    # Filter data
    filtered_df = df[
        (df['DEPARTMENT'].isin(selected_departments)) &
        (df['GENDER'].isin(selected_genders))
    ]
    
    # Create tabs for different analyses
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Overview", 
        "🏢 Department Analysis", 
        "👥 Demographics", 
        "💰 Income Analysis",
        "😊 Job Satisfaction",
        "🔗 Correlations"
    ])
    
    with tab1:
        overview_analysis(filtered_df)
    
    with tab2:
        department_analysis(filtered_df)
    
    with tab3:
        demographic_analysis(filtered_df)
    
    with tab4:
        income_analysis(filtered_df)
    
    with tab5:
        satisfaction_analysis(filtered_df)
    
    with tab6:
        correlation_analysis(filtered_df)

def overview_analysis(df):
    """Overall statistics and key metrics"""
    st.header("📊 Dataset Overview")
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    total_employees = len(df)
    attrition_count = len(df[df['ATTRITION'] == 'Yes'])
    attrition_rate = (attrition_count / total_employees) * 100
    avg_age = df['AGE'].mean()
    
    col1.metric("Total Employees", f"{total_employees:,}")
    col2.metric("Attrition Count", f"{attrition_count:,}")
    col3.metric("Attrition Rate", f"{attrition_rate:.1f}%")
    col4.metric("Average Age", f"{avg_age:.1f} years")
    
    # Attrition overview
    st.subheader("🎯 Attrition Overview")
    col1, col2 = st.columns(2)
    
    with col1:
        # Pie chart for attrition
        attrition_counts = df['ATTRITION'].value_counts()
        fig = px.pie(
            values=attrition_counts.values,
            names=attrition_counts.index,
            title="Employee Attrition Distribution",
            color_discrete_map={'Yes': '#ff6b6b', 'No': '#4ecdc4'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Key statistics table
        st.write("**Key Statistics:**")
        stats_data = {
            "Metric": ["Total Employees", "Employees Left", "Retention Rate", "Average Age", "Average Income"],
            "Value": [
                f"{total_employees:,}",
                f"{attrition_count:,}",
                f"{100-attrition_rate:.1f}%",
                f"{avg_age:.1f} years",
                f"${df['MONTHLY_INCOME'].mean():,.0f}"
            ]
        }
        st.dataframe(pd.DataFrame(stats_data), use_container_width=True)

def department_analysis(df):
    """Department-wise analysis"""
    st.header("🏢 Department & Job Role Analysis")
    
    # Department attrition analysis
    dept_analysis = df.groupby('DEPARTMENT').agg({
        'ATTRITION': ['count', lambda x: (x == 'Yes').sum()],
        'MONTHLY_INCOME': 'mean',
        'AGE': 'mean'
    }).round(2)
    
    dept_analysis.columns = ['Total_Employees', 'Attritioned', 'Avg_Income', 'Avg_Age']
    dept_analysis['Attrition_Rate'] = (dept_analysis['Attritioned'] / dept_analysis['Total_Employees'] * 100).round(1)
    dept_analysis = dept_analysis.sort_values('Attrition_Rate', ascending=False)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Attrition Rate by Department")
        fig = px.bar(
            dept_analysis.reset_index(),
            x='DEPARTMENT',
            y='Attrition_Rate',
            title="Attrition Rate by Department",
            color='Attrition_Rate',
            color_continuous_scale='reds'
        )
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📋 Department Summary")
        st.dataframe(dept_analysis, use_container_width=True)
    
    # Job role analysis
    st.subheader("👔 Job Role Analysis")
    
    role_analysis = df.groupby('JOB_ROLE').agg({
        'ATTRITION': ['count', lambda x: (x == 'Yes').sum()]
    })
    role_analysis.columns = ['Total_Employees', 'Attritioned']
    role_analysis['Attrition_Rate'] = (role_analysis['Attritioned'] / role_analysis['Total_Employees'] * 100).round(1)
    role_analysis = role_analysis[role_analysis['Total_Employees'] >= 5].sort_values('Attrition_Rate', ascending=False)
    
    fig = px.bar(
        role_analysis.head(10).reset_index(),
        x='Attrition_Rate',
        y='JOB_ROLE',
        orientation='h',
        title="Top 10 Job Roles by Attrition Rate",
        color='Attrition_Rate',
        color_continuous_scale='reds'
    )
    st.plotly_chart(fig, use_container_width=True)

def demographic_analysis(df):
    """Gender and age analysis"""
    st.header("👥 Demographics Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Gender Distribution")
        
        # Gender pie chart
        gender_counts = df['GENDER'].value_counts()
        fig = px.pie(
            values=gender_counts.values,
            names=gender_counts.index,
            title="Employee Distribution by Gender"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Gender attrition rates
        gender_attrition = df.groupby('GENDER')['ATTRITION'].apply(
            lambda x: (x == 'Yes').sum() / len(x) * 100
        ).round(1)
        
        st.write("**Attrition Rate by Gender:**")
        for gender, rate in gender_attrition.items():
            st.write(f"• {gender}: {rate}%")
    
    with col2:
        st.subheader("Age Distribution")
        
        # Age histogram
        fig = px.histogram(
            df,
            x='AGE',
            nbins=20,
            title="Age Distribution of Employees",
            color_discrete_sequence=['skyblue']
        )
        fig.add_vline(x=df['AGE'].mean(), line_dash="dash", line_color="red", 
                     annotation_text=f"Mean: {df['AGE'].mean():.1f}")
        st.plotly_chart(fig, use_container_width=True)
        
        # Age statistics
        st.write("**Age Statistics:**")
        age_stats = {
            "Average": f"{df['AGE'].mean():.1f} years",
            "Median": f"{df['AGE'].median():.1f} years",
            "Range": f"{df['AGE'].min()} - {df['AGE'].max()} years"
        }
        for stat, value in age_stats.items():
            st.write(f"• {stat}: {value}")
    
    # Attrition by age groups
    st.subheader("📊 Attrition by Age Groups")
    
    # Create age groups
    df_temp = df.copy()
    df_temp['Age_Group'] = pd.cut(df_temp['AGE'], 
                                 bins=[0, 30, 40, 50, 100], 
                                 labels=['Under 30', '30-40', '40-50', '50+'])
    
    age_group_attrition = df_temp.groupby('Age_Group')['ATTRITION'].apply(
        lambda x: (x == 'Yes').sum() / len(x) * 100
    ).round(1)
    
    fig = px.bar(
        x=age_group_attrition.index,
        y=age_group_attrition.values,
        title="Attrition Rate by Age Group",
        labels={'x': 'Age Group', 'y': 'Attrition Rate (%)'},
        color=age_group_attrition.values,
        color_continuous_scale='reds'
    )
    st.plotly_chart(fig, use_container_width=True)

def income_analysis(df):
    """Income-related analysis"""
    st.header("💰 Income Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Income Distribution")
        
        # Income histogram by attrition
        fig = px.histogram(
            df,
            x='MONTHLY_INCOME',
            color='ATTRITION',
            nbins=30,
            title="Monthly Income Distribution by Attrition",
            barmode='overlay',
            opacity=0.7
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Income Box Plot")
        
        # Box plot by attrition
        fig = px.box(
            df,
            y='MONTHLY_INCOME',
            x='ATTRITION',
            title="Income Distribution by Attrition Status"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Income bins analysis
    st.subheader("📊 Attrition by Income Level")
    
    # Create income bins
    df_temp = df.copy()
    df_temp['Income_Level'] = pd.qcut(df_temp['MONTHLY_INCOME'], 
                                     q=5, 
                                     labels=['Very Low', 'Low', 'Medium', 'High', 'Very High'])
    
    income_attrition = df_temp.groupby('Income_Level')['ATTRITION'].apply(
        lambda x: (x == 'Yes').sum() / len(x) * 100
    ).round(1)
    
    fig = px.bar(
        x=income_attrition.index,
        y=income_attrition.values,
        title="Attrition Rate by Income Level",
        labels={'x': 'Income Level', 'y': 'Attrition Rate (%)'},
        color=income_attrition.values,
        color_continuous_scale='reds'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Income statistics
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Average Income by Attrition:**")
        income_by_attrition = df.groupby('ATTRITION')['MONTHLY_INCOME'].agg(['mean', 'median', 'std']).round(0)
        st.dataframe(income_by_attrition)
    
    with col2:
        st.write("**Income Quartile Analysis:**")
        quartile_attrition = df_temp.groupby(pd.qcut(df_temp['MONTHLY_INCOME'], q=4))['ATTRITION'].apply(
            lambda x: (x == 'Yes').sum() / len(x) * 100
        ).round(1)
        quartile_df = pd.DataFrame({
            'Quartile': ['Q1', 'Q2', 'Q3', 'Q4'],
            'Attrition_Rate': quartile_attrition.values
        })
        st.dataframe(quartile_df)

def satisfaction_analysis(df):
    """Job satisfaction analysis"""
    st.header("😊 Job Satisfaction Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Satisfaction Distribution")
        
        # Satisfaction distribution
        satisfaction_counts = df['JOB_SATISFACTION'].value_counts().sort_index()
        fig = px.bar(
            x=satisfaction_counts.index,
            y=satisfaction_counts.values,
            title="Employee Distribution by Job Satisfaction Level",
            labels={'x': 'Satisfaction Level', 'y': 'Number of Employees'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Attrition by Satisfaction")
        
        # Attrition rate by satisfaction
        satisfaction_attrition = df.groupby('JOB_SATISFACTION')['ATTRITION'].apply(
            lambda x: (x == 'Yes').sum() / len(x) * 100
        ).round(1)
        
        fig = px.bar(
            x=satisfaction_attrition.index,
            y=satisfaction_attrition.values,
            title="Attrition Rate by Job Satisfaction Level",
            labels={'x': 'Satisfaction Level', 'y': 'Attrition Rate (%)'},
            color=satisfaction_attrition.values,
            color_continuous_scale='reds'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Satisfaction level interpretation
    st.subheader("📖 Satisfaction Scale Interpretation")
    interpretation = {
        "Level": [1, 2, 3, 4],
        "Description": ["Low Satisfaction", "Medium Satisfaction", "High Satisfaction", "Very High Satisfaction"],
        "Attrition Rate": [f"{satisfaction_attrition.get(i, 0):.1f}%" for i in range(1, 5)]
    }
    st.dataframe(pd.DataFrame(interpretation))
    
    # Correlation with attrition
    correlation = df['JOB_SATISFACTION'].corr(df['ATTRITION'].map({'Yes': 1, 'No': 0}))
    st.info(f"📊 **Correlation between Job Satisfaction and Attrition**: {correlation:.3f}")

def correlation_analysis(df):
    """Correlation and relationships analysis"""
    st.header("🔗 Feature Correlations")
    
    # Get numerical columns
    numerical_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if 'EMPLOYEE_NUMBER' in numerical_cols:
        numerical_cols.remove('EMPLOYEE_NUMBER')
    
    # Add encoded attrition for correlation
    df_corr = df[numerical_cols].copy()
    df_corr['ATTRITION_NUMERIC'] = df['ATTRITION'].map({'Yes': 1, 'No': 0})
    
    # Correlation matrix
    correlation_matrix = df_corr.corr()
    
    # Correlation heatmap
    fig = px.imshow(
        correlation_matrix,
        text_auto=True,
        aspect="auto",
        title="Feature Correlation Heatmap",
        color_continuous_scale='RdBu_r'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Correlations with attrition
    st.subheader("🎯 Correlations with Attrition")
    
    attrition_corrs = correlation_matrix['ATTRITION_NUMERIC'].drop('ATTRITION_NUMERIC').sort_values(key=abs, ascending=False)
    
    # Top correlations
    fig = px.bar(
        x=attrition_corrs.values,
        y=attrition_corrs.index,
        orientation='h',
        title="Feature Correlations with Attrition",
        color=attrition_corrs.values,
        color_continuous_scale='RdBu_r'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Correlation table
    corr_df = pd.DataFrame({
        'Feature': attrition_corrs.index,
        'Correlation': attrition_corrs.values.round(3)
    })
    st.dataframe(corr_df, use_container_width=True)

if __name__ == "__main__":
    main() 