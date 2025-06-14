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

# Add ML Registry imports for model inference
from snowflake.ml.registry import Registry
import ast

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

# Load data - updated to handle the Snowpark DataFrame properly
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

# Helper functions for model inference
def get_available_models():
    """Get list of available models from the registry"""
    try:
        reg = Registry(
            session=session,
            database_name=session.get_current_database(),
            schema_name='ML_MODELING',
        )
        models = reg.show_models()
        if not models.empty:
            return models['name'].tolist()
        else:
            return []
    except Exception as e:
        st.error(f"Error accessing model registry: {e}")
        return []

def get_available_tables():
    """Get list of available tables for inference"""
    try:
        # Get tables from current database and ML_MODELING schema
        tables_df = session.sql("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'ML_MODELING' 
            AND table_name LIKE '%EMPLOYEE_ATTRITION%'
            ORDER BY table_name
        """).collect()
        return [row['TABLE_NAME'] for row in tables_df]
    except Exception as e:
        st.error(f"Error getting available tables: {e}")
        return []

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
        "🤖 Model Inference"
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
        model_inference_section()

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
    """Gender and age analysis - Fixed with working matplotlib implementation"""
    st.header("👥 Demographics Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Gender Distribution")
        
        # Gender pie chart - Keep plotly as it works
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
        
        # Fixed Age histogram using matplotlib
        fig2, ax2 = plt.subplots(figsize=(8, 6))
        ax2.hist(df['AGE'], bins=20, alpha=0.7, color='skyblue', edgecolor='black')
        ax2.axvline(df['AGE'].mean(), color='red', linestyle='--', linewidth=2, 
                   label=f'Mean: {df["AGE"].mean():.1f}')
        ax2.axvline(df['AGE'].median(), color='green', linestyle='--', linewidth=2, 
                   label=f'Median: {df["AGE"].median():.0f}')
        ax2.set_xlabel('Age')
        ax2.set_ylabel('Frequency')
        ax2.set_title('Age Distribution of Employees')
        ax2.legend()
        ax2.grid(axis='y', alpha=0.3)
        st.pyplot(fig2)
        plt.close()
        
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
    """Income-related analysis - Fixed with working matplotlib implementation"""
    st.header("💰 Income Analysis")
    
    # Fixed Income analysis using matplotlib (from notebook)
    st.subheader("💰 Monthly Income Distribution by Attrition")
    
    # Create income analysis visualizations using matplotlib  
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))  
    
    # Income distribution by attrition status  
    ax = axes[0, 0]  
    stayed = df[df['ATTRITION'] == 'No']['MONTHLY_INCOME']  
    left = df[df['ATTRITION'] == 'Yes']['MONTHLY_INCOME']  
    ax.hist(stayed, bins=30, alpha=0.7, label='Stayed', color='lightblue', density=True)  
    ax.hist(left, bins=30, alpha=0.7, label='Left', color='salmon', density=True)  
    ax.set_title('Monthly Income Distribution by Attrition')  
    ax.set_xlabel('Monthly Income ($)')  
    ax.set_ylabel('Density')  
    ax.legend()  
    ax.grid(axis='y', alpha=0.3)  
    
    # Box plot comparison  
    ax = axes[0, 1]  
    df.boxplot(column='MONTHLY_INCOME', by='ATTRITION', ax=ax)  
    ax.set_title('Monthly Income Box Plot by Attrition Status')  
    ax.set_xlabel('Attrition Status')  
    ax.set_ylabel('Monthly Income ($)')  
    
    # Income bins analysis  
    df_temp = df.copy()
    df_temp['INCOME_BIN'] = pd.cut(df_temp['MONTHLY_INCOME'],   
                                   bins=5, labels=['Very Low', 'Low', 'Medium', 'High', 'Very High'])  
    
    # Attrition rate by income bin  
    ax = axes[1, 0]  
    attrition_by_income = df_temp.groupby('INCOME_BIN')['ATTRITION'].apply(  
        lambda x: (x == 'Yes').sum() / len(x) * 100  
    ).reset_index()  
    attrition_by_income.columns = ['INCOME_BIN', 'ATTRITION_RATE']  
    bars = ax.bar(attrition_by_income['INCOME_BIN'], attrition_by_income['ATTRITION_RATE'],   
                  color='coral', alpha=0.7)  
    ax.set_title('Attrition Rate by Income Level')  
    ax.set_xlabel('Income Level')  
    ax.set_ylabel('Attrition Rate (%)')  
    ax.grid(axis='y', alpha=0.3)  
    
    # Add value labels on bars  
    for bar in bars:  
        height = bar.get_height()  
        ax.text(bar.get_x() + bar.get_width()/2., height + 0.5,  
                f'{height:.1f}%', ha='center', va='bottom')  
                
    # Count by income bin and attrition  
    ax = axes[1, 1]  
    income_counts = df_temp.groupby(['INCOME_BIN', 'ATTRITION']).size().unstack()  
    income_counts.plot(kind='bar', ax=ax, color=['lightblue', 'salmon'], alpha=0.7)  
    ax.set_title('Employee Count by Income Level and Attrition')  
    ax.set_xlabel('Income Level')  
    ax.set_ylabel('Employee Count')  
    ax.legend(title='Attrition')  
    ax.grid(axis='y', alpha=0.3)  
    plt.setp(ax.get_xticklabels(), rotation=45)  
    
    plt.tight_layout()  
    st.pyplot(fig)  
    plt.close()
    
    # Statistical summary  
    st.subheader("📈 Income-Attrition Statistical Summary")  
    col1, col2 = st.columns(2)  
    
    with col1:  
        st.write("**Average Monthly Income by Attrition Status:**")  
        avg_income = df.groupby('ATTRITION')['MONTHLY_INCOME'].agg(['mean', 'median', 'std'])  
        st.dataframe(avg_income.round(2))  
        
    with col2:  
        st.write("**Attrition Rate by Income Quartile:**")  
        df_temp['INCOME_QUARTILE'] = pd.qcut(df_temp['MONTHLY_INCOME'],   
                                           q=4, labels=['Q1', 'Q2', 'Q3', 'Q4'])  
        quartile_attrition = df_temp.groupby('INCOME_QUARTILE')['ATTRITION'].apply(  
            lambda x: (x == 'Yes').sum() / len(x) * 100  
        ).round(2)  
        st.dataframe(quartile_attrition.to_frame('Attrition Rate (%)'))

def satisfaction_analysis(df):
    """Job satisfaction analysis - Fixed with working matplotlib implementation"""
    st.header("😊 Job Satisfaction Analysis")
    
    # Check if JOB_SATISFACTION column exists
    if 'JOB_SATISFACTION' not in df.columns:
        st.error("JOB_SATISFACTION column not found in the dataset")
        return
    
    # Group by job satisfaction and calculate metrics
    satisfaction_analysis = df.groupby('JOB_SATISFACTION').agg({
        'ATTRITION': ['count', lambda x: (x == 'Yes').sum()]
    })
    satisfaction_analysis.columns = ['total_employees', 'attritioned']
    satisfaction_analysis['attrition_rate_pct'] = (satisfaction_analysis['attritioned'] / satisfaction_analysis['total_employees'] * 100).round(1)
    satisfaction_analysis = satisfaction_analysis.reset_index()
    
    # Create side-by-side analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Satisfaction Levels Distribution")
        
        # Satisfaction level distribution using matplotlib
        satisfaction_levels = satisfaction_analysis['JOB_SATISFACTION'].tolist()
        satisfaction_counts = satisfaction_analysis['total_employees'].tolist()
        
        fig1, ax1 = plt.subplots(figsize=(8, 6))
        bars = ax1.bar(satisfaction_levels, satisfaction_counts, color='lightblue', alpha=0.8)
        ax1.set_xlabel('Job Satisfaction Level')
        ax1.set_ylabel('Number of Employees')
        ax1.set_title('Employee Distribution by Job Satisfaction Level')
        ax1.grid(axis='y', alpha=0.3)
        
        # Add count labels on bars
        for bar, count in zip(bars, satisfaction_counts):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                    f'{count}', ha='center', va='bottom', fontweight='bold')
        
        st.pyplot(fig1)
        plt.close()
    
    with col2:
        st.subheader("⚠️ Attrition Rate by Satisfaction Level")
        
        # Attrition rate by satisfaction level using matplotlib
        attrition_rates = satisfaction_analysis['attrition_rate_pct'].tolist()
        
        fig2, ax2 = plt.subplots(figsize=(8, 6))
        bars = ax2.bar(satisfaction_levels, attrition_rates, color='salmon', alpha=0.8)
        ax2.set_xlabel('Job Satisfaction Level')
        ax2.set_ylabel('Attrition Rate (%)')
        ax2.set_title('Attrition Rate by Job Satisfaction Level')
        ax2.grid(axis='y', alpha=0.3)
        
        # Add percentage labels on bars
        for bar, rate in zip(bars, attrition_rates):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                    f'{rate:.1f}%', ha='center', va='bottom', fontweight='bold')
        
        st.pyplot(fig2)
        plt.close()
    
    # Detailed satisfaction analysis table
    st.subheader("📋 Detailed Job Satisfaction Analysis")
    
    # Create a more readable table
    satisfaction_display = []
    for _, row in satisfaction_analysis.iterrows():
        satisfaction_display.append({
            'Satisfaction Level': f"Level {row['JOB_SATISFACTION']}",
            'Total Employees': row['total_employees'],
            'Employees Who Left': row['attritioned'],
            'Attrition Rate (%)': f"{row['attrition_rate_pct']:.1f}%"
        })
    satisfaction_df = pd.DataFrame(satisfaction_display)
    st.dataframe(satisfaction_df, use_container_width=True)
    
    # Satisfaction level interpretation
    st.subheader("📖 Satisfaction Scale Interpretation")
    interpretation = {
        "Level": [1, 2, 3, 4],
        "Description": ["Low Satisfaction", "Medium Satisfaction", "High Satisfaction", "Very High Satisfaction"],
        "Attrition Rate": [f"{satisfaction_analysis[satisfaction_analysis['JOB_SATISFACTION'] == i]['attrition_rate_pct'].values[0] if len(satisfaction_analysis[satisfaction_analysis['JOB_SATISFACTION'] == i]) > 0 else 0:.1f}%" for i in range(1, 5)]
    }
    st.dataframe(pd.DataFrame(interpretation))
    
    # Correlation with attrition
    correlation = df['JOB_SATISFACTION'].corr(df['ATTRITION'].map({'Yes': 1, 'No': 0}))
    st.info(f"📊 **Correlation between Job Satisfaction and Attrition**: {correlation:.3f}")

def model_inference_section():
    """New section for model inference"""
    st.header("🤖 Model Inference")
    st.markdown("*Run predictions using trained ML models on selected datasets*")
    st.markdown("---")
    
    # Get available models and tables
    available_models = get_available_models()
    available_tables = get_available_tables()
    
    if not available_models:
        st.warning("No models found in the ML_MODELING schema. Please train a model first.")
        return
    
    if not available_tables:
        st.warning("No suitable tables found for inference in the ML_MODELING schema.")
        return
    
    # Model selection
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🎯 Model Selection")
        selected_model = st.selectbox(
            "Select Model:",
            available_models,
            index=0,  # Default to first model
            help="Choose the model to use for inference"
        )
    
    with col2:
        st.subheader("📊 Dataset Selection")
        selected_table = st.selectbox(
            "Select Dataset:",
            available_tables,
            help="Choose the dataset to run inference on"
        )
    
    # Show the Python code that will be executed
    st.subheader("📝 Code Transparency")
    st.markdown("*The following Python code will be executed when you run inference:*")
    
    code_snippet = f"""
# Initialize Snowflake ML Registry
from snowflake.ml.registry import Registry

reg = Registry(
    session=session,
    database_name='{session.get_current_database()}',
    schema_name='ML_MODELING'
)

# Get the latest version of the selected model
prod_model = reg.get_model('{selected_model}').last()

# Load the selected dataset
inference_data = session.table('HR_ANALYTICS.ML_MODELING.{selected_table}')

# Run inference to get churn probabilities
results = prod_model.run(inference_data, function_name='PREDICT_PROBA')

# Convert to pandas for analysis
results_df = results.to_pandas()
"""
    
    st.code(code_snippet, language='python')
    
    if st.button("🚀 Run Inference", type="primary"):
        try:
            with st.spinner("Running model inference..."):
                # Initialize registry
                reg = Registry(
                    session=session,
                    database_name=session.get_current_database(),
                    schema_name='ML_MODELING',
                )
                
                # Get the latest version of the selected model
                prod_model = reg.get_model(selected_model).last()
                
                # Load the selected dataset
                inference_data = session.table(f"HR_ANALYTICS.ML_MODELING.{selected_table}")
                
                # Run inference
                results = prod_model.run(inference_data, function_name='PREDICT_PROBA')
                
                # Convert to pandas for display
                results_df = results.to_pandas()
                
                st.success("✅ Inference completed successfully!")
                
                # Display results summary
                st.subheader("📋 Inference Results Summary")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Records", len(results_df))
                with col2:
                    if 'PREDICT_PROBA_1' in results_df.columns:
                        avg_churn_prob = results_df['PREDICT_PROBA_1'].mean()
                        st.metric("Avg Churn Probability", f"{avg_churn_prob:.3f}")
                with col3:
                    if 'PREDICT_PROBA_1' in results_df.columns:
                        high_risk_count = len(results_df[results_df['PREDICT_PROBA_1'] > 0.5])
                        st.metric("High Risk Employees", high_risk_count)
                
                # Top 20 High-Risk Employees Section
                st.subheader("⚠️ Top 20 Employees Most Likely to Churn")
                st.markdown("*Employees with the highest predicted churn probability*")
                
                if 'PREDICT_PROBA_1' in results_df.columns:
                    # Get top 20 employees by churn probability
                    top_20_risk = results_df.nlargest(20, 'PREDICT_PROBA_1')
                    
                    # Select and format relevant columns for display
                    display_columns_top20 = []
                    if 'EMPLOYEE_NUMBER' in top_20_risk.columns:
                        display_columns_top20.append('EMPLOYEE_NUMBER')
                    if 'PREDICT_PROBA_1' in top_20_risk.columns:
                        display_columns_top20.append('PREDICT_PROBA_1')
                    if 'PREDICT_PROBA_0' in top_20_risk.columns:
                        display_columns_top20.append('PREDICT_PROBA_0')
                    
                    if display_columns_top20:
                        top_20_display = top_20_risk[display_columns_top20].copy()
                        top_20_display['PREDICT_PROBA_1'] = top_20_display['PREDICT_PROBA_1'].round(4)
                        top_20_display['PREDICT_PROBA_0'] = top_20_display['PREDICT_PROBA_0'].round(4)
                        
                        # Add rank column
                        top_20_display.insert(0, 'Risk Rank', range(1, len(top_20_display) + 1))
                        
                        # Rename columns for better readability
                        column_rename = {
                            'PREDICT_PROBA_1': 'Churn Probability',
                            'PREDICT_PROBA_0': 'Retention Probability',
                            'EMPLOYEE_NUMBER': 'Employee ID'
                        }
                        top_20_display = top_20_display.rename(columns=column_rename)
                        
                        # Style the dataframe with color coding
                        def highlight_risk(val):
                            if isinstance(val, float):
                                if val > 0.7:
                                    return 'background-color: #ffcccc'  # Light red for high risk
                                elif val > 0.5:
                                    return 'background-color: #fff2cc'  # Light yellow for medium risk
                            return ''
                        
                        styled_df = top_20_display.style.applymap(highlight_risk, subset=['Churn Probability'])
                        st.dataframe(styled_df, use_container_width=True)
                        
                        # Summary insights for top 20
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            very_high_risk = len(top_20_display[top_20_display['Churn Probability'] > 0.7])
                            st.metric("Very High Risk (>70%)", very_high_risk, help="Employees with >70% churn probability")
                        with col2:
                            high_risk = len(top_20_display[(top_20_display['Churn Probability'] > 0.5) & (top_20_display['Churn Probability'] <= 0.7)])
                            st.metric("High Risk (50-70%)", high_risk, help="Employees with 50-70% churn probability")
                        with col3:
                            avg_top_20_risk = top_20_display['Churn Probability'].mean()
                            st.metric("Avg Risk (Top 20)", f"{avg_top_20_risk:.3f}", help="Average churn probability of top 20")
                else:
                    st.warning("Churn probability column not found in results")
                
                # Display detailed results
                st.subheader("🔍 All Prediction Results")
                
                # Select relevant columns for display
                display_columns = []
                if 'EMPLOYEE_NUMBER' in results_df.columns:
                    display_columns.append('EMPLOYEE_NUMBER')
                if 'PREDICT_PROBA_0' in results_df.columns:
                    display_columns.append('PREDICT_PROBA_0')
                if 'PREDICT_PROBA_1' in results_df.columns:
                    display_columns.append('PREDICT_PROBA_1')
                
                if display_columns:
                    # Sort by churn probability (descending)
                    if 'PREDICT_PROBA_1' in results_df.columns:
                        results_display = results_df[display_columns].sort_values('PREDICT_PROBA_1', ascending=False)
                        results_display['PREDICT_PROBA_0'] = results_display['PREDICT_PROBA_0'].round(4)
                        results_display['PREDICT_PROBA_1'] = results_display['PREDICT_PROBA_1'].round(4)
                    else:
                        results_display = results_df[display_columns]
                    
                    st.dataframe(results_display, use_container_width=True)
                    
                    # Download button for results
                    csv = results_display.to_csv(index=False)
                    st.download_button(
                        label="📥 Download All Results as CSV",
                        data=csv,
                        file_name=f"inference_results_{selected_model}_{selected_table}.csv",
                        mime="text/csv"
                    )
                    
                    # Download button for top 20 high-risk employees
                    if 'PREDICT_PROBA_1' in results_df.columns:
                        top_20_csv = top_20_display.to_csv(index=False)
                        st.download_button(
                            label="📥 Download Top 20 High-Risk Employees as CSV",
                            data=top_20_csv,
                            file_name=f"top_20_high_risk_{selected_model}_{selected_table}.csv",
                            mime="text/csv"
                        )
                else:
                    st.warning("Expected prediction columns not found in results")
                    st.write("Available columns:", results_df.columns.tolist())
                    st.dataframe(results_df.head(), use_container_width=True)
                
        except Exception as e:
            st.error(f"Error running inference: {e}")
            st.write("Please check that:")
            st.write("- The selected model exists and is properly trained")
            st.write("- The selected dataset has the correct schema")
            st.write("- You have the necessary permissions")

if __name__ == "__main__":
    main() 