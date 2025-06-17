/***************************************************************************************************
| A | M | S |   | L | A | B | S |   | C | U | S | T | O | M | E | R |   | D | E | M | O |

Demo:         AMS Labs ML and Predictive Analytics Setup
Create Date:  2025-06-15
Purpose:      Setup environment for HR employee attrition analysis and machine learning workflows
Data Source:  HR_ANALYTICS.ML_MODELING.HR_EMPLOYEE_ATTRITION
Customer:     Human Resources Analytics and Predictive Modeling
****************************************************************************************************

/*----------------------------------------------------------------------------------
This script sets up the environment for HR analytics and machine learning workflows.
It configures Git integration for accessing datasets, creates the necessary database
schema, loads HR employee attrition data, and prepares Snowflake notebooks and
Streamlit applications for ML model development and deployment.

Key Concepts:
  • Git Integration: Native Git repository connectivity for data and code access
  • File Formats: CSV parsing configuration for HR datasets
  • Data Loading: Bulk data ingestion from external repositories
  • Snowflake Notebooks: Integrated development environment for ML workflows
  • Streamlit Applications: Interactive dashboards for ML model insights
----------------------------------------------------------------------------------*/

-- HR Analytics - Load Dataset using Snowflake Git Integration
-- Enhanced approach leveraging Snowflake's native Git capabilities
USE ROLE accountadmin;

-- Create database and schema
CREATE DATABASE IF NOT EXISTS HR_ANALYTICS;
USE DATABASE HR_ANALYTICS;
CREATE SCHEMA IF NOT EXISTS ML_MODELING;
USE SCHEMA ML_MODELING;

  -- Create Notebooks
CREATE NOTEBOOK AMS_ML_ATTRITION_ANALYTICS
 FROM '@ams_labs.data_engineering.ams_labs_repo/branches/main/Session 5: ML and Predictive Analytics'
 MAIN_FILE = 'AMS_ML_ATTRITION_ANALYTICS.ipynb'
 QUERY_WAREHOUSE = notebook_wh;

-- Create file format for CSV
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  skip_header = 1
  FIELD_DELIMITER = ','
  RECORD_DELIMITER = '\n'
  NULL_IF = ('NULL', 'null', '')
  EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE TABLE HR_EMPLOYEE_ATTRITION AS
SELECT 
    $1::NUMBER as AGE,
    $2::VARCHAR(10) as ATTRITION,
    $3::VARCHAR(50) as BUSINESS_TRAVEL,
    $4::NUMBER as DAILY_RATE,
    $5::VARCHAR(100) as DEPARTMENT,
    $6::NUMBER as DISTANCE_FROM_HOME,
    $7::NUMBER as EDUCATION,
    $8::VARCHAR(100) as EDUCATION_FIELD,
    $9::NUMBER as EMPLOYEE_COUNT,
    $10::NUMBER as EMPLOYEE_NUMBER,
    $11::NUMBER as ENVIRONMENT_SATISFACTION,
    $12::VARCHAR(10) as GENDER,
    $13::NUMBER as HOURLY_RATE,
    $14::NUMBER as JOB_INVOLVEMENT,
    $15::NUMBER as JOB_LEVEL,
    $16::VARCHAR(100) as JOB_ROLE,
    $17::NUMBER as JOB_SATISFACTION,
    $18::VARCHAR(20) as MARITAL_STATUS,
    $19::NUMBER as MONTHLY_INCOME,
    $20::NUMBER as MONTHLY_RATE,
    $21::NUMBER as NUM_COMPANIES_WORKED,
    $22::VARCHAR(5) as OVER_18,
    $23::VARCHAR(10) as OVER_TIME,
    $24::NUMBER as PERCENT_SALARY_HIKE,
    $25::NUMBER as PERFORMANCE_RATING,
    $26::NUMBER as RELATIONSHIP_SATISFACTION,
    $27::NUMBER as STANDARD_HOURS,
    $28::NUMBER as STOCK_OPTION_LEVEL,
    $29::NUMBER as TOTAL_WORKING_YEARS,
    $30::NUMBER as TRAINING_TIMES_LAST_YEAR,
    $31::NUMBER as WORK_LIFE_BALANCE,
    $32::NUMBER as YEARS_AT_COMPANY,
    $33::NUMBER as YEARS_IN_CURRENT_ROLE,
    $34::NUMBER as YEARS_SINCE_LAST_PROMOTION,
    $35::NUMBER as YEARS_WITH_CURR_MANAGER
FROM '@ams_labs.data_engineering.AMS_LABS_REPO/branches/main/Session 5: ML and Predictive Analytics/HR-Employee-Attrition.csv'
(file_format => 'CSV_FORMAT');

-- Verify the data load
SELECT 
    COUNT(*) as TOTAL_ROWS,
    COUNT(DISTINCT EMPLOYEE_NUMBER) as UNIQUE_EMPLOYEES
FROM HR_EMPLOYEE_ATTRITION;

-- Basic data quality checks
SELECT 
    'Missing Values Check' as CHECK_TYPE,
    SUM(CASE WHEN AGE IS NULL THEN 1 ELSE 0 END) as NULL_AGE,
    SUM(CASE WHEN ATTRITION IS NULL THEN 1 ELSE 0 END) as NULL_ATTRITION,
    SUM(CASE WHEN DEPARTMENT IS NULL THEN 1 ELSE 0 END) as NULL_DEPARTMENT
FROM HR_EMPLOYEE_ATTRITION;