-- =====================================================
-- Lab1 - Data Transformation Pipeline
-- Bronze → Silver → Gold → Platinum Layers
-- Showcasing: Dynamic Tables, UDFs, Stored Procedures, Tasks
-- CUSTOMER-ALIGNED: Recruitment analytics with working days calculations
-- =====================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE AMS_LABS;
USE SCHEMA DATA_ENGINEERING;

-- =====================================================
-- SECTION 1: CUSTOMER-SPECIFIC USER DEFINED FUNCTIONS (UDFs)
-- =====================================================

-- 1.1: Python UDF - Calculate working days between two dates
CREATE OR REPLACE FUNCTION CALCULATE_WORKING_DAYS(start_date DATE, end_date DATE)
RETURNS NUMBER
LANGUAGE PYTHON
RUNTIME_VERSION = 3.12
HANDLER = 'calculate_working_days'
AS
$$
def calculate_working_days(start_date, end_date):
    if start_date is None or end_date is None:
        return None
    
    try:
        from datetime import timedelta
        
        if end_date < start_date:
            return 0
        
        working_days = 0
        current_date = start_date
        
        while current_date <= end_date:
            if current_date.weekday() < 5:  # Monday to Friday
                working_days += 1
            current_date += timedelta(days=1)
        
        return working_days
    except Exception:
        return None
$$;

-- 1.3: SQL UDF - Generate unique application identifier (Customer Requirement)
-- PURPOSE: Create composite unique key for data integration and deduplication
-- CUSTOMER VALUE: Reliable unique identifier for downstream systems
CREATE OR REPLACE FUNCTION GENERATE_UNIQUE_APPLICATION_ID(job_number STRING, workday_id STRING, candidate_id STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
    CASE 
        WHEN job_number IS NULL OR workday_id IS NULL OR candidate_id IS NULL THEN NULL
        ELSE TRIM(job_number) || '-' || TRIM(workday_id) || '-' || TRIM(candidate_id)
    END
$$;

-- 1.4: SQL UDF - Job function categorization (Customer Business Logic)
CREATE OR REPLACE FUNCTION CATEGORIZE_JOB_FUNCTION(job_title STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
    CASE 
        WHEN UPPER(job_title) LIKE '%DIRECTOR%' OR UPPER(job_title) LIKE '%HEAD OF%' THEN 'Leadership'
        WHEN UPPER(job_title) LIKE '%MANAGER%' OR UPPER(job_title) LIKE '%LEAD%' THEN 'Management'
        WHEN UPPER(job_title) LIKE '%RECRUIT%' OR UPPER(job_title) LIKE '%TALENT%' THEN 'Recruitment'
        WHEN UPPER(job_title) LIKE '%ANALYST%' OR UPPER(job_title) LIKE '%DATA%' THEN 'Analytics'
        WHEN UPPER(job_title) LIKE '%COORDINAT%' OR UPPER(job_title) LIKE '%ADMIN%' THEN 'Operations'
        ELSE 'Other'
    END
$$;

-- 1.5: SQL UDF - Working days bucket categorization
CREATE OR REPLACE FUNCTION CATEGORIZE_WORKING_DAYS_BUCKET(working_days NUMBER)
RETURNS STRING
LANGUAGE SQL
AS
$$
    CASE 
        WHEN working_days IS NULL THEN 'Unknown'
        WHEN working_days <= 5 THEN '1-5 days'
        WHEN working_days <= 30 THEN '6-30 days'
        WHEN working_days <= 90 THEN '31-90 days'
        ELSE '90+ days'
    END
$$;

-- =====================================================
-- SECTION 2: APPLICATION OUTCOME MAPPING (CTAS)
-- =====================================================

-- 2.0: Create Application Outcome Categories Mapping Table
CREATE OR REPLACE TABLE APPLICATION_OUTCOME_MAPPING AS
SELECT DISTINCT
    APPLICATION_STATUS_CLEAN,
    CASE 
        -- Applied: Initial application states
        WHEN APPLICATION_STATUS_CLEAN IN ('NEW', 'OPEN') THEN 'Applied'
        
        -- In Process: Active review states
        WHEN APPLICATION_STATUS_CLEAN IN ('IN PROCESS', 'INVITED', 'COMPLETED') THEN 'In Process'
        
        -- Offered: Offer-related states
        WHEN APPLICATION_STATUS_CLEAN IN ('OFFERED', 'OFFER APPROVED', 'VERBAL OFFER ACCEPTED', 
                                         'OFFER SENT FOR APPROVAL', 'OFFER ACCEPTED') THEN 'Offered'
        
        -- Hired: Successful completion
        WHEN APPLICATION_STATUS_CLEAN IN ('HIRED', 'WORKDAY NEW HIRE INTEGRATION', 
                                         'WORKDAY NEW HIRE INTEGRATION MANUAL') THEN 'Hired'
        
        -- Declined: Unsuccessful outcomes
        WHEN APPLICATION_STATUS_CLEAN IN ('REJECTED', 'OFFER DECLINED', 'INVITATION DECLINED', 
                                         'AUTO-DECLINED', 'OFFER NOT APPROVED') THEN 'Declined'
        
        -- Withdrawn: Withdrawn/cancelled applications
        WHEN APPLICATION_STATUS_CLEAN IN ('WITHDRAWN', 'CANCELLED', 'CLOSED', 'AUTO-CLOSED') THEN 'Withdrawn'
        
        ELSE 'Other'
    END AS APPLICATION_OUTCOME_CATEGORY,
    
    -- Funnel stage ordering for analysis
    CASE 
        WHEN APPLICATION_STATUS_CLEAN IN ('NEW', 'OPEN') THEN 1
        WHEN APPLICATION_STATUS_CLEAN IN ('IN PROCESS', 'INVITED', 'COMPLETED') THEN 2
        WHEN APPLICATION_STATUS_CLEAN IN ('OFFERED', 'OFFER APPROVED', 'VERBAL OFFER ACCEPTED', 
                                         'OFFER SENT FOR APPROVAL', 'OFFER ACCEPTED') THEN 3
        WHEN APPLICATION_STATUS_CLEAN IN ('HIRED', 'WORKDAY NEW HIRE INTEGRATION', 
                                         'WORKDAY NEW HIRE INTEGRATION MANUAL') THEN 4
        WHEN APPLICATION_STATUS_CLEAN IN ('REJECTED', 'OFFER DECLINED', 'INVITATION DECLINED', 
                                         'AUTO-DECLINED', 'OFFER NOT APPROVED') THEN 5
        WHEN APPLICATION_STATUS_CLEAN IN ('WITHDRAWN', 'CANCELLED', 'CLOSED', 'AUTO-CLOSED') THEN 6
        ELSE 7
    END AS FUNNEL_ORDER
FROM (
    SELECT DISTINCT TRIM(UPPER(APPLICATIONSTATUS)) AS APPLICATION_STATUS_CLEAN
    FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    WHERE APPLICATIONSTATUS IS NOT NULL
);

-- =====================================================
-- SECTION 3: SILVER LAYER - DYNAMIC TABLES
-- =====================================================

-- 3.1: Silver Applications - Cleaned and Standardized Data
CREATE OR REPLACE DYNAMIC TABLE SILVER_APPLICATIONS_CLEANED
  TARGET_LAG = '1 minute'
  WAREHOUSE = COMPUTE_WH
  AS
SELECT 
    -- Primary Keys
    TA_APPLICATIONS_ID,
    TA_APPLICATIONS_UK,
    JOBNUMBER,
    WORKDAYID,
    
    -- Job Information (cleaned)
    TRIM(UPPER(JOBTITLE)) AS JOB_TITLE_CLEAN,
    TRIM(UPPER(JOBSTATUS)) AS JOB_STATUS_CLEAN,
    
    -- Candidate Information
    CANDIDATEID,
    TRIM(UPPER(CANDIDATESTATUS)) AS CANDIDATE_STATUS_CLEAN,
    TRIM(UPPER(APPLICATIONSTATUS)) AS APPLICATION_STATUS_CLEAN,
    
    -- Sourcing Information (standardized)
    CASE 
        WHEN APPLICATIONSOURCINGCHANNELTYPE IS NULL THEN 'Unknown'
        ELSE TRIM(UPPER(APPLICATIONSOURCINGCHANNELTYPE))
    END AS SOURCING_CHANNEL_TYPE_CLEAN,
    
    CASE 
        WHEN APPLICATIONSOURCINGCHANNELNAME IS NULL THEN 'Unknown'
        ELSE TRIM(APPLICATIONSOURCINGCHANNELNAME)
    END AS SOURCING_CHANNEL_NAME_CLEAN,
    
    -- Geographic Information (standardized)
    CASE 
        WHEN CANDIDATECOUNTRY IS NULL THEN 'Unknown'
        ELSE TRIM(UPPER(CANDIDATECOUNTRY))
    END AS CANDIDATE_COUNTRY_CLEAN,
    
    CASE 
        WHEN CANDIDATELOCATION IS NULL THEN 'Unknown'
        ELSE TRIM(CANDIDATELOCATION)
    END AS CANDIDATE_LOCATION_CLEAN,
    
    -- Date Fields (converted to proper dates)
    TRY_TO_DATE(STATUSLASTCHANGEDTOOPEN, 'DD/MM/YYYY') AS STATUS_LAST_CHANGED_TO_OPEN_DATE,
    TRY_TO_DATE(STATUSLASTCHANGEDTOINPROCESS, 'DD/MM/YYYY') AS STATUS_LAST_CHANGED_TO_INPROCESS_DATE,
    TRY_TO_DATE(STATUSLASTCHANGEDTOHIRED, 'DD/MM/YYYY') AS STATUS_LAST_CHANGED_TO_HIRED_DATE,
    TRY_TO_DATE(STATUSLASTCHANGEDTOREJECTED, 'DD/MM/YYYY') AS STATUS_LAST_CHANGED_TO_REJECTED_DATE,
    TRY_TO_DATE(HIREDDATE, 'DD/MM/YYYY') AS HIRED_DATE_CLEAN,
    
    -- Original snapshot date
    SNAPSHOT_DATE
FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP;

-- =====================================================
-- SECTION 3.1: AI-POWERED COUNTRY-TO-REGION MAPPING TABLE
-- =====================================================
-- Create Country-to-Region Mapping Table using AI_CLASSIFY
-- PURPOSE: Showcase Snowflake's AI_CLASSIFY function for geographic classification
CREATE OR REPLACE TABLE COUNTRY_REGION_MAPPING AS
WITH unique_countries AS (
    SELECT DISTINCT 
        CANDIDATE_COUNTRY_CLEAN AS COUNTRY
    FROM SILVER_APPLICATIONS_CLEANED 
    WHERE CANDIDATE_COUNTRY_CLEAN IS NOT NULL 
    AND TRIM(CANDIDATE_COUNTRY_CLEAN) != ''
)
SELECT 
    COUNTRY,
    AI_CLASSIFY(
        COUNTRY, 
        ['APAC', 'EMEA', 'Americas', 'Other']
    ):labels[0]::STRING AS GEOGRAPHIC_REGION
FROM unique_countries;
SELECT * FROM COUNTRY_REGION_MAPPING;


-- 3.2: Silver Applications - Enriched with UDF-derived fields
CREATE OR REPLACE DYNAMIC TABLE SILVER_APPLICATIONS_ENRICHED
  TARGET_LAG = '1 minute'
  WAREHOUSE = COMPUTE_WH
  AS
SELECT 
    sac.*,
    
    -- UDF: Job Title Categorization (SQL UDF)
    CATEGORIZE_JOB_FUNCTION(sac.JOB_TITLE_CLEAN) AS JOB_FUNCTION_CATEGORY,
    
    -- UDF: Customer Unique Application ID (SQL UDF)
    GENERATE_UNIQUE_APPLICATION_ID(sac.JOBNUMBER, sac.WORKDAYID, sac.CANDIDATEID) AS UNIQUE_APPLICATION_ID,
    
    -- UDF: Working Days Calculations (Python UDF)
    CALCULATE_WORKING_DAYS(
        sac.STATUS_LAST_CHANGED_TO_OPEN_DATE,
        sac.STATUS_LAST_CHANGED_TO_HIRED_DATE
    ) AS WORKING_DAYS_TO_HIRE,
    
    CALCULATE_WORKING_DAYS(
        sac.STATUS_LAST_CHANGED_TO_OPEN_DATE,
        sac.STATUS_LAST_CHANGED_TO_REJECTED_DATE
    ) AS WORKING_DAYS_TO_REJECTION,
    
    -- UDF: Working Days Categorization (SQL UDF)
    CATEGORIZE_WORKING_DAYS_BUCKET(
        CALCULATE_WORKING_DAYS(
            sac.STATUS_LAST_CHANGED_TO_OPEN_DATE,
            sac.STATUS_LAST_CHANGED_TO_HIRED_DATE
        )
    ) AS HIRE_TIME_BUCKET,
    
    CATEGORIZE_WORKING_DAYS_BUCKET(
        CALCULATE_WORKING_DAYS(
            sac.STATUS_LAST_CHANGED_TO_OPEN_DATE,
            sac.STATUS_LAST_CHANGED_TO_REJECTED_DATE
        )
    ) AS REJECTION_TIME_BUCKET,
    
    -- Join with outcome mapping for refined categories
    aom.APPLICATION_OUTCOME_CATEGORY,
    aom.FUNNEL_ORDER,
    
    -- Time-based calculations
    EXTRACT(YEAR FROM sac.SNAPSHOT_DATE) AS SNAPSHOT_YEAR,
    EXTRACT(QUARTER FROM sac.SNAPSHOT_DATE) AS SNAPSHOT_QUARTER,
    EXTRACT(MONTH FROM sac.SNAPSHOT_DATE) AS SNAPSHOT_MONTH,
    
    -- AI-Powered Geographic region mapping (using AI_CLASSIFY mapping table)
    COALESCE(crm.GEOGRAPHIC_REGION, 'Other') AS GEOGRAPHIC_REGION

FROM SILVER_APPLICATIONS_CLEANED sac
LEFT JOIN APPLICATION_OUTCOME_MAPPING aom 
    ON sac.APPLICATION_STATUS_CLEAN = aom.APPLICATION_STATUS_CLEAN
LEFT JOIN COUNTRY_REGION_MAPPING crm
    ON sac.CANDIDATE_COUNTRY_CLEAN = crm.COUNTRY;

-- =====================================================
-- SECTION 4: GOLD LAYER - DYNAMIC TABLES
-- =====================================================
select top 100 * from SILVER_APPLICATIONS_ENRICHED;

-- 4.1: Gold Customer Hiring Metrics - Customer-Specific KPIs
CREATE OR REPLACE DYNAMIC TABLE GOLD_CUSTOMER_HIRING_METRICS
  TARGET_LAG = '1 minute'
  WAREHOUSE = COMPUTE_WH
  AS
WITH base_metrics AS (
    SELECT 
        -- Time dimensions
        SNAPSHOT_YEAR,
        SNAPSHOT_QUARTER,
        SNAPSHOT_MONTH,
        
        -- Geographic dimensions
        GEOGRAPHIC_REGION,
        CANDIDATE_COUNTRY_CLEAN,
        
        -- Business dimensions
        JOB_FUNCTION_CATEGORY,
        APPLICATION_OUTCOME_CATEGORY,
        SOURCING_CHANNEL_TYPE_CLEAN,
        SOURCING_CHANNEL_NAME_CLEAN,
        
        -- Customer-specific time analysis
        HIRE_TIME_BUCKET,
        REJECTION_TIME_BUCKET,
        
        -- Key Metrics
        COUNT(*) AS TOTAL_APPLICATIONS,
        COUNT(DISTINCT CANDIDATEID) AS UNIQUE_CANDIDATES,
        COUNT(DISTINCT JOBNUMBER) AS UNIQUE_JOBS,
        COUNT(DISTINCT UNIQUE_APPLICATION_ID) AS UNIQUE_APPLICATION_IDS,
        
        -- Customer Required: Working Days Analysis
        AVG(WORKING_DAYS_TO_HIRE) AS AVG_WORKING_DAYS_TO_HIRE,
        MEDIAN(WORKING_DAYS_TO_HIRE) AS MEDIAN_WORKING_DAYS_TO_HIRE,
        MIN(WORKING_DAYS_TO_HIRE) AS MIN_WORKING_DAYS_TO_HIRE,
        MAX(WORKING_DAYS_TO_HIRE) AS MAX_WORKING_DAYS_TO_HIRE,
        
        AVG(WORKING_DAYS_TO_REJECTION) AS AVG_WORKING_DAYS_TO_REJECTION,
        MEDIAN(WORKING_DAYS_TO_REJECTION) AS MEDIAN_WORKING_DAYS_TO_REJECTION,
        MIN(WORKING_DAYS_TO_REJECTION) AS MIN_WORKING_DAYS_TO_REJECTION,
        MAX(WORKING_DAYS_TO_REJECTION) AS MAX_WORKING_DAYS_TO_REJECTION,
        
        -- Funnel Stage Counts
        SUM(CASE WHEN APPLICATION_OUTCOME_CATEGORY = 'Applied' THEN 1 ELSE 0 END) AS APPLIED_COUNT,
        SUM(CASE WHEN APPLICATION_OUTCOME_CATEGORY = 'In Process' THEN 1 ELSE 0 END) AS IN_PROCESS_COUNT,
        SUM(CASE WHEN APPLICATION_OUTCOME_CATEGORY = 'Offered' THEN 1 ELSE 0 END) AS OFFERED_COUNT,
        SUM(CASE WHEN APPLICATION_OUTCOME_CATEGORY = 'Hired' THEN 1 ELSE 0 END) AS HIRED_COUNT,
        SUM(CASE WHEN APPLICATION_OUTCOME_CATEGORY = 'Declined' THEN 1 ELSE 0 END) AS DECLINED_COUNT,
        SUM(CASE WHEN APPLICATION_OUTCOME_CATEGORY = 'Withdrawn' THEN 1 ELSE 0 END) AS WITHDRAWN_COUNT
        
    FROM SILVER_APPLICATIONS_ENRICHED
    GROUP BY 
        SNAPSHOT_YEAR, SNAPSHOT_QUARTER, SNAPSHOT_MONTH,
        GEOGRAPHIC_REGION, CANDIDATE_COUNTRY_CLEAN,
        JOB_FUNCTION_CATEGORY, APPLICATION_OUTCOME_CATEGORY,
        SOURCING_CHANNEL_TYPE_CLEAN, SOURCING_CHANNEL_NAME_CLEAN,
        HIRE_TIME_BUCKET, REJECTION_TIME_BUCKET
),
funnel_totals AS (
    SELECT 
        SNAPSHOT_YEAR,
        SNAPSHOT_QUARTER,
        SNAPSHOT_MONTH,
        GEOGRAPHIC_REGION,
        CANDIDATE_COUNTRY_CLEAN,
        JOB_FUNCTION_CATEGORY,
        SOURCING_CHANNEL_TYPE_CLEAN,
        SOURCING_CHANNEL_NAME_CLEAN,
        HIRE_TIME_BUCKET,
        REJECTION_TIME_BUCKET,
        
        -- Total counts across all stages for conversion calculations
        SUM(APPLIED_COUNT) AS TOTAL_APPLIED,
        SUM(IN_PROCESS_COUNT) AS TOTAL_IN_PROCESS,
        SUM(OFFERED_COUNT) AS TOTAL_OFFERED,
        SUM(HIRED_COUNT) AS TOTAL_HIRED,
        SUM(DECLINED_COUNT) AS TOTAL_DECLINED,
        SUM(WITHDRAWN_COUNT) AS TOTAL_WITHDRAWN,
        SUM(TOTAL_APPLICATIONS) AS TOTAL_ALL_APPLICATIONS
        
    FROM base_metrics
    GROUP BY 
        SNAPSHOT_YEAR, SNAPSHOT_QUARTER, SNAPSHOT_MONTH,
        GEOGRAPHIC_REGION, CANDIDATE_COUNTRY_CLEAN,
        JOB_FUNCTION_CATEGORY, SOURCING_CHANNEL_TYPE_CLEAN, SOURCING_CHANNEL_NAME_CLEAN,
        HIRE_TIME_BUCKET, REJECTION_TIME_BUCKET
)
SELECT 
    bm.*,
    
    -- Funnel Progression Percentages (showing progression through stages)
    CASE WHEN ft.TOTAL_ALL_APPLICATIONS > 0 THEN
        ROUND((ft.TOTAL_IN_PROCESS * 100.0) / ft.TOTAL_ALL_APPLICATIONS, 2)
    ELSE 0 END AS PCT_REACHED_IN_PROCESS,
    
    CASE WHEN ft.TOTAL_ALL_APPLICATIONS > 0 THEN
        ROUND((ft.TOTAL_OFFERED * 100.0) / ft.TOTAL_ALL_APPLICATIONS, 2)
    ELSE 0 END AS PCT_REACHED_OFFERED,
    
    CASE WHEN ft.TOTAL_ALL_APPLICATIONS > 0 THEN
        ROUND((ft.TOTAL_HIRED * 100.0) / ft.TOTAL_ALL_APPLICATIONS, 2)
    ELSE 0 END AS PCT_REACHED_HIRED,
    
    -- Overall Success Rate
    CASE WHEN ft.TOTAL_ALL_APPLICATIONS > 0 THEN
        ROUND((ft.TOTAL_HIRED * 100.0) / ft.TOTAL_ALL_APPLICATIONS, 2)
    ELSE 0 END AS OVERALL_SUCCESS_RATE,
    
    -- Drop-off Analysis
    CASE WHEN ft.TOTAL_ALL_APPLICATIONS > 0 THEN
        ROUND((ft.TOTAL_DECLINED * 100.0) / ft.TOTAL_ALL_APPLICATIONS, 2)
    ELSE 0 END AS PCT_DECLINED,
    
    CASE WHEN ft.TOTAL_ALL_APPLICATIONS > 0 THEN
        ROUND((ft.TOTAL_WITHDRAWN * 100.0) / ft.TOTAL_ALL_APPLICATIONS, 2)
    ELSE 0 END AS PCT_WITHDRAWN

FROM base_metrics bm
LEFT JOIN funnel_totals ft 
    ON bm.SNAPSHOT_YEAR = ft.SNAPSHOT_YEAR
    AND bm.SNAPSHOT_QUARTER = ft.SNAPSHOT_QUARTER
    AND bm.SNAPSHOT_MONTH = ft.SNAPSHOT_MONTH
    AND bm.GEOGRAPHIC_REGION = ft.GEOGRAPHIC_REGION
    AND bm.CANDIDATE_COUNTRY_CLEAN = ft.CANDIDATE_COUNTRY_CLEAN
    AND bm.JOB_FUNCTION_CATEGORY = ft.JOB_FUNCTION_CATEGORY
    AND bm.SOURCING_CHANNEL_TYPE_CLEAN = ft.SOURCING_CHANNEL_TYPE_CLEAN
    AND bm.SOURCING_CHANNEL_NAME_CLEAN = ft.SOURCING_CHANNEL_NAME_CLEAN
    AND bm.HIRE_TIME_BUCKET = ft.HIRE_TIME_BUCKET
    AND bm.REJECTION_TIME_BUCKET = ft.REJECTION_TIME_BUCKET;

-- 4.2: Gold Application Trends - Time Series Analysis
CREATE OR REPLACE DYNAMIC TABLE GOLD_APPLICATION_TRENDS
  TARGET_LAG = '1 minute'
  WAREHOUSE = COMPUTE_WH
  AS
SELECT 
    SNAPSHOT_DATE,
    SNAPSHOT_YEAR,
    SNAPSHOT_MONTH,
    GEOGRAPHIC_REGION,
    
    -- Daily metrics
    COUNT(*) AS DAILY_APPLICATIONS,
    COUNT(DISTINCT CANDIDATEID) AS DAILY_UNIQUE_CANDIDATES,
    
    -- Rolling averages (30-day window)
    AVG(COUNT(*)) OVER (
        PARTITION BY GEOGRAPHIC_REGION 
        ORDER BY SNAPSHOT_DATE 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS ROLLING_30DAY_AVG_APPLICATIONS,
    
    -- Success rates by region
    ROUND(
        (SUM(CASE WHEN APPLICATION_STATUS_CLEAN = 'HIRED' THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 
        2
    ) AS DAILY_SUCCESS_RATE,
    
    -- Top sourcing channel by volume
    MODE(SOURCING_CHANNEL_TYPE_CLEAN) AS TOP_SOURCING_CHANNEL,
    
    -- Application age distribution
    COUNT(CASE WHEN WORKING_DAYS_TO_HIRE <= 5 THEN 1 END) AS FRESH_APPLICATIONS,
    COUNT(CASE WHEN WORKING_DAYS_TO_HIRE > 90 THEN 1 END) AS AGED_APPLICATIONS

FROM SILVER_APPLICATIONS_ENRICHED
GROUP BY SNAPSHOT_DATE, SNAPSHOT_YEAR, SNAPSHOT_MONTH, GEOGRAPHIC_REGION
ORDER BY SNAPSHOT_DATE;



-- =====================================================
-- SECTION 5: PLATINUM LAYER - STORED PROCEDURES & TASKS
-- =====================================================

-- 5.1: Stored Procedure for Executive Dashboard Data
-- 
-- PURPOSE: This stored procedure creates a comprehensive executive summary table 
-- that aggregates key recruitment metrics into a format suitable for C-level reporting.
-- It analyzes data from the current year and identifies:
--   - Total application volume
--   - Overall success/hire rates
--   - Top performing regions by success rate
--   - Most effective sourcing channels
--   - Highest volume job functions
-- 
-- The procedure uses CTEs to calculate rankings and includes error handling
-- to ensure reliable execution in automated environments.
--
CREATE OR REPLACE PROCEDURE GENERATE_EXECUTIVE_DASHBOARD()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    result_message STRING;
    total_applications INTEGER;
    success_rate DECIMAL(5,2);
    top_region STRING;
BEGIN
    
    -- Create or replace the executive summary table
    CREATE OR REPLACE TABLE PLATINUM_EXECUTIVE_DASHBOARD AS
    WITH summary_stats AS (
        SELECT 
            'Global Summary' AS metric_category,
            'Total Applications' AS metric_name,
            SUM(TOTAL_APPLICATIONS)::STRING AS metric_value,
            'Count' AS metric_unit,
            CURRENT_DATE() AS report_date
        FROM GOLD_CUSTOMER_HIRING_METRICS
        WHERE SNAPSHOT_YEAR = EXTRACT(YEAR FROM CURRENT_DATE())
        
        UNION ALL
        
        SELECT 
            'Global Summary' AS metric_category,
            'Overall Success Rate' AS metric_name,
            ROUND(AVG(OVERALL_SUCCESS_RATE), 2)::STRING || '%' AS metric_value,
            'Percentage' AS metric_unit,
            CURRENT_DATE() AS report_date
        FROM GOLD_CUSTOMER_HIRING_METRICS
        WHERE SNAPSHOT_YEAR = EXTRACT(YEAR FROM CURRENT_DATE())
        
        UNION ALL
        
        SELECT 
            'Regional Performance' AS metric_category,
            'Top Performing Region' AS metric_name,
            GEOGRAPHIC_REGION AS metric_value,
            'Region' AS metric_unit,
            CURRENT_DATE() AS report_date
        FROM (
            SELECT 
                GEOGRAPHIC_REGION,
                AVG(OVERALL_SUCCESS_RATE) AS avg_success_rate,
                ROW_NUMBER() OVER (ORDER BY AVG(OVERALL_SUCCESS_RATE) DESC) AS rn
            FROM GOLD_CUSTOMER_HIRING_METRICS
            WHERE SNAPSHOT_YEAR = EXTRACT(YEAR FROM CURRENT_DATE())
            GROUP BY GEOGRAPHIC_REGION
        ) 
        WHERE rn = 1
        
        UNION ALL
        
        SELECT 
            'Sourcing Analysis' AS metric_category,
            'Most Effective Sourcing Channel' AS metric_name,
            SOURCING_CHANNEL_TYPE_CLEAN AS metric_value,
            'Channel' AS metric_unit,
            CURRENT_DATE() AS report_date
        FROM (
            SELECT 
                SOURCING_CHANNEL_TYPE_CLEAN,
                AVG(OVERALL_SUCCESS_RATE) AS avg_success_rate,
                ROW_NUMBER() OVER (ORDER BY AVG(OVERALL_SUCCESS_RATE) DESC) AS rn
            FROM GOLD_CUSTOMER_HIRING_METRICS
            WHERE SNAPSHOT_YEAR = EXTRACT(YEAR FROM CURRENT_DATE())
            GROUP BY SOURCING_CHANNEL_TYPE_CLEAN
        )
        WHERE rn = 1
        
        UNION ALL
        
        SELECT 
            'Job Function Analysis' AS metric_category,
            'Highest Volume Job Function' AS metric_name,
            JOB_FUNCTION_CATEGORY AS metric_value,
            'Function' AS metric_unit,
            CURRENT_DATE() AS report_date
        FROM (
            SELECT 
                JOB_FUNCTION_CATEGORY,
                SUM(TOTAL_APPLICATIONS) AS total_apps,
                ROW_NUMBER() OVER (ORDER BY SUM(TOTAL_APPLICATIONS) DESC) AS rn
            FROM GOLD_CUSTOMER_HIRING_METRICS
            WHERE SNAPSHOT_YEAR = EXTRACT(YEAR FROM CURRENT_DATE())
            GROUP BY JOB_FUNCTION_CATEGORY
        )
        WHERE rn = 1
    )
    SELECT * FROM summary_stats;
    
    -- Get summary statistics for the result message
    SELECT COUNT(*) INTO total_applications 
    FROM TA_APPLICATION_DATA_BRONZE_DEDUP;
    
    SELECT AVG(OVERALL_SUCCESS_RATE) INTO success_rate 
    FROM GOLD_CUSTOMER_HIRING_METRICS 
    WHERE SNAPSHOT_YEAR = EXTRACT(YEAR FROM CURRENT_DATE());
    
    result_message := 'Executive Dashboard Generated Successfully. ' ||
                     'Total Applications Processed: ' || total_applications::STRING ||
                     '. Average Success Rate: ' || COALESCE(success_rate::STRING, 'N/A') || '%.';
    
    RETURN result_message;
    
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error generating executive dashboard: ' || SQLERRM;
END;
$$;

-- 5.2: Create a Task to automate the executive dashboard generation
CREATE OR REPLACE TASK PLATINUM_EXECUTIVE_DASHBOARD_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON * * * * * UTC' -- Every minute
    AS
    CALL GENERATE_EXECUTIVE_DASHBOARD();

-- =====================================================
-- SECTION 5.3: SIMPLE TASK DAG DEMONSTRATION
-- =====================================================

-- 5.3.1: Simple Data Summary Procedure
-- This procedure creates a basic summary of pipeline activity
CREATE OR REPLACE PROCEDURE CREATE_DAILY_SUMMARY()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    
    -- Create a simple daily summary table
    CREATE OR REPLACE TABLE DAILY_PIPELINE_SUMMARY AS
    SELECT 
        CURRENT_DATE() AS summary_date,
        'Pipeline Summary' AS report_type,
        (SELECT COUNT(*) FROM TA_APPLICATION_DATA_BRONZE_DEDUP) AS total_bronze_records,
        (SELECT COUNT(*) FROM SILVER_APPLICATIONS_ENRICHED) AS total_silver_records,
        (SELECT COUNT(*) FROM PLATINUM_EXECUTIVE_DASHBOARD) AS executive_dashboard_metrics,
        CURRENT_TIMESTAMP() AS created_at;
    
    RETURN 'Daily summary created successfully with ' || 
           (SELECT COUNT(*) FROM TA_APPLICATION_DATA_BRONZE_DEDUP)::STRING || ' total records processed.';
    
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error creating daily summary: ' || SQLERRM;
END;
$$;

-- 5.3.2: Task DAG Definition (Two Simple Tasks)
-- 
-- TASK 1: Executive Dashboard (Root - runs on schedule)
-- TASK 2: Daily Summary (Child - runs after executive dashboard completes)
--

-- Task 2: Daily Summary Task (depends on executive dashboard task)
CREATE OR REPLACE TASK DAILY_SUMMARY_TASK
    WAREHOUSE = COMPUTE_WH
    AFTER PLATINUM_EXECUTIVE_DASHBOARD_TASK  -- This creates the dependency!
    AS
    CALL CREATE_DAILY_SUMMARY();

-- =====================================================
-- SECTION 5.4: TASK DAG VISUALIZATION
-- =====================================================

/*
SIMPLE TASK DAG:

    ┌─────────────────────────────────┐
    │ PLATINUM_EXECUTIVE_DASHBOARD_   │  ← Root Task (Scheduled)
    │ TASK                            │    Runs every minute
    │ CRON: * * * * * UTC             │
    └─────────────┬───────────────────┘
                  │ AFTER dependency
                  ▼
    ┌─────────────────────────────────┐
    │ DAILY_SUMMARY_TASK              │  ← Child Task (Triggered)
    │                                 │    Runs automatically after parent
    │ Creates pipeline summary        │    completes successfully
    └─────────────────────────────────┘

HOW IT WORKS:
1. Executive Dashboard Task runs every minute
2. When it completes successfully, Daily Summary Task automatically starts
3. If Executive Dashboard Task fails, Daily Summary Task will NOT run
4. This demonstrates basic task dependency and sequential execution

KEY CONCEPTS DEMONSTRATED:
✅ Scheduled Tasks (CRON schedule)
✅ Dependent Tasks (AFTER clause)  
✅ Sequential Workflow
✅ Automatic Triggering
✅ Error Handling (child won't run if parent fails)
*/

-- View the task dependency relationship
SHOW TASKS LIKE '%DASHBOARD%' IN SCHEMA DATA_ENGINEERING;
SHOW TASKS LIKE '%SUMMARY%' IN SCHEMA DATA_ENGINEERING;

-- Start the task DAG (commented out for safety - uncomment to activate)
-- Note: Only the root task needs to be resumed; child tasks start automatically
ALTER TASK PLATINUM_EXECUTIVE_DASHBOARD_TASK RESUME;
EXECUTE TASK PLATINUM_EXECUTIVE_DASHBOARD_TASK;

-- Test the procedures individually
-- CALL GENERATE_EXECUTIVE_DASHBOARD();
-- CALL CREATE_DAILY_SUMMARY();

-- Check results
-- SELECT * FROM PLATINUM_EXECUTIVE_DASHBOARD;
-- SELECT * FROM DAILY_PIPELINE_SUMMARY;

-- =====================================================
-- SECTION 6: VIEWS FOR EASY ACCESS
-- =====================================================

-- 6.1: Summary view for analysts
-- 
-- PURPOSE: This view provides business analysts with a high-level overview of recruitment 
-- performance across different dimensions. It aggregates data by region, job function, 
-- and application outcome to show patterns and trends in the recruitment process.
--
CREATE OR REPLACE VIEW ANALYST_APPLICATION_SUMMARY AS
SELECT 
    GEOGRAPHIC_REGION,
    JOB_FUNCTION_CATEGORY,
    APPLICATION_OUTCOME_CATEGORY,
    SNAPSHOT_YEAR,
    COUNT(*) AS application_count,
    ROUND(AVG(OVERALL_SUCCESS_RATE), 2) AS avg_success_rate,
    LISTAGG(DISTINCT SOURCING_CHANNEL_TYPE_CLEAN, ', ') AS sourcing_channels_used
FROM GOLD_CUSTOMER_HIRING_METRICS
GROUP BY GEOGRAPHIC_REGION, JOB_FUNCTION_CATEGORY, APPLICATION_OUTCOME_CATEGORY, SNAPSHOT_YEAR
ORDER BY SNAPSHOT_YEAR DESC, application_count DESC;

-- Demonstrate what this view shows: Cross-dimensional analysis of recruitment performance
-- This helps analysts understand which combinations of region, job function, and outcomes
-- are most/least successful, and what sourcing channels are being used.
SELECT 
    GEOGRAPHIC_REGION,
    JOB_FUNCTION_CATEGORY,
    APPLICATION_OUTCOME_CATEGORY,
    application_count,
    avg_success_rate,
    sourcing_channels_used
FROM ANALYST_APPLICATION_SUMMARY 
WHERE SNAPSHOT_YEAR = EXTRACT(YEAR FROM CURRENT_DATE())
  AND application_count >= 10  -- Focus on meaningful sample sizes
ORDER BY avg_success_rate DESC
LIMIT 10;

-- 6.2: Real-time monitoring view
-- 
-- PURPOSE: This view provides data engineers and operations teams with a real-time
-- snapshot of the entire data pipeline health. It shows row counts across all layers
-- and table types, helping identify data flow issues or pipeline failures.
--
CREATE OR REPLACE VIEW REALTIME_PIPELINE_STATUS AS
SELECT 
    'Bronze Layer' AS layer_name,
    'TA_APPLICATION_DATA_BRONZE_DEDUP' AS table_name,
    COUNT(*) AS record_count,
    MAX(SNAPSHOT_DATE) AS latest_data_date,
    'Static Table' AS table_type
FROM TA_APPLICATION_DATA_BRONZE_DEDUP

UNION ALL

SELECT 
    'Silver Layer' AS layer_name,
    'SILVER_APPLICATIONS_ENRICHED' AS table_name,
    COUNT(*) AS record_count,
    MAX(SNAPSHOT_DATE) AS latest_data_date,
    'Dynamic Table' AS table_type
FROM SILVER_APPLICATIONS_ENRICHED

UNION ALL

SELECT 
    'Gold Layer' AS layer_name,
    'GOLD_CUSTOMER_HIRING_METRICS' AS table_name,
    SUM(TOTAL_APPLICATIONS) AS record_count,
    CURRENT_DATE() AS latest_data_date,
    'Dynamic Table' AS table_type
FROM GOLD_CUSTOMER_HIRING_METRICS

UNION ALL

SELECT 
    'Platinum Layer' AS layer_name,
    'PLATINUM_EXECUTIVE_DASHBOARD' AS table_name,
    COUNT(*) AS record_count,
    MAX(REPORT_DATE) AS latest_data_date,
    'Stored Procedure Output' AS table_type
FROM PLATINUM_EXECUTIVE_DASHBOARD;

-- Demonstrate what this view shows: Pipeline health monitoring at a glance
-- This helps operations teams quickly identify if any layer has data flow issues,
-- unexpected row count drops, or refresh problems.
SELECT 
    layer_name,
    table_name,
    record_count,
    latest_data_date,
    table_type,
    CASE 
        WHEN record_count = 0 THEN '⚠️ NO DATA'
        WHEN record_count < 100 THEN '⚠️ LOW VOLUME'
        ELSE '✅ HEALTHY'
    END AS health_status,
    DATEDIFF('day', latest_data_date, CURRENT_DATE()) AS days_since_last_update
FROM REALTIME_PIPELINE_STATUS 
ORDER BY 
    CASE layer_name 
        WHEN 'Bronze Layer' THEN 1 
        WHEN 'Silver Layer' THEN 2 
        WHEN 'Gold Layer' THEN 3 
        WHEN 'Platinum Layer' THEN 4 
    END;



-- 6.3: Funnel Metrics Summary View
-- 
-- PURPOSE: This view provides HR teams and business leaders with summarized 
-- recruitment funnel performance across key dimensions. It shows how candidates
-- progress through the hiring process and where drop-offs occur, enabling
-- data-driven optimization of recruitment strategies.
--
CREATE OR REPLACE VIEW FUNNEL_METRICS_SUMMARY AS
SELECT 
    GEOGRAPHIC_REGION,
    CANDIDATE_COUNTRY_CLEAN,
    JOB_FUNCTION_CATEGORY,
    SUM(TOTAL_APPLICATIONS) AS total_applications,
    
    -- Funnel progression percentages (of all applicants, how many reach each stage)
    ROUND(AVG(PCT_REACHED_IN_PROCESS), 2) AS avg_pct_reached_in_process,
    ROUND(AVG(PCT_REACHED_OFFERED), 2) AS avg_pct_reached_offered,
    ROUND(AVG(PCT_REACHED_HIRED), 2) AS avg_pct_reached_hired,
    
    -- Drop-off analysis (where do we lose candidates)
    ROUND(AVG(PCT_DECLINED), 2) AS avg_pct_declined,
    ROUND(AVG(PCT_WITHDRAWN), 2) AS avg_pct_withdrawn,
    
    -- Overall success rate
    ROUND(AVG(OVERALL_SUCCESS_RATE), 2) AS avg_overall_success_rate
    
FROM GOLD_CUSTOMER_HIRING_METRICS
WHERE SNAPSHOT_YEAR = EXTRACT(YEAR FROM CURRENT_DATE())
GROUP BY GEOGRAPHIC_REGION, CANDIDATE_COUNTRY_CLEAN, JOB_FUNCTION_CATEGORY
ORDER BY total_applications DESC;

-- Demonstrate what this view shows: Funnel optimization insights
-- This helps HR teams identify which regions, countries, or job functions have
-- the most efficient hiring funnels and where process improvements are needed.
SELECT 
    GEOGRAPHIC_REGION,
    JOB_FUNCTION_CATEGORY,
    total_applications,
    avg_pct_reached_in_process,
    avg_pct_reached_offered,
    avg_pct_reached_hired,
    avg_overall_success_rate,
    CASE 
        WHEN avg_overall_success_rate >= 15 THEN '🟢 High Performance'
        WHEN avg_overall_success_rate >= 8 THEN '🟡 Average Performance' 
        ELSE '🔴 Needs Improvement'
    END AS funnel_performance_rating
FROM FUNNEL_METRICS_SUMMARY 
WHERE total_applications >= 50  -- Focus on statistically significant samples
ORDER BY avg_overall_success_rate DESC
LIMIT 15;

-- =====================================================
-- PIPELINE SUMMARY
-- =====================================================
/*
PIPELINE ARCHITECTURE SUMMARY:

1. BRONZE LAYER (TA_APPLICATION_DATA_BRONZE_DEDUP)
   - Raw, deduplicated application data
   - Static table with manual refresh

2. SILVER LAYER (Dynamic Tables)
   - SILVER_APPLICATIONS_CLEANED: Data cleaning and standardization
   - SILVER_APPLICATIONS_ENRICHED: UDF-enhanced data with business logic
   - TARGET_LAG: 5-10 minutes for near real-time processing

3. APPLICATION OUTCOME MAPPING (CTAS)
   - APPLICATION_OUTCOME_MAPPING: Deterministic status-to-funnel mapping
   - 6 refined categories: Applied, In Process, Offered, Hired, Declined, Withdrawn
   - Includes funnel ordering for proper stage progression

4. GOLD LAYER (Dynamic Tables)
   - GOLD_CUSTOMER_HIRING_METRICS: Aggregated KPIs and funnel conversion metrics
   - GOLD_APPLICATION_TRENDS: Time series analysis and trends
   - TARGET_LAG: 1 minute for near real-time analytical workloads

5. PLATINUM LAYER (Stored Procedures + Tasks)
   - PLATINUM_EXECUTIVE_DASHBOARD: Executive summary table
   - Automated via PLATINUM_EXECUTIVE_DASHBOARD_TASK
   - Scheduled daily at 8 AM UTC

6. UDFs DEMONSTRATED:
   - Python UDF: CALCULATE_WORKING_DAYS_TO_HIRE (date calculations)
   - SQL UDF: CATEGORIZE_JOB_FUNCTION (text categorization)

7. FUNNEL CATEGORIES (via Mapping Table):
   - Applied: New, Open (initial application states)
   - In Process: In Process, Invited, Completed (actively being reviewed)
   - Offered: Offered, Offer approved, Verbal Offer Accepted (offer stages)
   - Hired: Hired, Workday New Hire Integration (successful outcomes)
   - Declined: Rejected, Offer declined, Auto-declined (unsuccessful outcomes)
   - Withdrawn: Withdrawn, Cancelled, Closed, Auto-closed (withdrawn applications)

8. ADDITIONAL FEATURES:
   - Views for easy analyst access (including simplified funnel metrics summary)
   - Real-time monitoring capabilities
   - Error handling in stored procedures

This pipeline showcases Snowflake's declarative data engineering approach,
multi-language support, automated processing capabilities, and deterministic
funnel analysis for recruitment optimization.
*/ 