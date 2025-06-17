/***************************************************************************************************
| A | M | S |   | L | A | B | S |   | C | U | S | T | O | M | E | R |   | D | E | M | O |

Demo:         AMS Labs Data Ingestion and Loading 
Create Date:  2025-06-15
Purpose:      Demonstrate Snowflake data ingestion capabilities using CSV files and Snowpipe
Data Source:  AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE
Customer:     Talent Acquisition Data Quality & Analytics
****************************************************************************************************

/*----------------------------------------------------------------------------------
This script demonstrates core Snowflake data ingestion capabilities for loading 
talent acquisition application data. It covers file staging, pipe creation for 
automated loading, and basic data quality validation. The script processes 
CSV files from a GitHub repository stage into a bronze layer table.

Key Concepts:
  • File Formats: Configuration for parsing CSV files with custom delimiters
  • Snowpipe: Automated data loading from staged files
  • Stages: Internal and external storage locations for data files
----------------------------------------------------------------------------------*/

use role accountadmin;

use database ams_labs;
use schema data_engineering;

-- =====================================================
-- DATA INGESTION LAB - Loading TA Application Data
-- =====================================================

-- Step 1: Copy the first 10 files into our stage
-- This copies files 002-010 from the repository into our internal stage
COPY FILES
  INTO @TA_APPLICATION_DATA
  FROM '@ams_labs_repo/branches/main/Session 1: Ingestion and Engineering/Dataset/TA_Application_Data/'
  PATTERN='.*TA_Applications_Data_For_Workshops_(00[2-9]|010)\.csv';

-- Verify files were copied successfully
LIST @AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA;

-- Step 2: Create a file format that handles the CSV structure
-- This format handles semicolon-delimited files with headers and quoted fields
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = CSV
    FIELD_DELIMITER = ';'
    RECORD_DELIMITER = '\n'
    PARSE_HEADER = FALSE
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ESCAPE_UNENCLOSED_FIELD = NONE
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    NULL_IF = ('NULL', 'null', '', '""', 'N/A');

-- Step 3: Create a pipe for automatic data loading
-- This pipe will automatically load data from chunk files as they arrive
CREATE OR REPLACE PIPE mypipe_internal
  AUTO_INGEST = TRUE
  AS
    COPY INTO AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE
    FROM @TA_APPLICATION_DATA
    PATTERN = '.*TA_Applications_Data_For_Workshops_.*\.csv'
    FILE_FORMAT = (FORMAT_NAME = 'csv_format')
    ON_ERROR = CONTINUE;

-- Check pipe status
select SYSTEM$PIPE_STATUS('mypipe_internal');

-- Manually refresh the pipe to process any existing files
ALTER PIPE mypipe_internal REFRESH;

-- Step 4: Verify the data load
-- Check total number of records loaded
SELECT COUNT(*) as TOTAL_ROWS FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE;

-- Step 5: Sample the data to verify structure
-- View first 100 records ordered by application ID
SELECT * FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE
order by TA_APPLICATIONS_UK
LIMIT 100;

-- Step 6: Show table structure
-- Display column names and data types
DESCRIBE TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE;

-- =====================================================
-- Data Quality Checks (Optional - Uncomment to run)
-- =====================================================

-- Check unique application IDs
-- SELECT 
--     COUNT(*) as TOTAL_RECORDS,
--     COUNT(DISTINCT "TA_Applications_ID") as UNIQUE_APPLICATION_IDS
-- FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE;

-- Check job status distribution
-- SELECT 
--     jobstatus,
--     COUNT(*) as COUNT
-- FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE
-- GROUP BY jobstatus
-- ORDER BY COUNT DESC;

-- Check application status distribution
-- SELECT 
--     APPLICATIONSTATUS,
--     COUNT(*) as COUNT
-- FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE
-- GROUP BY APPLICATIONSTATUS
-- ORDER BY COUNT DESC;

-- =====================================================
-- Data Exploration and Deduplication
-- =====================================================

-- Step 7: Explore data with time-based ordering
-- View records ordered by job number and snapshot date to understand data structure
SELECT * FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE
order by JOBNUMBER, snapshot_date
LIMIT 500;

-- Step 8: Create Dynamic Table for automatic deduplication
-- This table automatically refreshes every minute and removes duplicates
-- keeping the most recent record for each application ID and status combination
CREATE OR REPLACE DYNAMIC TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
TARGET_LAG = '1 minute'
WAREHOUSE = COMPUTE_WH
AS 
WITH ranked_data AS (
    SELECT 
        * exclude(Insert_Date_DB,
        Update_Date_DB,
        Insert_File_DB,
        Update_File_DB,
        Reload_Date_DB,
        Snapshot_Type,
        Snapshot_Insert_Date),
        ROW_NUMBER() OVER (
            PARTITION BY TA_Applications_ID, APPLICATIONSTATUS 
            ORDER BY Snapshot_Date DESC
        ) as rn
    FROM TA_APPLICATION_DATA_BRONZE
)
SELECT * exclude(rn)
FROM ranked_data 
WHERE rn = 1;

/*
Technical columns excluded from business analysis:
- Insert_Date_DB: Database insertion timestamp
- Update_Date_DB: Last update timestamp  
- Insert_File_DB: Source file name
- Update_File_DB: Update file name
- Reload_Date_DB: Database reload timestamp
- Snapshot_Type: Snapshot type indicator
- Snapshot_Insert_Date: Snapshot insertion date

The deduplication keeps the most recent record per application ID and status.
*/

-- Step 9: Verify deduplicated data
-- Sample the cleaned, deduplicated dataset
select top 500 * from AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP;

-- Step 10: Let's upload the rest of the files (BUT ONLY AFTER WE RUN THROUGH THE PIPELINE SO 
-- WE CAN SEE THE PIPELINE WORKING!)

-- COPY FILES
--   INTO @TA_APPLICATION_DATA
--   FROM '@ams_labs_repo/branches/main/Session 1: Ingestion and Engineering/Dataset/TA_Application_Data/'
--   PATTERN='.*TA_Applications_Data_For_Workshops_(01[1-9]|02[0-9])\.csv';