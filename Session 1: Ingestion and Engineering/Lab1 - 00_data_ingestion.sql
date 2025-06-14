use role accountadmin;

use database ams_labs;
use schema data_engineering;

-- =====================================================

-- -- Step 1: Create a file format that handles the CSV structure
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

-- Step 4: Load data from all fixed chunk files
CREATE OR REPLACE PIPE mypipe_internal
  AUTO_INGEST = TRUE
  AS
    COPY INTO AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE
    FROM @TA_APPLICATION_DATA
    PATTERN = '.*TA_Applications_Data_For_Workshops v3_chunk_.*\.csv'
    FILE_FORMAT = (FORMAT_NAME = 'csv_format')
    ON_ERROR = CONTINUE;

    select SYSTEM$PIPE_STATUS('mypipe_internal');

ALTER PIPE mypipe_internal REFRESH;

-- Step 5: Verify the data load
SELECT COUNT(*) as TOTAL_ROWS FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE;

-- Step 6: Sample the data to verify structure
SELECT * FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE
order by TA_APPLICATIONS_UK
LIMIT 100;

-- Step 7: Show final table structure
DESCRIBE TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE;

-- =====================================================
-- Data Quality Checks
-- =====================================================

-- -- Check unique application IDs
-- SELECT 
--     COUNT(*) as TOTAL_RECORDS,
--     COUNT(DISTINCT "TA_Applications_ID") as UNIQUE_APPLICATION_IDS
-- FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_clone
-- ;

-- -- Check job status distribution
-- -- Using Copilot, we can try asking a question like 'I have just loaded data into TA_APPLICATION_DATA_BRONZE. Read the schema of this table and help me check the job status distribution.'
-- SELECT 
--     jobstatus,
--     COUNT(*) as COUNT
-- FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE
-- GROUP BY jobstatus
-- ORDER BY COUNT DESC;

-- -- Check application status distribution
-- SELECT 
--     APPLICATIONSTATUS,
--     COUNT(*) as COUNT
-- FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE
-- GROUP BY APPLICATIONSTATUS
-- ORDER BY COUNT DESC;

----------
-- Step 6: Sample the data to verify structure
SELECT * FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE
order by JOBNUMBER, snapshot_date
LIMIT 500;

-- Create Dynamic Table for automatic deduplication that refreshes every minute
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
Columns to REMOVE for Business Deduplication:
Insert_Date_DB - Timestamp when record was inserted into database
Update_Date_DB - Timestamp when record was last updated
Insert_File_DB - Source file name (e.g., "Export_-Applications_202111180216_fixed.csv")
Update_File_DB - Update file name (e.g., "TAApplication_Updates_202312121306.xlsx.csv")
Reload_Date_DB - Database reload timestamp
Snapshot_Date - When the data snapshot was taken
Snapshot_Type - Type of snapshot (usually "M" for monthly)
Snapshot_Insert_Date - When snapshot was inserted
*/

select top 500 * from AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP;
-- Use generate to create age