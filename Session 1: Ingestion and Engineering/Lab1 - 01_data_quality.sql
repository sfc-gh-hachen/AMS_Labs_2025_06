/***************************************************************************************************
| A | M | S |   | L | A | B | S |   | C | U | S | T | O | M | E | R |   | D | E | M | O |

Demo:         AMS Labs Customer Requirements Demo
Version:      v2.0
Create Date:  December 2024
Purpose:      Demonstrate Snowflake RBAC and Data Quality for Customer Requirements
Data Source:  AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE
Customer:     Talent Acquisition Data Quality & Analytics
****************************************************************************************************

****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
Dec 2024            Assistant           Initial adaptation from Horizon Labs demo
Dec 2024            Assistant           Updated for customer-specific requirements and validation
***************************************************************************************************/

/*----------------------------------------------------------------------------------
Customer Data Quality Requirements:
  • JobNumber = VARCHAR + PREFIX "AMS" mandatory
  • WorkdayID = VARCHAR + PREFIX "JR" mandatory  
  • JobStatus = ALPHA specific values mandatory
  • CandidateStatus = ALPHA specific values mandatory
  • Geographic region mapping and analytics
  • Working days calculations for hiring metrics
----------------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------------
Snowflake's approach to access control combines aspects from both of the following models:
  • Discretionary Access Control (DAC): Each object has an owner, who can in turn grant access to that object.
  • Role-based Access Control (RBAC): Access privileges are assigned to roles, which are in turn assigned to users.

Key Concepts:
  • Securable object: An entity to which access can be granted
  • Role: An entity to which privileges can be granted
  • Privilege: A defined level of access to an object
  • User: A user identity recognized by Snowflake
----------------------------------------------------------------------------------*/

/*************************************************/
/*************************************************/
/*           R B A C    S E T U P                */
/*************************************************/
/*************************************************/

/*----------------------------------------------------------------------------------
Step 1 - Environment Setup and System Roles Overview
----------------------------------------------------------------------------------*/

-- Set up our working environment
USE DATABASE AMS_LABS;
USE SCHEMA DATA_ENGINEERING;
use warehouse COMPUTE_WH;



/*************************************************/
/*************************************************/
/* D A T A   Q U A L I T Y   M O N I T O R I N G */
/*************************************************/
/*************************************************/

/*----------------------------------------------------------------------------------
Step 5 - Data Quality Monitoring Setup

Note: Before proceeding, examine your table structure to identify appropriate columns
Run: DESCRIBE TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE;
----------------------------------------------------------------------------------*/

-- Switch to ACCOUNTADMIN for data quality monitoring setup
USE ROLE ACCOUNTADMIN;

-- First, let's examine the table structure
DESCRIBE TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE;

/*----------------------------------------------------------------------------------
Step 6 - System Data Metric Functions (DMFs)

These are pre-built functions provided by Snowflake for common data quality checks.
We're monitoring key fields in the talent acquisition application data:
- TA_APPLICATIONS_ID: Primary identifier for applications
- CANDIDATEID: Candidate identifier 
- JOBNUMBER: Job posting identifier
- TA_APPLICATIONS_UK: Unique key for applications
----------------------------------------------------------------------------------*/

-- Set up data metric schedule
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE
-- for demo purposes, let's change the frequency to 5 minute
--SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
SET DATA_METRIC_SCHEDULE = '5 minute';

-- Volume Monitoring - Track total row count
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE 
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();

-- Accuracy Monitoring - Track null values for critical fields
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE 
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (TA_APPLICATIONS_ID);

ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE 
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (JOBNUMBER);

-- Uniqueness Monitoring - Track unique and duplicate values for key identifiers
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE 
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.UNIQUE_COUNT ON (TA_APPLICATIONS_UK);

ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE 
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (TA_APPLICATIONS_UK);

/*----------------------------------------------------------------------------------
Step 7 - Custom Data Metric Function for Customer Requirements

This demonstrates creating a custom DMF for customer-specific data quality checks.
We're creating a validation function for JOBNUMBER format:
- Should begin with "AMS" prefix as per customer requirements
----------------------------------------------------------------------------------*/

-- Create a custom DMF for JOBNUMBER format validation (Customer Requirement)
CREATE OR REPLACE DATA METRIC FUNCTION AMS_LABS.DATA_ENGINEERING.INVALID_JOBNUMBER_FORMAT_COUNT(
    IN_TABLE TABLE(IN_COL STRING)
)
RETURNS NUMBER 
AS
'SELECT COUNT_IF(
    IN_COL IS NOT NULL 
    AND FALSE = (IN_COL ILIKE ''AMS%'')
) FROM IN_TABLE';

-- Grant access to the custom function
GRANT ALL ON FUNCTION AMS_LABS.DATA_ENGINEERING.INVALID_JOBNUMBER_FORMAT_COUNT(TABLE(STRING)) TO ROLE PUBLIC;

-- Test the custom function
SELECT AMS_LABS.DATA_ENGINEERING.INVALID_JOBNUMBER_FORMAT_COUNT(
    SELECT JOBNUMBER FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE
) AS INVALID_JOBNUMBER_FORMAT_COUNT;

-- Add the custom DMF to the table for JobNumber validation
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE 
    ADD DATA METRIC FUNCTION AMS_LABS.DATA_ENGINEERING.INVALID_JOBNUMBER_FORMAT_COUNT ON (JOBNUMBER);

-- Keep the existing WorkdayID validation as it's also a customer requirement
CREATE OR REPLACE DATA METRIC FUNCTION AMS_LABS.DATA_ENGINEERING.INVALID_WORKDAYID_FORMAT_COUNT(
    IN_TABLE TABLE(IN_COL STRING)
)
RETURNS NUMBER 
AS
'SELECT COUNT_IF(
    IN_COL IS NOT NULL 
    AND FALSE = (IN_COL REGEXP ''^JR-[0-9]{5}$'')
) FROM IN_TABLE';

-- Grant access to the custom function
GRANT ALL ON FUNCTION AMS_LABS.DATA_ENGINEERING.INVALID_WORKDAYID_FORMAT_COUNT(TABLE(STRING)) TO ROLE PUBLIC;

-- Add the WorkdayID validation DMF
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE 
    ADD DATA METRIC FUNCTION AMS_LABS.DATA_ENGINEERING.INVALID_WORKDAYID_FORMAT_COUNT ON (WORKDAYID);

-- Create custom DMF for JobStatus validation (Customer Requirement)
CREATE OR REPLACE DATA METRIC FUNCTION AMS_LABS.DATA_ENGINEERING.INVALID_JOBSTATUS_COUNT(
    IN_TABLE TABLE(IN_COL STRING)
)
RETURNS NUMBER 
AS
'SELECT COUNT_IF(
    IN_COL IS NOT NULL 
    AND IN_COL NOT IN (''Closed/Filled'', ''Closed'', ''Offer Accepted'', ''On Hold'', ''Pending'')
) FROM IN_TABLE';

-- Grant access to the custom function
GRANT ALL ON FUNCTION AMS_LABS.DATA_ENGINEERING.INVALID_JOBSTATUS_COUNT(TABLE(STRING)) TO ROLE PUBLIC;

-- Test the custom function
SELECT AMS_LABS.DATA_ENGINEERING.INVALID_JOBSTATUS_COUNT(
    SELECT JOBSTATUS FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE
) AS INVALID_JOBSTATUS_COUNT;

-- Add the JobStatus validation DMF
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE 
    ADD DATA METRIC FUNCTION AMS_LABS.DATA_ENGINEERING.INVALID_JOBSTATUS_COUNT ON (JOBSTATUS);

-- Create custom DMF for CandidateStatus validation (Customer Requirement)
CREATE OR REPLACE DATA METRIC FUNCTION AMS_LABS.DATA_ENGINEERING.INVALID_CANDIDATESTATUS_COUNT(
    IN_TABLE TABLE(IN_COL STRING)
)
RETURNS NUMBER 
AS
'SELECT COUNT_IF(
    IN_COL IS NOT NULL 
    AND IN_COL NOT IN (''New'', ''Active'', ''Inactive'', ''In Hiring'', ''To be Archived'')
) FROM IN_TABLE';

-- Grant access to the custom function
GRANT ALL ON FUNCTION AMS_LABS.DATA_ENGINEERING.INVALID_CANDIDATESTATUS_COUNT(TABLE(STRING)) TO ROLE PUBLIC;

-- Test the custom function
SELECT AMS_LABS.DATA_ENGINEERING.INVALID_CANDIDATESTATUS_COUNT(
    SELECT CANDIDATESTATUS FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE
) AS INVALID_CANDIDATESTATUS_COUNT;

-- Add the CandidateStatus validation DMF
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE 
    ADD DATA METRIC FUNCTION AMS_LABS.DATA_ENGINEERING.INVALID_CANDIDATESTATUS_COUNT ON (CANDIDATESTATUS);



/*----------------------------------------------------------------------------------
Step 8 - Monitor Data Quality Results
----------------------------------------------------------------------------------*/

-- Verify the data metric schedule is set
SHOW PARAMETERS LIKE 'DATA_METRIC_SCHEDULE' IN TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE;

-- Review active data metric functions
SELECT 
    metric_name, 
    ref_entity_name, 
    schedule, 
    schedule_status 
FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    ref_entity_name => 'AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE', 
    ref_entity_domain => 'TABLE'
));

SELECT 
    change_commit_time,
    measurement_time,
    table_database,
    table_schema,
    table_name,
    metric_name,
    value
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_database = 'AMS_LABS'
    AND table_schema = 'DATA_ENGINEERING'
    AND table_name = 'TA_APPLICATION_DATA_BRONZE'
ORDER BY change_commit_time DESC;