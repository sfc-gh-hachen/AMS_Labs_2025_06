/***************************************************************************************************
| A | M | S |   | L | A | B | S |   | A | D | M | I | N |   | M | O | N | I | T | O | R | I | N | G |

Demo:         AMS Labs Data Admin Monitoring for TA Application Data
Version:      v1.0
Create Date:  2025-06-15
Purpose:      Monitor access, performance, and compliance for HR/recruiting data
Target Table: AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
****************************************************************************************************

****************************************************************************************************
SUMMARY OF FEATURES
- Access History Analysis (Read/Write Operations)
- Query Performance Monitoring
- Sensitive Data Flow Tracking
- Security & Compliance Auditing
- HR-Specific Data Usage Patterns
***************************************************************************************************/

/*************************************************/
/*************************************************/
/*           S E T U P                           */
/*************************************************/
/*************************************************/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE AMS_LABS;
USE SCHEMA DATA_ENGINEERING;

-- Note: Access History has up to 3 hours latency, so recent queries may not appear immediately

/*************************************************/
/*************************************************/
/* A C C E S S   H I S T O R Y   A N A L Y S I S */
/*************************************************/
/*************************************************/

/*----------------------------------------------------------------------------------
Step 1 - Table Access Frequency Analysis

Monitor how frequently our TA application data is being accessed
----------------------------------------------------------------------------------*/
select top 100 * from snowflake.account_usage.access_history;

-- How many queries have accessed our TA application table and view directly?
-- Breakdown between Read and Write operations
SELECT 
    value:"objectName"::STRING AS object_name,
    CASE 
        WHEN object_modified_by_ddl IS NOT NULL THEN 'WRITE'
        ELSE 'READ'
    END AS operation_type,
    COUNT(DISTINCT query_id) AS number_of_queries,
    COUNT(DISTINCT user_name) AS unique_users,
    MAX(query_start_time) AS last_operation_time
FROM snowflake.account_usage.access_history,
LATERAL FLATTEN (input => direct_objects_accessed)
WHERE object_name ILIKE '%TA_APPLICATION_DATA_BRONZE_DEDUP'
GROUP BY object_name, operation_type
ORDER BY object_name, number_of_queries DESC;

-- In workspace, we can further interogate the data by simply asking questions like, 
-- 'Show me the user names from the query above.'
-- SELECT DISTINCT user_name
-- FROM snowflake.account_usage.access_history
-- WHERE query_id IN (
--     SELECT query_id 
--     FROM snowflake.account_usage.access_history,
--     LATERAL FLATTEN (input => direct_objects_accessed)
--     WHERE value:"objectName"::STRING ILIKE '%TA_APPLICATION_DATA_BRONZE_DEDUP%'
-- );




/*----------------------------------------------------------------------------------
Step 2 - User Access Patterns

Identify who is accessing the TA data and when
----------------------------------------------------------------------------------*/

-- Top users accessing TA application data
SELECT
    ah.user_name,
    COUNT(DISTINCT ah.query_id) AS total_queries,
    COUNT(DISTINCT CASE WHEN qh.query_type = 'SELECT' THEN ah.query_id END) AS read_queries,
    COUNT(DISTINCT CASE WHEN qh.query_type != 'SELECT' THEN ah.query_id END) AS write_queries,
    MIN(ah.query_start_time) AS first_access,
    MAX(ah.query_start_time) AS last_access
FROM snowflake.account_usage.access_history ah
JOIN snowflake.account_usage.query_history qh ON ah.query_id = qh.query_id,
LATERAL FLATTEN(input => ah.direct_objects_accessed) obj
WHERE obj.value:"objectName"::STRING ILIKE '%TA_APPLICATION_DATA_BRONZE_DEDUP'
    AND ah.query_start_time > dateadd(day, -30, current_date())
GROUP BY ah.user_name
ORDER BY total_queries DESC;

-- Role-based access analysis
SELECT
    qh.role_name,
    COUNT(DISTINCT ah.query_id) AS total_queries,
    COUNT(DISTINCT ah.user_name) AS unique_users,
    MAX(ah.query_start_time) AS last_access
FROM snowflake.account_usage.access_history ah
JOIN snowflake.account_usage.query_history qh ON ah.query_id = qh.query_id,
LATERAL FLATTEN(input => ah.direct_objects_accessed) obj
WHERE obj.value:"objectName"::STRING ILIKE '%TA_APPLICATION_DATA_BRONZE_DEDUP'
    AND ah.query_start_time > dateadd(day, -30, current_date())
GROUP BY qh.role_name
ORDER BY total_queries DESC;

SELECT
    qh.role_name AS metric_category,
    qh.user_name,
    COUNT(DISTINCT qh.query_id) AS total_queries,
    COUNT(DISTINCT CASE WHEN qh.execution_status = 'SUCCESS' THEN qh.query_id END) AS successful_queries,
    COUNT(DISTINCT CASE WHEN qh.execution_status != 'SUCCESS' THEN qh.query_id END) AS failed_queries,
    ROUND(AVG(qh.total_elapsed_time/1000), 2) AS avg_execution_time_sec,
    MIN(qh.start_time) AS first_query,
    MAX(qh.start_time) AS last_query
FROM snowflake.account_usage.query_history qh
WHERE qh.start_time > dateadd(day, -30, current_date()) 
AND USER_NAME NOT IN ('SYSTEM', 'WORKSHEETS_APP_USER', 'FIRST_USER')
GROUP BY 1, 2
ORDER BY total_queries DESC;

/*----------------------------------------------------------------------------------
Step 3 - Recent Query Analysis

Examine recent read and write operations on TA data
----------------------------------------------------------------------------------*/

-- Recent READ queries on TA application data
SELECT
    qh.user_name,
    qh.role_name,
    qh.start_time,
    qh.total_elapsed_time/1000 AS elapsed_seconds,
    LEFT(qh.query_text, 200) AS query_preview,
    value:objectName::string AS accessed_table
FROM snowflake.account_usage.query_history AS qh
JOIN snowflake.account_usage.access_history AS ah ON qh.query_id = ah.query_id,
LATERAL FLATTEN(input => ah.direct_objects_accessed) obj
WHERE qh.query_type = 'SELECT' 
    AND obj.value:objectName::STRING ILIKE '%TA_APPLICATION_DATA_BRONZE_DEDUP'
    AND qh.start_time > dateadd(day, -7, current_date())
ORDER BY qh.start_time DESC
LIMIT 20;

/*************************************************/
/*************************************************/
/* S E C U R I T Y   &   C O M P L I A N C E     */
/*************************************************/
/*************************************************/

/*----------------------------------------------------------------------------------
Step 4 - Security Auditing

Monitor for potential security issues and policy violations
----------------------------------------------------------------------------------*/

-- Failed queries on TA data (potential access violations)
SELECT
    qh.user_name,
    qh.role_name,
    qh.start_time,
    qh.execution_status,
    qh.error_code,
    qh.error_message,
    LEFT(qh.query_text, 200) AS query_preview
FROM snowflake.account_usage.query_history qh
WHERE qh.query_text ILIKE '%TA_APPLICATION_DATA_BRONZE_DEDUP'
    AND qh.execution_status != 'SUCCESS'
    AND qh.start_time > dateadd(day, -7, current_date())
ORDER BY qh.start_time DESC
LIMIT 20;

-- Unusual access patterns (queries outside business hours)
SELECT
    qh.user_name,
    qh.role_name,
    qh.start_time,
    EXTRACT(HOUR FROM qh.start_time) AS query_hour,
    EXTRACT(DOW FROM qh.start_time) AS day_of_week,  -- 0=Sunday, 6=Saturday
    LEFT(qh.query_text, 200) AS query_preview
FROM snowflake.account_usage.query_history qh
WHERE qh.query_text ILIKE '%TA_APPLICATION_DATA_BRONZE_DEDUP'
    AND qh.start_time > dateadd(day, -7, current_date())
    AND (
        EXTRACT(HOUR FROM qh.start_time) < 7 OR  -- Before 7 AM
        EXTRACT(HOUR FROM qh.start_time) > 19 OR -- After 7 PM
        EXTRACT(DOW FROM qh.start_time) IN (0, 6) -- Weekend
    )
ORDER BY qh.start_time DESC;
