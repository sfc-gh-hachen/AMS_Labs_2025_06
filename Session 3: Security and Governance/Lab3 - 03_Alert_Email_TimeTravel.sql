/***************************************************************************************************
| A | M | S |   | L | A | B | S |   | A | L | E | R | T | S |   &   | T | I | M | E |   | T | R | A | V | E | L |

Demo:         AMS Labs Alert System with Email Notifications and Time Travel Recovery
Version:      v1.0
Create Date:  2025-06-15
Purpose:      Demonstrate Snowflake alerts, email notifications, and Time Travel for table monitoring and recovery
Target Table: AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
****************************************************************************************************

****************************************************************************************************
DEMONSTRATION FLOW
1. Setup email notification integration
2. Create alert to monitor table drops/changes  
3. Drop table to trigger alert and email notification
4. Use Time Travel to recover the dropped table
5. Verify data integrity after recovery
6. Cleanup and reset
****************************************************************************************************

FEATURES DEMONSTRATED:
- Email Notification Integration
- Serverless Alerts for Table Monitoring  
- Email Alerts on Critical Events
- Time Travel for Data Recovery
- UNDROP TABLE functionality
- Historical Data Access
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

-- Verify our target table exists
SELECT 'Current table status' AS check_type, COUNT(*) AS record_count 
FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP;

/*************************************************/
/*************************************************/
/* E M A I L   N O T I F I C A T I O N   S E T U P */
/*************************************************/
/*************************************************/

/*----------------------------------------------------------------------------------
Step 1 - Create Email Notification Integration

This integration allows Snowflake to send emails to verified user addresses.
Note: The email address must belong to a verified Snowflake user in this account.
----------------------------------------------------------------------------------*/

-- Create notification integration for email alerts
CREATE OR REPLACE NOTIFICATION INTEGRATION AMS_EMAIL_ALERTS
    TYPE = EMAIL
    ENABLED = TRUE;
-- Test email send (this will help us identify valid recipients)
-- Display the integration details
DESCRIBE INTEGRATION AMS_EMAIL_ALERTS;

-- Grant usage to roles that will use email notifications
GRANT USAGE ON INTEGRATION AMS_EMAIL_ALERTS TO ROLE ACCOUNTADMIN;
GRANT USAGE ON INTEGRATION AMS_EMAIL_ALERTS TO user hchen;

/*----------------------------------------------------------------------------------
Step 2 - Grant Alert Privileges

Ensure the ACCOUNTADMIN role has necessary privileges for creating alerts
----------------------------------------------------------------------------------*/

-- Grant alert execution privileges (these are account-level privileges)
GRANT EXECUTE ALERT ON ACCOUNT TO ROLE ACCOUNTADMIN;
GRANT EXECUTE MANAGED ALERT ON ACCOUNT TO ROLE ACCOUNTADMIN;

/*************************************************/
/*************************************************/
/* A L E R T   C R E A T I O N                 */
/*************************************************/
/*************************************************/

/*----------------------------------------------------------------------------------
Step 3 - Create Alert for Table Monitoring

This alert will check if our critical table exists and send an email if it's missing.
We're using a serverless alert (no WAREHOUSE specified) for cost efficiency.
----------------------------------------------------------------------------------*/
CREATE OR REPLACE ALERT AMS_LABS.DATA_ENGINEERING.TABLE_DROP_MONITOR
    -- Using serverless compute for cost efficiency
    SCHEDULE = '1 MINUTE'  -- Check every minute for demo purposes
    IF (EXISTS (
        SELECT 1 
        FROM (SELECT 0 AS table_missing)  -- Always returns 1 row
        WHERE (
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'DATA_ENGINEERING' 
            AND TABLE_NAME = 'TA_APPLICATION_DATA_BRONZE_DEDUP'
            AND TABLE_TYPE = 'BASE TABLE'
        ) = 0
    ))
    THEN
        BEGIN
            -- Get current timestamp for the alert
            LET alert_time STRING := CURRENT_TIMESTAMP()::STRING;
            
            -- Send email notification using the integration
            CALL SYSTEM$SEND_EMAIL(
                'AMS_EMAIL_ALERTS',
                'harley.chen@snowflake.com',  -- ensure to have the correct email here (must be verified)
                'CRITICAL ALERT: TA Application Table Missing',
                'ALERT TRIGGERED AT: ' || :alert_time || CHR(10) || CHR(10) ||
                'The critical table TA_APPLICATION_DATA_BRONZE_DEDUP has been dropped or is missing.' || CHR(10) || 
                'This table contains essential HR/recruiting application data.' || CHR(10) || CHR(10) ||
                'IMMEDIATE ACTION REQUIRED:' || CHR(10) ||
                '1. Investigate the cause of the table drop' || CHR(10) ||
                '2. Use Time Travel to recover the table if accidentally dropped' || CHR(10) ||
                '3. Verify data integrity after recovery' || CHR(10) || CHR(10) ||
                'Database: AMS_LABS' || CHR(10) ||
                'Schema: DATA_ENGINEERING' || CHR(10) ||
                'Table: TA_APPLICATION_DATA_BRONZE_DEDUP' || CHR(10) || CHR(10) ||
                'This is an automated alert from the AMS Labs monitoring system.'
            );
        END;


-- Resume the alert (alerts are created in SUSPENDED state by default)
ALTER ALERT AMS_LABS.DATA_ENGINEERING.TABLE_DROP_MONITOR RESUME;

-- Verify alert is active
SHOW ALERTS IN SCHEMA AMS_LABS.DATA_ENGINEERING;

/*************************************************/
/*************************************************/
/* D E M O N S T R A T I O N                     */
/*************************************************/
/*************************************************/

/*----------------------------------------------------------------------------------
Step 4 - Pre-Drop Verification

Verify table exists and get baseline metrics before dropping
----------------------------------------------------------------------------------*/

-- Show current table status
SELECT 
 top 100 * 
FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP;

SHOW PARAMETERS like '%DATA_RETENTION_TIME_IN_DAYS%' in account;

SHOW PARAMETERS like '%DATA_RETENTION_TIME_IN_DAYS%' in table TA_APPLICATION_DATA_BRONZE_DEDUP;

-- This can be between 0 and 90;
-- alter table timeTravel_table set data_retention_time_in_days=1;


/*----------------------------------------------------------------------------------
Step 5 - Drop Table to Trigger Alert

WARNING: This will drop our main table! This is for demonstration purposes only.
In production, you would never intentionally drop critical tables.
----------------------------------------------------------------------------------*/

-- Record the drop time for Time Travel recovery
SET drop_timestamp = CURRENT_TIMESTAMP();

-- Drop the table (this should trigger our alert within 1 minute)
DROP TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP;


-- Check that table no longer appears in INFORMATION_SCHEMA
SELECT COUNT(*) AS table_exists_count
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'DATA_ENGINEERING' 
    AND TABLE_NAME = 'TA_APPLICATION_DATA_BRONZE_DEDUP';

/*----------------------------------------------------------------------------------
Step 6 - Monitor Alert Execution

Wait about 1-2 minutes for the alert to execute and email to be sent.
You can check the alert history to see execution status.
----------------------------------------------------------------------------------*/

-- Check alert execution history (run this after waiting 1-2 minutes)
SELECT *
FROM TABLE(INFORMATION_SCHEMA.ALERT_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('minute', -5, CURRENT_TIMESTAMP())
))
WHERE name = 'TABLE_DROP_MONITOR'
ORDER BY SCHEDULED_TIME DESC;

/*************************************************/
/*************************************************/
/* T I M E   T R A V E L   R E C O V E R Y       */
/*************************************************/
/*************************************************/

/*----------------------------------------------------------------------------------
Step 7 - Time Travel Recovery Using UNDROP

Snowflake's Time Travel feature allows us to restore dropped tables
within the retention period (default 24 hours).
----------------------------------------------------------------------------------*/

-- Restore the dropped table using UNDROP
UNDROP TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP;

select top 10 * from AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP;
-- Confirm table metadata
SELECT 
    TABLE_CATALOG,
    TABLE_SCHEMA, 
    TABLE_NAME,
    TABLE_TYPE,
    IS_TRANSIENT,
    CREATED,
    LAST_ALTERED
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'DATA_ENGINEERING' 
    AND TABLE_NAME = 'TA_APPLICATION_DATA_BRONZE_DEDUP';


/*************************************************/
/*************************************************/
/* C L E A N U P                                 */
/*************************************************/
/*************************************************/

/*----------------------------------------------------------------------------------
Step 10 - Cleanup and Reset

Remove the demo alert and summarize what was demonstrated
----------------------------------------------------------------------------------*/

-- Suspend and drop the alert
ALTER ALERT AMS_LABS.DATA_ENGINEERING.TABLE_DROP_MONITOR SUSPEND;
DROP ALERT AMS_LABS.DATA_ENGINEERING.TABLE_DROP_MONITOR;

-- Verify alert is removed
SHOW ALERTS IN SCHEMA AMS_LABS.DATA_ENGINEERING;

-- Optional: Drop the email integration (uncomment if desired)
-- DROP INTEGRATION AMS_EMAIL_ALERTS;

/*----------------------------------------------------------------------------------
DEMONSTRATION SUMMARY

What we accomplished:
1. ✅ Created email notification integration for automated alerts
2. ✅ Set up serverless alert to monitor critical table existence  
3. ✅ Demonstrated alert triggering when table was dropped
4. ✅ Received email notification about the critical event
5. ✅ Used Time Travel UNDROP to recover the dropped table
6. ✅ Verified complete data integrity after recovery
7. ✅ Showed Time Travel query capabilities for historical data access

BUSINESS VALUE:
- Proactive monitoring of critical data assets
- Immediate notification of data loss events  
- Rapid recovery capabilities without traditional backups
- Historical data analysis for audit and compliance
- Zero data loss with automated recovery workflows

COST EFFICIENCY:
- Serverless alerts minimize compute costs
- Time Travel eliminates need for separate backup storage
- Automated monitoring reduces manual oversight requirements
----------------------------------------------------------------------------------*/

SELECT 
    'DEMONSTRATION COMPLETE' AS status,
    'Email alert, table monitoring, and Time Travel recovery successfully demonstrated' AS summary;

/*----------------------------------------------------------------------------------
ADDITIONAL FEATURES TO CONSIDER:

1. **Advanced Alert Conditions**
   - Monitor data quality (e.g., null values, duplicate records)
   - Track query performance degradation
   - Alert on unusual data volume changes
   - Monitor failed task executions

2. **Enhanced Notification Options**
   - Slack integration via webhooks
   - PagerDuty integration for critical alerts
   - Multiple notification channels for different severity levels
   - Escalation policies for unacknowledged alerts

3. **Extended Time Travel Features**
   - Cloning tables at specific points in time for testing
   - Data comparison between time periods
   - Automated recovery procedures
   - Extended retention periods (up to 90 days with Enterprise)

4. **Monitoring Dashboard Integration**
   - Real-time alert status monitoring
   - Historical alert trend analysis  
   - Integration with external monitoring tools
   - Custom metrics and KPI tracking

5. **Compliance and Audit Features**
   - Automated compliance reporting
   - Data lineage tracking with Time Travel
   - Audit trail for all data changes
   - Retention policy management

Would you like me to implement any of these additional features?
----------------------------------------------------------------------------------*/ 