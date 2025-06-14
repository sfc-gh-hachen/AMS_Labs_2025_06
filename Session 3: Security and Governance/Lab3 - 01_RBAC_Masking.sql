/***************************************************************************************************
| A | M | S |   | L | A | B | S |   | D | A | T | A |   | G | O | V | E | R | N | A | N | C | E |

Demo:         AMS Labs Data Governance for TA Application Data
Version:      v1.0
Create Date:  2025-06-15
Purpose:      Demonstrate Snowflake data governance features with HR/recruiting data
Target Table: AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
****************************************************************************************************

****************************************************************************************************
SUMMARY OF FEATURES
- Data Classification (Automatic & Custom)
- Tagging Framework for HR Data
- Dynamic Data Masking for PII Protection
- Row Access Policies for Geographic/Status-based Access
- Aggregation Policies for Privacy Protection
- Projection Policies for Sensitive Column Control
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



-- Let's examine the current roles in our account
SHOW ROLES;

-- Filter to show Snowflake system-defined roles
SELECT
    "name",
    "comment"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "name" IN ('ORGADMIN','ACCOUNTADMIN','SYSADMIN','USERADMIN','SECURITYADMIN','PUBLIC');
/**
  Snowflake System Defined Role Definitions:
   1 - ORGADMIN: Role that manages operations at the organization level.
   2 - ACCOUNTADMIN: Role that encapsulates the SYSADMIN and SECURITYADMIN system-defined roles.
        It is the top-level role in the system and should be granted only to a limited/controlled number of users
        in your account.
   3 - SECURITYADMIN: Role that can manage any object grant globally, as well as create, monitor,
      and manage users and roles.
   4 - USERADMIN: Role that is dedicated to user and role management only.
   5 - SYSADMIN: Role that has privileges to create warehouses and databases in an account.
      If, as recommended, you create a role hierarchy that ultimately assigns all custom roles to the SYSADMIN role, this role also has
      the ability to grant privileges on warehouses, databases, and other objects to other roles.
   6 - PUBLIC: Pseudo-role that is automatically granted to every user and every role in your account. The PUBLIC role can own securable
      objects, just like any other role; however, the objects owned by the role are available to every other
      user and role in your account.

                                +---------------+
                                | ACCOUNTADMIN  |
                                +---------------+
                                  ^    ^     ^
                                  |    |     |
                    +-------------+-+  |    ++-------------+
                    | SECURITYADMIN |  |    |   SYSADMIN   |<------------+
                    +---------------+  |    +--------------+             |
                            ^          |     ^        ^                  |
                            |          |     |        |                  |
                    +-------+-------+  |     |  +-----+-------+  +-------+-----+
                    |   USERADMIN   |  |     |  | CUSTOM ROLE |  | CUSTOM ROLE |
                    +---------------+  |     |  +-------------+  +-------------+
                            ^          |     |      ^              ^      ^
                            |          |     |      |              |      |
                            |          |     |      |              |    +-+-----------+
                            |          |     |      |              |    | CUSTOM ROLE |
                            |          |     |      |              |    +-------------+
                            |          |     |      |              |           ^
                            |          |     |      |              |           |
                            +----------+-----+---+--+--------------+-----------+
                                                 |
                                            +----+-----+
                                            |  PUBLIC  |
                                            +----------+
**/

/*----------------------------------------------------------------------------------
Step 1 - Create AMS-Specific Roles

Now that we understand System Defined Roles, let's create custom roles for
our AMS Labs data governance demonstration.
----------------------------------------------------------------------------------*/

-- Use USERADMIN to create our custom role
USE ROLE USERADMIN;
USE SECONDARY ROLES NONE;

-- Create a Data Analyst role for AMS Labs
CREATE OR REPLACE ROLE AMS_DATA_ANALYST
    COMMENT = 'AMS Labs Data Analyst Role - Limited access for data analysis';

/*----------------------------------------------------------------------------------
Step 2 - Grant Privileges Using SECURITYADMIN
----------------------------------------------------------------------------------*/

-- Switch to SECURITYADMIN for privilege management
USE ROLE SECURITYADMIN;

-- Grant warehouse privileges (adjust warehouse name as needed)
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE AMS_DATA_ANALYST;

-- Grant database and schema access
GRANT USAGE ON DATABASE AMS_LABS TO ROLE AMS_DATA_ANALYST;
GRANT USAGE ON SCHEMA AMS_LABS.DATA_ENGINEERING TO ROLE AMS_DATA_ANALYST;

-- Set up SQL variable for current user and grant roles
SET MY_USER_ID = CURRENT_USER();
GRANT ROLE AMS_DATA_ANALYST TO USER identifier($MY_USER_ID);

/*----------------------------------------------------------------------------------
Step 3 - Test Role Access
----------------------------------------------------------------------------------*/

-- Test access with the analyst role
USE ROLE AMS_DATA_ANALYST;

SELECT top 100 *
FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP;
-- This should NOT work - basic SELECT access because we haven't granted the role such privilege!

USE ROLE SECURITYADMIN;
GRANT SELECT ON TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP TO ROLE AMS_DATA_ANALYST;

USE ROLE AMS_DATA_ANALYST;
-- This should NOT work - basic SELECT access because we haven't granted the role such privilege!
SELECT top 100 *
FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP;


/*************************************************/
/*************************************************/
/*               DATA MASKING                    */
/*************************************************/
/*************************************************/

USE ROLE ACCOUNTADMIN;

-- Create a governance schema for our policies and tags
CREATE SCHEMA IF NOT EXISTS AMS_LABS.GOVERNANCE;
USE SCHEMA AMS_LABS.GOVERNANCE;

/*************************************************/
/*************************************************/
/* D A T A   C L A S S I F I C A T I O N       */
/*************************************************/
/*************************************************/

/*----------------------------------------------------------------------------------
Step 4 - Automatic Data Classification

Snowflake can automatically detect and classify sensitive data in your tables.
Let's classify our TA application data to identify PII and sensitive information.
----------------------------------------------------------------------------------*/

-- First, let's see what data we have
SELECT * FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP LIMIT 5;

-- Run automatic classification on our TA application table
CALL SYSTEM$CLASSIFY('AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP', {'auto_tag': true});

-- View the tags that Snowflake applied automatically
SELECT 
    TAG_DATABASE, 
    TAG_SCHEMA, 
    OBJECT_NAME, 
    COLUMN_NAME, 
    TAG_NAME, 
    TAG_VALUE
FROM TABLE(
    AMS_LABS.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
        'AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP',
        'table'
    )
);

desc table AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP;
/*----------------------------------------------------------------------------------
Step 5 - Custom Classification for HR-Specific Patterns

Create custom classifiers for HR/recruiting specific data patterns.
Define your own semantic category, specify the privacy category, and 
specify regular expressions to match column value patterns while optionally matching the column name.
----------------------------------------------------------------------------------*/
-- -- Create a custom classifier for Workday IDs (JR-##### format)
-- CREATE OR REPLACE SNOWFLAKE.DATA_PRIVACY.CUSTOM_CLASSIFIER WORKDAY_ID_CLASSIFIER();

-- -- Add regex pattern for Workday ID format
-- CALL WORKDAY_ID_CLASSIFIER!ADD_REGEX(
--     'WORKDAY_JOB_REQ',
--     'IDENTIFIER',
--     '^JR-[0-9]{5}$'
-- );
-- -- View the classifier patterns
-- SELECT WORKDAY_ID_CLASSIFIER!LIST();
-- -- Apply custom classification
-- CALL SYSTEM$CLASSIFY(
--     'AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP',
--     {
--         'auto_tag': true, 
--         'custom_classifiers': ['AMS_LABS.GOVERNANCE.WORKDAY_ID_CLASSIFIER']
--     }
-- );

/*************************************************/
/*************************************************/
/* T A G G I N G   F R A M E W O R K           */
/*************************************************/
/*************************************************/
/*----------------------------------------------------------------------------------
Step 6 - Create Custom Tags for HR Data Governance

Create tags specific to HR/recruiting data governance needs
----------------------------------------------------------------------------------*/

-- Cost center tag for chargeback
CREATE OR REPLACE TAG AMS_LABS.GOVERNANCE.COST_CENTER 
    ALLOWED_VALUES 'Engineering', 'Analytics', 'Sales', 'HR', 'Operations'
    COMMENT = 'Department cost center for resource allocation';

-- Data sensitivity levels
CREATE OR REPLACE TAG AMS_LABS.GOVERNANCE.DATA_SENSITIVITY 
    ALLOWED_VALUES 'Public', 'Internal', 'Confidential', 'Highly Confidential'
    COMMENT = 'Data sensitivity classification';

-- PII type classification
CREATE OR REPLACE TAG AMS_LABS.GOVERNANCE.PII_TYPE 
    ALLOWED_VALUES 'Name', 'Location', 'ID', 'Contact', 'Employment'
    COMMENT = 'Type of personally identifiable information';

/*----------------------------------------------------------------------------------
Step 7 - Apply Tags to Table and Columns
----------------------------------------------------------------------------------*/

-- Table level tags
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    SET TAG AMS_LABS.GOVERNANCE.DATA_SENSITIVITY = 'Confidential';

ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    SET TAG AMS_LABS.GOVERNANCE.COST_CENTER = 'HR';

-- Column level tags - Candidate PII
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    MODIFY COLUMN CANDIDATEID SET TAG AMS_LABS.GOVERNANCE.PII_TYPE = 'ID';

ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    MODIFY COLUMN CANDIDATECITY SET TAG AMS_LABS.GOVERNANCE.PII_TYPE = 'Location';

ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    MODIFY COLUMN CANDIDATELOCATION SET TAG AMS_LABS.GOVERNANCE.PII_TYPE = 'Location';

ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    MODIFY COLUMN CANDIDATECOUNTRY SET TAG AMS_LABS.GOVERNANCE.PII_TYPE = 'Location';

ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    MODIFY COLUMN EMPLOYEENUMBER SET TAG AMS_LABS.GOVERNANCE.PII_TYPE = 'Employment';

-- View all tags on our table
SELECT
    TAG_DATABASE,
    TAG_SCHEMA,
    TAG_NAME,
    COLUMN_NAME,
    TAG_VALUE
FROM TABLE(
    INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
        'AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP',
        'table'
    )
);

/*************************************************/
/*************************************************/
/* D Y N A M I C   D A T A   M A S K I N G     */
/*************************************************/
/*************************************************/

/*----------------------------------------------------------------------------------
Step 8 - Create Masking Policies for PII Protection

Create masking policies that will protect sensitive data based on user roles
----------------------------------------------------------------------------------*/

-- Masking policy for candidate IDs
CREATE OR REPLACE MASKING POLICY AMS_LABS.GOVERNANCE.MASK_CANDIDATE_ID AS
    (VAL STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN VAL
        ELSE 'XXXX-' || SUBSTR(VAL, -4)  -- Show only last 4 characters
    END;

-- Masking policy for location data
CREATE OR REPLACE MASKING POLICY AMS_LABS.GOVERNANCE.MASK_LOCATION AS
    (VAL STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN VAL
        WHEN CURRENT_ROLE() IN ('AMS_DATA_ANALYST') THEN 
            CASE 
                WHEN LEN(VAL) > 0 THEN LEFT(VAL, 3) || '***'  -- Show only first 3 chars
                ELSE VAL
            END
        ELSE '***MASKED***'
    END;

-- Masking policy for employee numbers
CREATE OR REPLACE MASKING POLICY AMS_LABS.GOVERNANCE.MASK_EMPLOYEE_NUMBER AS
    (VAL STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN VAL
        ELSE '***CONFIDENTIAL***'
    END;

/*----------------------------------------------------------------------------------
Step 9 - Apply Masking Policies to Columns
----------------------------------------------------------------------------------*/
-- Apply masking to candidate ID
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    MODIFY COLUMN CANDIDATEID SET MASKING POLICY AMS_LABS.GOVERNANCE.MASK_CANDIDATE_ID;
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    MODIFY COLUMN CANDIDATECITY SET MASKING POLICY AMS_LABS.GOVERNANCE.MASK_LOCATION;
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    MODIFY COLUMN CANDIDATELOCATION SET MASKING POLICY AMS_LABS.GOVERNANCE.MASK_LOCATION;
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    MODIFY COLUMN EMPLOYEENUMBER SET MASKING POLICY AMS_LABS.GOVERNANCE.MASK_EMPLOYEE_NUMBER;

-- Test masking as ACCOUNTADMIN (should see full data)
USE ROLE ACCOUNTADMIN;
SELECT 
    CANDIDATEID,
    CANDIDATECITY,
    CANDIDATELOCATION,
    EMPLOYEENUMBER
FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
LIMIT 100;

-- Test masking as AMS_DATA_ANALYST (should see masked data)
USE ROLE AMS_DATA_ANALYST;
SELECT 
    CANDIDATEID,
    CANDIDATECITY,
    CANDIDATELOCATION,
    EMPLOYEENUMBER
FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
LIMIT 100;

/*************************************************/
/*************************************************/
/* R O W   A C C E S S   P O L I C I E S       */
/*************************************************/
/*************************************************/

/*----------------------------------------------------------------------------------
Step 10 - Row Access Policies for Geographic Restrictions

Implement row-level security based on candidate location
----------------------------------------------------------------------------------*/
-- Switch back to ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Create a mapping table for role-based geographic access
CREATE OR REPLACE TABLE AMS_LABS.GOVERNANCE.LOCATION_ACCESS_MAP (
    ROLE STRING,
    ALLOWED_COUNTRY STRING
);

-- Insert access rules (analysts can only see US candidates)
INSERT INTO AMS_LABS.GOVERNANCE.LOCATION_ACCESS_MAP VALUES
    ('AMS_DATA_ANALYST', 'United States'),
    ('ACCOUNTADMIN', 'ALL');

-- Create row access policy based on candidate country
CREATE OR REPLACE ROW ACCESS POLICY AMS_LABS.GOVERNANCE.CANDIDATE_LOCATION_POLICY
    AS (CANDIDATECOUNTRY STRING) RETURNS BOOLEAN ->
    CURRENT_ROLE() IN ('ACCOUNTADMIN')  -- Admin sees all
    OR EXISTS (
        SELECT 1 
        FROM AMS_LABS.GOVERNANCE.LOCATION_ACCESS_MAP 
        WHERE ROLE = CURRENT_ROLE() 
            AND (ALLOWED_COUNTRY = CANDIDATECOUNTRY OR ALLOWED_COUNTRY = 'ALL')
    );

-- Apply row access policy
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    ADD ROW ACCESS POLICY AMS_LABS.GOVERNANCE.CANDIDATE_LOCATION_POLICY ON (CANDIDATECOUNTRY);

-- Test row access as different roles
USE ROLE AMS_DATA_ANALYST;
SELECT 
    CANDIDATECOUNTRY, 
    COUNT(*) as candidate_count 
FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
GROUP BY CANDIDATECOUNTRY;

USE ROLE ACCOUNTADMIN;
SELECT 
    CANDIDATECOUNTRY,
    COUNT(*) as candidate_count 
FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
GROUP BY 1;

/*************************************************/
/*************************************************/
/* A G G R E G A T I O N   P O L I C I E S     */
/*************************************************/
/*************************************************/

/*----------------------------------------------------------------------------------
Step 11 - Aggregation Policies for Privacy Protection

Ensure queries return aggregated results only (minimum 20 records)
----------------------------------------------------------------------------------*/

-- Create aggregation policy
CREATE OR REPLACE AGGREGATION POLICY AMS_LABS.GOVERNANCE.MIN_GROUP_SIZE_POLICY
    AS () RETURNS AGGREGATION_CONSTRAINT ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN NO_AGGREGATION_CONSTRAINT()
        ELSE AGGREGATION_CONSTRAINT(MIN_GROUP_SIZE => 20)
    END;

-- Apply to our table
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    SET AGGREGATION POLICY AMS_LABS.GOVERNANCE.MIN_GROUP_SIZE_POLICY;

-- Test aggregation policy
USE ROLE AMS_DATA_ANALYST;

-- This should fail (trying to see individual records)
SELECT * FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP LIMIT 10;

-- This should work (aggregate query)
SELECT 
    JOBSTATUS,
    APPLICATIONSTATUS,
    COUNT(*) as application_count
FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
GROUP BY JOBSTATUS, APPLICATIONSTATUS
ORDER BY application_count DESC;

USE ROLE ACCOUNTADMIN;
SELECT 
    JOBSTATUS,
    APPLICATIONSTATUS,
    COUNT(*) as application_count
FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
GROUP BY JOBSTATUS, APPLICATIONSTATUS
ORDER BY application_count DESC;

-- Remove projection policies
-- ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
--     UNSET AGGREGATION POLICY;
-- DROP PROJECTION POLICY IF EXISTS AMS_LABS.GOVERNANCE.HIDE_SENSITIVE_COLUMNS;
/*************************************************/
/*************************************************/
/* P R O J E C T I O N   P O L I C I E S       */
/*************************************************/
/*************************************************/

/*----------------------------------------------------------------------------------
Step 12 - Projection Policies for Sensitive Columns

Hide specific sensitive columns from selection while allowing filtering
----------------------------------------------------------------------------------*/

USE ROLE ACCOUNTADMIN;
-- Create projection policy for highly sensitive data
CREATE OR REPLACE PROJECTION POLICY AMS_LABS.GOVERNANCE.HIDE_SENSITIVE_COLUMNS
    AS () RETURNS PROJECTION_CONSTRAINT -> 
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN PROJECTION_CONSTRAINT(ALLOW => true)
        ELSE PROJECTION_CONSTRAINT(ALLOW => false)
    END;

-- Apply to sensitive columns (3 most sensitive as requested)
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    MODIFY COLUMN EMPLOYEENUMBER SET PROJECTION POLICY AMS_LABS.GOVERNANCE.HIDE_SENSITIVE_COLUMNS;
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    MODIFY COLUMN REASONOFREJECTION SET PROJECTION POLICY AMS_LABS.GOVERNANCE.HIDE_SENSITIVE_COLUMNS;

-- Test projection policy
USE ROLE AMS_DATA_ANALYST;

-- This will fail (trying to select protected columns)
SELECT EMPLOYEENUMBER, REASONOFREJECTION FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP LIMIT 10;

USE ROLE ACCOUNTADMIN;
SELECT EMPLOYEENUMBER, REASONOFREJECTION FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP LIMIT 10;

/*************************************************/
/*************************************************/
/* V A L I D A T I O N   &   S U M M A R Y     */
/*************************************************/
/*************************************************/

/*----------------------------------------------------------------------------------
Step 13 - Validate All Governance Policies

Summary of what we've implemented:
1. Automatic and custom data classification
2. Comprehensive tagging framework
3. Dynamic data masking for PII
4. Row access policies for geographic restrictions
5. Aggregation policies (20 record minimum)
6. Projection policies for sensitive columns
----------------------------------------------------------------------------------*/

-- View all policies on our table
SELECT 
    POLICY_KIND,
    POLICY_NAME,
    POLICY_SCHEMA,
    REF_COLUMN_NAME
FROM TABLE(
    INFORMATION_SCHEMA.POLICY_REFERENCES(
        REF_ENTITY_NAME => 'AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP',
        REF_ENTITY_DOMAIN => 'TABLE'
    )
);

-- Test complete governance as AMS_DATA_ANALYST
USE ROLE AMS_DATA_ANALYST;
-- Run a typical analyst query
SELECT 
    JOBSTATUS,
    APPLICATIONSTATUS,
    CANDIDATECOUNTRY,
    COUNT(*) as applications,
    COUNT(DISTINCT CANDIDATEID) as unique_candidates
FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
WHERE APPLICATIONSTATUS IN ('Hired', 'Offered', 'In Process') and REASONOFREJECTION NOT ILIKE ('%REJECTED%')
GROUP BY JOBSTATUS, APPLICATIONSTATUS, CANDIDATECOUNTRY
ORDER BY applications DESC;

-- Reset to ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;
-- Run a typical analyst query
SELECT 
    JOBSTATUS,
    APPLICATIONSTATUS,
    CANDIDATECOUNTRY,
    COUNT(*) as applications,
    COUNT(DISTINCT CANDIDATEID) as unique_candidates
FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
WHERE APPLICATIONSTATUS IN ('Hired', 'Offered', 'In Process') and REASONOFREJECTION NOT ILIKE ('%REJECTED%')
GROUP BY JOBSTATUS, APPLICATIONSTATUS, CANDIDATECOUNTRY
ORDER BY applications DESC;


USE ROLE ACCOUNTADMIN;
-- masking policy propagation to views and downstream datasets
CREATE OR REPLACE VIEW  AMS_LABS.DATA_ENGINEERING.VW_TA_APPLICATION_DATA_BRONZE_DEDUP AS
SELECT 
    JOBSTATUS,
    APPLICATIONSTATUS,
    CANDIDATECOUNTRY,
    COUNT(*) as applications,
    COUNT(DISTINCT CANDIDATEID) as unique_candidates
FROM AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
WHERE APPLICATIONSTATUS IN ('Hired', 'Offered', 'In Process') and REASONOFREJECTION NOT ILIKE ('%REJECTED%')
GROUP BY JOBSTATUS, APPLICATIONSTATUS, CANDIDATECOUNTRY
ORDER BY applications DESC;


GRANT SELECT ON VIEW AMS_LABS.DATA_ENGINEERING.VW_TA_APPLICATION_DATA_BRONZE_DEDUP TO ROLE AMS_DATA_ANALYST;
USE ROLE AMS_DATA_ANALYST;

SELECT *
FROM AMS_LABS.DATA_ENGINEERING.VW_TA_APPLICATION_DATA_BRONZE_DEDUP
LIMIT 100;

USE ROLE ACCOUNTADMIN;
SELECT *
FROM AMS_LABS.DATA_ENGINEERING.VW_TA_APPLICATION_DATA_BRONZE_DEDUP
LIMIT 100;

/*----------------------------------------------------------------------------------
-- CLEANUP (Optional - Uncomment to remove all policies)
USE ROLE ACCOUNTADMIN;
-- Remove projection policies
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    MODIFY COLUMN EMPLOYEENUMBER UNSET PROJECTION POLICY;
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    MODIFY COLUMN REASONOFREJECTION UNSET PROJECTION POLICY;
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    MODIFY COLUMN CURRENTPOSITION UNSET PROJECTION POLICY;

-- Remove aggregation policy
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    UNSET AGGREGATION POLICY;

-- Remove row access policy
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    DROP ROW ACCESS POLICY AMS_LABS.GOVERNANCE.CANDIDATE_LOCATION_POLICY;

-- Remove masking policies
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    MODIFY COLUMN CANDIDATEID UNSET MASKING POLICY;
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    MODIFY COLUMN CANDIDATECITY UNSET MASKING POLICY;
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    MODIFY COLUMN CANDIDATELOCATION UNSET MASKING POLICY;
ALTER TABLE AMS_LABS.DATA_ENGINEERING.TA_APPLICATION_DATA_BRONZE_DEDUP
    MODIFY COLUMN EMPLOYEENUMBER UNSET MASKING POLICY;

-- Drop policies
DROP MASKING POLICY IF EXISTS AMS_LABS.GOVERNANCE.MASK_CANDIDATE_ID;
DROP MASKING POLICY IF EXISTS AMS_LABS.GOVERNANCE.MASK_LOCATION;
DROP MASKING POLICY IF EXISTS AMS_LABS.GOVERNANCE.MASK_EMPLOYEE_NUMBER;
DROP MASKING POLICY IF EXISTS AMS_LABS.GOVERNANCE.PII_TAG_MASK;
DROP ROW ACCESS POLICY IF EXISTS AMS_LABS.GOVERNANCE.CANDIDATE_LOCATION_POLICY;
DROP AGGREGATION POLICY IF EXISTS AMS_LABS.GOVERNANCE.MIN_GROUP_SIZE_POLICY;
DROP PROJECTION POLICY IF EXISTS AMS_LABS.GOVERNANCE.HIDE_SENSITIVE_COLUMNS;

-- Drop tags
DROP TAG IF EXISTS AMS_LABS.GOVERNANCE.COST_CENTER;
DROP TAG IF EXISTS AMS_LABS.GOVERNANCE.DATA_SENSITIVITY;
DROP TAG IF EXISTS AMS_LABS.GOVERNANCE.PII_TYPE;
DROP TAG IF EXISTS AMS_LABS.GOVERNANCE.HR_DATA_CATEGORY;
DROP TAG IF EXISTS AMS_LABS.GOVERNANCE.PII_COLUMN;

-- Drop mapping table
DROP TABLE IF EXISTS AMS_LABS.GOVERNANCE.LOCATION_ACCESS_MAP;
DROP VIEW IF EXISTS AMS_LABS.DATA_ENGINEERING.VW_TA_APPLICATION_DATA_BRONZE_DEDUP;
-- Drop schema
DROP SCHEMA IF EXISTS AMS_LABS.GOVERNANCE;
----------------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------------
Differential Privacy [OPTIONAL]

Differential privacy is a widely recognized standard for data privacy that limits the risk that a user could leak sensitive information from a sensitive dataset. It protects the identity and information of individual entities in the data, for example, people, corporations, and locations. While each individual entity’s information is protected, differential privacy still lets data consumers learn statistics, trends, and behaviors about groups of individuals.

Differential privacy provides strong protection against re-identification that is particularly effective against targeted privacy attacks. This protection lets you share sensitive data across teams, outside of your organization, and across regulatory lines. Differential privacy mitigates the increased re-identification risk associated with joining two sensitive datasets, adding new fields, unmasking existing fields, or providing individual rows instead of pre-aggregated data.

Unlike other privacy methodologies, differential privacy does the following:

- Protects against targeted privacy attacks, for example differencing and amplification attacks.
- Quantifies and manages the trade-off between privacy and utility, that is, controls how much non-sensitive information data consumers can learn about the data.
- Removes the need for data providers to transform sensitive data to reduce re-identification risk (for example, masking, redaction, and bucketing).
----------------------------------------------------------------------------------*/
-- --https://docs.snowflake.com/en/user-guide/tutorials/diff-privacy#introduction
-- USE ROLE ACCOUNTADMIN;
-- CREATE ROLE dp_tutorial_analyst;

-- -- You can find your own user name by running "SELECT CURRENT_USER();"
-- GRANT ROLE dp_tutorial_analyst TO USER hchen;

-- CREATE OR REPLACE WAREHOUSE dp_tutorial_wh;
-- GRANT USAGE ON WAREHOUSE dp_tutorial_wh TO ROLE dp_tutorial_analyst;

-- -- Create the table
-- CREATE OR REPLACE DATABASE dp_db;
-- CREATE OR REPLACE SCHEMA dp_db.dp_schema;
-- USE SCHEMA dp_db.dp_schema;
-- CREATE OR REPLACE TABLE dp_tutorial_diabetes_survey (
--   patient_id TEXT,
--   is_smoker BOOLEAN,
--   has_difficulty_walking BOOLEAN,
--   gender TEXT,
--   age INT,
--   has_diabetes BOOLEAN,
--   income_code INT);

-- -- Populate the table
-- INSERT INTO dp_db.dp_schema.dp_tutorial_diabetes_survey
-- VALUES
-- ('ID-23493', TRUE, FALSE, 'male', 39, TRUE, 2),
-- ('ID-00923', FALSE, FALSE, 'female', 82, TRUE, 5),
-- ('ID-24020', FALSE, FALSE, 'male', 69, FALSE, 8),
-- ('ID-92848', TRUE, TRUE, 'other', 75, FALSE, 3),
-- ('ID-62937', FALSE, FALSE, 'male', 46, TRUE, 5);

-- -- Define a privacy policy. Use default budget, budget window, max budget per aggregate.
-- CREATE OR REPLACE DATABASE policy_db;
-- CREATE OR REPLACE SCHEMA policy_db.diff_priv_policies;
-- CREATE OR REPLACE PRIVACY POLICY policy_db.diff_priv_policies.patients_policy AS () RETURNS privacy_budget ->
--   CASE
--     WHEN CURRENT_ROLE() = 'ACCOUNTADMIN' THEN no_privacy_policy()
--     WHEN CURRENT_ROLE() IN ('DP_TUTORIAL_ANALYST')
--       THEN privacy_budget(budget_name => 'clinical_analysts')
--     ELSE privacy_budget(budget_name => 'default')
-- END;

-- -- Assign the privacy policy to the table.
-- ALTER TABLE dp_db.dp_schema.dp_tutorial_diabetes_survey
-- ADD PRIVACY POLICY policy_db.diff_priv_policies.patients_policy ENTITY KEY (patient_id);

-- -- Define privacy domains.
-- ALTER TABLE dp_db.dp_schema.dp_tutorial_diabetes_survey ALTER (
-- COLUMN gender SET PRIVACY DOMAIN IN ('female', 'male', 'other'),
-- COLUMN age SET PRIVACY DOMAIN BETWEEN (0, 90),
-- COLUMN income_code SET PRIVACY DOMAIN BETWEEN (1, 8)
-- );

-- GRANT USAGE ON DATABASE dp_db TO ROLE dp_tutorial_analyst;
-- GRANT USAGE ON SCHEMA dp_db.dp_schema TO ROLE dp_tutorial_analyst;
-- GRANT SELECT
--   ON TABLE dp_db.dp_schema.dp_tutorial_diabetes_survey
--   TO ROLE dp_tutorial_analyst;

--   USE ROLE ACCOUNTADMIN;
-- SELECT * FROM dp_db.dp_schema.dp_tutorial_diabetes_survey;

-- USE ROLE dp_tutorial_analyst;
-- SELECT * FROM dp_db.dp_schema.dp_tutorial_diabetes_survey;

-- -- Run a basic query without DP.
-- USE ROLE ACCOUNTADMIN;
-- SELECT COUNT(DISTINCT patient_id)
--   FROM dp_db.dp_schema.dp_tutorial_diabetes_survey
--   WHERE income_code = 5;

--   USE ROLE dp_tutorial_analyst;
-- SELECT COUNT(DISTINCT patient_id)
--   FROM dp_db.dp_schema.dp_tutorial_diabetes_survey
--   WHERE income_code = 5;

-- -- Retrieve noise interval for the previous query.
-- USE ROLE dp_tutorial_analyst;
-- SELECT COUNT(DISTINCT patient_id) as c,
--   DP_INTERVAL_LOW(c) as LOW,
--   DP_INTERVAL_HIGH(c) as HIGH
--   FROM dp_db.dp_schema.dp_tutorial_diabetes_survey
--   WHERE income_code = 5;
