--SETUP
use role accountadmin;
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';
create database if not exists AMS_LABS;
create schema if not exists data_engineering;
use database ams_labs;
use schema data_engineering;

create warehouse doc_ai_wh
WAREHOUSE_SIZE = XSMALL
AUTO_SUSPEND = 60;

create warehouse Streamlit_wh
WAREHOUSE_SIZE = XSMALL
AUTO_SUSPEND = 60;

create warehouse cortex_search_wh
WAREHOUSE_SIZE = XSMALL
AUTO_SUSPEND = 60;

create warehouse notebook_wh;
-- WAREHOUSE_TYPE = {STANDARD | 'SNOWPARK-OPTIMIZED'}: Determines whether the warehouse uses general-purpose compute or is ; for Snowpark workloads.
-- WAREHOUSE_SIZE = {XSMALL | SMALL | ... | X6LARGE}
-- RESOURCE_CONSTRAINT = {STANDARD_GEN_1 | STANDARD_GEN_2 | MEMORY_1X | MEMORY_1X_x86 | MEMORY_16X | MEMORY_16X_x86 | MEMORY_64X | MEMORY_64X_x86}: Assigns a predefined constraint class to limit architecture and resource use.
-- MULTI-CLUSTER SETTINGS
    -- MAX_CLUSTER_COUNT = <num>: Defines the maximum number of clusters for multi-cluster warehouses to scale out under heavy load.
    -- MIN_CLUSTER_COUNT = <num>: Defines the minimum number of clusters for multi-cluster warehouses to maintain under light load.
    -- SCALING_POLICY = {STANDARD | ECONOMY}: Chooses between faster but costlier scaling (STANDARD) or slower, more cost-efficient scaling (ECONOMY).
-- RESOURCE_MONITOR = <monitor_name>: Associates a resource monitor to track and enforce credit usage for the warehouse.
-- COMMENT = '<string_literal>': Adds a descriptive note to the warehouse metadata for documentation.
-- ENABLE_QUERY_ACCELERATION = {TRUE | FALSE}: Toggles Snowflake’s query acceleration service for faster query processing.
-- WITH TAG (<tag_name> = '<tag_value>' [, ...]): Assigns one or more user-defined metadata tags to the warehouse.
-- STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = <num>: Sets the maximum time a statement can wait in the queue before timing out.
-- STATEMENT_TIMEOUT_IN_SECONDS = <num>: Defines the maximum execution time for a statement before it is automatically aborted.;

USE WAREHOUSE COMPUTE_WH;

CREATE STAGE TA_APPLICATION_DATA 
	DIRECTORY = ( ENABLE = true ) 
	ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' );
CREATE STAGE STRUCTURED_RESUMES 
	DIRECTORY = ( ENABLE = true ) 
	ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' );
CREATE STAGE CVS
    DIRECTORY = ( ENABLE = true ) 
    ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' );
    
-- CREATE STAGE MOCK_INTERVIEW_AUDIOS
--     DIRECTORY = ( ENABLE = true ) 
--     ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' );

-- Document AI privileges
USE ROLE ACCOUNTADMIN;

CREATE ROLE doc_ai_role;
GRANT DATABASE ROLE SNOWFLAKE.DOCUMENT_INTELLIGENCE_CREATOR TO ROLE doc_ai_role;

--To grant warehouse usage and operating privileges to the doc_ai_role role, run the following commands:
GRANT USAGE, OPERATE ON WAREHOUSE doc_ai_wh TO ROLE doc_ai_role;

--To ensure that the doc_ai_role role can use the database and the schema, run the following commands:
GRANT USAGE ON DATABASE AMS_LABS TO ROLE doc_ai_role;
GRANT USAGE ON SCHEMA AMS_LABS.data_engineering TO ROLE doc_ai_role;

--To ensure that the doc_ai_role role can create a stage to store the documents for extraction, run the following commands:
GRANT CREATE STAGE ON SCHEMA AMS_LABS.data_engineering TO ROLE doc_ai_role;

--To ensure that the doc_ai_role role can create model builds (instances of the DOCUMENT_INTELLIGENCE class), run the following commands:
GRANT CREATE SNOWFLAKE.ML.DOCUMENT_INTELLIGENCE ON SCHEMA AMS_LABS.data_engineering TO ROLE doc_ai_role;
GRANT CREATE MODEL ON SCHEMA AMS_LABS.data_engineering TO ROLE doc_ai_role;

--To ensure that the doc_ai_role role can create processing pipelines, run the following commands:
GRANT CREATE STREAM, CREATE TABLE, CREATE TASK, CREATE VIEW ON SCHEMA AMS_LABS.data_engineering TO ROLE doc_ai_role;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE doc_ai_role;


-- Set up SQL variable for current user and grant roles
SET MY_USER_ID = CURRENT_USER();
grant role doc_ai_role to user identifier($MY_USER_ID);


CREATE OR REPLACE API INTEGRATION git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com')
  ALLOWED_AUTHENTICATION_SECRETS = (myco_git_secret)
  ENABLED = TRUE;

DESC INTEGRATION git_api_integration;
