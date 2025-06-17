/***************************************************************************************************
| A | M | S |   | L | A | B | S |   | C | U | S | T | O | M | E | R |   | D | E | M | O |

Demo:         AMS Labs Environment Setup and Configuration
Create Date:  2025-06-15
Purpose:      Complete environment setup for AMS Labs workshop sessions
Data Source:  GitHub Repository Integration with Multi-Session Data Sources
Customer:     AMS Labs Workshop - Comprehensive Snowflake Capabilities Demo
****************************************************************************************************

/*----------------------------------------------------------------------------------
This script provides comprehensive environment setup for the AMS Labs workshop 
series covering data engineering, security, governance, machine learning, AI, 
and integration capabilities. It establishes the foundational infrastructure 
including databases, warehouses, stages, roles, Git integration, and Streamlit 
applications across all workshop sessions.

Key Concepts:
  • Environment Setup: Database, schema, and warehouse configuration
  • Security & Access: Role-based access control and Document AI privileges
  • Git Integration: Repository connection for automated file management
  • Data Staging: Internal stages for structured and unstructured data
  • Streamlit Applications: Multi-session dashboard and analytics deployment
  • Resource Management: Warehouse sizing and auto-suspension policies
----------------------------------------------------------------------------------*/

--SETUP
use role accountadmin;
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';
create database if not exists AMS_LABS;
create schema if not exists data_engineering;
use database ams_labs;
use schema data_engineering;
create OR REPLACE WAREHOUSE COMPUTE_WH
WAREHOUSE_SIZE = XSMALL
AUTO_SUSPEND = 60
RESOURCE_CONSTRAINT = 'STANDARD_GEN_2';
-- WAREHOUSE_TYPE = {STANDARD | 'SNOWPARK-OPTIMIZED'}: Determines whether the warehouse uses general-purpose compute or is ; for Snowpark workloads.
-- WAREHOUSE_SIZE = {XSMALL | SMALL | ... | X6LARGE}
-- RESOURCE_CONSTRAINT = {STANDARD_GEN_1 | STANDARD_GEN_2 | MEMORY_1X | MEMORY_1X_x86 | MEMORY_16X | MEMORY_16X_x86 | MEMORY_64X | MEMORY_64X_x86}: Assigns a predefined constraint class to limit architecture and resource use.
-- MULTI-CLUSTER SETTINGS
    -- MAX_CLUSTER_COUNT = <num>: Defines the maximum number of clusters for multi-cluster warehouses to scale out under heavy load.
    -- MIN_CLUSTER_COUNT = <num>: Defines the minimum number of clusters for multi-cluster warehouses to maintain under light load.
    -- SCALING_POLICY = {STANDARD | ECONOMY}: Chooses between faster but costlier scaling (STANDARD) or slower, more cost-efficient scaling (ECONOMY).
-- RESOURCE_MONITOR = <monitor_name>: Associates a resource monitor to track and enforce credit usage for the warehouse.
-- COMMENT = '<string_literal>': Adds a descriptive note to the warehouse metadata for documentation.
-- ENABLE_QUERY_ACCELERATION = {TRUE | FALSE}: Toggles Snowflake's query acceleration service for faster query processing.
-- WITH TAG (<tag_name> = '<tag_value>' [, ...]): Assigns one or more user-defined metadata tags to the warehouse.
-- STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = <num>: Sets the maximum time a statement can wait in the queue before timing out.
-- STATEMENT_TIMEOUT_IN_SECONDS = <num>: Defines the maximum execution time for a statement before it is automatically aborted.;

create OR REPLACE WAREHOUSE doc_ai_wh
WAREHOUSE_SIZE = XSMALL
AUTO_SUSPEND = 60;

create OR REPLACE WAREHOUSE Streamlit_wh
WAREHOUSE_SIZE = XSMALL
AUTO_SUSPEND = 60
RESOURCE_CONSTRAINT = 'STANDARD_GEN_2';

create OR REPLACE WAREHOUSE cortex_search_wh
WAREHOUSE_SIZE = XSMALL
AUTO_SUSPEND = 60;

create OR REPLACE WAREHOUSE notebook_wh
WAREHOUSE_SIZE = XSMALL
AUTO_SUSPEND = 60;

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
  -- ALLOWED_AUTHENTICATION_SECRETS = (myco_git_secret)
  ENABLED = TRUE;

DESC INTEGRATION git_api_integration;

-- Create Git repository integration
CREATE OR REPLACE GIT REPOSITORY AMS_LABS_REPO
    API_INTEGRATION = git_api_integration
  ORIGIN = 'https://github.com/sfc-gh-hachen/AMS_Labs_2025_06.git'
  GIT_CREDENTIALS = NULL;

-- List files to verify the repository connection
SHOW GIT BRANCHES IN GIT REPOSITORY AMS_LABS_REPO;

-- Refresh the Git repository clone
ALTER GIT REPOSITORY AMS_LABS_REPO FETCH;
LS @AMS_LABS_REPO/branches/main;

-- Copy the structured resumes into the structured resumes stage directly from the repository
COPY FILES
  INTO @STRUCTURED_RESUMES
  FROM '@ams_labs_repo/branches/main/Session 1: Ingestion and Engineering/Dataset/Structured Resumes/'
  PATTERN='.*\.json';

-- Copy all CV files from Session 6 into the CVS stage, maintaining folder structure
COPY FILES
  INTO '@CVS/Business Analyst/'
  FROM '@ams_labs_repo/branches/main/Session 6: Snowflake Cortex AI/CVs/Business Analyst/'
  PATTERN='.*\.docx';

COPY FILES
  INTO '@CVS/Business Relationship Manager/'
  FROM '@ams_labs_repo/branches/main/Session 6: Snowflake Cortex AI/CVs/Business Relationship Manager/'
  PATTERN='.*\.docx';

COPY FILES
  INTO '@CVS/Customer Service/'
  FROM '@ams_labs_repo/branches/main/Session 6: Snowflake Cortex AI/CVs/Customer Service/'
  PATTERN='.*\.docx';

COPY FILES
  INTO '@CVS/Data Scientist/'
  FROM '@ams_labs_repo/branches/main/Session 6: Snowflake Cortex AI/CVs/Data Scientist/'
  PATTERN='.*\.docx';

COPY FILES
  INTO '@CVS/Head Of Compliance/'
  FROM '@ams_labs_repo/branches/main/Session 6: Snowflake Cortex AI/CVs/Head Of Compliance/'
  PATTERN='.*\.docx';

COPY FILES
  INTO '@CVS/Investment Banker/'
  FROM '@ams_labs_repo/branches/main/Session 6: Snowflake Cortex AI/CVs/Investment Banker/'
  PATTERN='.*\.docx';

COPY FILES
  INTO '@CVS/PEP Risk Analyst/'
  FROM '@ams_labs_repo/branches/main/Session 6: Snowflake Cortex AI/CVs/PEP Risk Analyst/'
  PATTERN='.*\.docx';

COPY FILES
  INTO '@CVS/Private Banking Execs/'
  FROM '@ams_labs_repo/branches/main/Session 6: Snowflake Cortex AI/CVs/Private Banking Execs/'
  PATTERN='.*\.docx';

COPY FILES
  INTO '@CVS/Product Managers/'
  FROM '@ams_labs_repo/branches/main/Session 6: Snowflake Cortex AI/CVs/Product Managers/'
  PATTERN='.*\.docx';

COPY FILES
  INTO '@CVS/Risk Assessment Assoc/'
  FROM '@ams_labs_repo/branches/main/Session 6: Snowflake Cortex AI/CVs/Risk Assessment Assoc/'
  PATTERN='.*\.docx';