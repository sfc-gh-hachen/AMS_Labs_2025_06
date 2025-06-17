use role accountadmin;
--ability to run across cloud if claude is not in your region:
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = ' ANY_REGION' ;
-- Create roles create role Snowflake_intelligence_admin_rl;
--need to add ability to create databases
GRANT CREATE DATABASE ON ACCOUNT TO ROLE Snowflake_intelligence_admin_rl;
-- Warehouse that is going to be used for Cortex Search Service creation as well as query execution.
create warehouse Snowflake_ intelligence_wh with warehouse_size =
'X-SMALL' ;
grant usage on warehouse Snowflake_intelligence wh to role Snowflake_intelligence_admin_rl;
-- Create a database. This will hold configuration and other objects to support Snowflake Intelligence.
create database Snowflake_intelligence;
grant ownership on database Snowflake_intelligence to role Snowflake_intelligence_admin_rl;
-- Dynamically grant role 'Snowflake_intelligence_admin_rl' to the current user
DECLARE
sql_command STRING;
BEGIN
sql_command := 'GRANT ROLE Snowflake_intelligence_admin_rl TO USER
"' | I CURRENT_USER() || '";';
EXECUTE IMMEDIATE sql_command;
RETURN 'Role Snowflake_intelligence_admin_rl granted successfully to
user ' || CURRENT_USER() ;
END;
-- Set up stages and tables for configuration.

use role Snowflake_intelligence_admin_rl;
use database Snowflake_intelligence;
-- Set up a temp schema for file upload (only temporary stages will be created here).
create or replace schema Snowflake_intelligence. temp;
grant usage on schema Snowflake_intelligence.temp to role public;
-- OPTIONAL: Set up stages and tables for configuration - you can have your semantic models be anywhere else, just make sure that the users have grants to them
create schema if not exists config;
use schema config;
create stage semantic_models encryption = (type = 'SNOWFLAKE_SSE') ;


use role Snowflake_intelligence_admin_rl;
create schema if not exists Snowflake_intelligence.agents;
-- Make SI agents in general discoverable to everyone.
grant usage on schema Snowflake_intelligence.agents to role public;
CREATE OR REPLACE ROW ACCESS POLICY
Snowflake_intelligence.agents.agent_policy
AS (grantee_roles ARRAY) RETURNS BOOLEAN ->
ARRAY_SIZE(FILTER(grantee_roles:: ARRAY(VARCHAR), role ->
is_role_in_session (role))) <> 0;
-- Create an agent config table. Multiple tables can be created to give granular
-- UPDATE/INSERT permissions to different roles. create or replace table Snowflake_intelligence.agents.config (
agent_name varchar not null, agent_description varchar, grantee_roles array not null, tools array, tool-resources object, tool-choice object, response_instruction varchar, sample_questions array,
constraint pk_agent_name primary key (agent_name)
with row access policy Snowflake_intelligence.agents.agent_policy on (grantee_roles) ;
grant select on table Snowflake_intelligence.agents.config to role public;