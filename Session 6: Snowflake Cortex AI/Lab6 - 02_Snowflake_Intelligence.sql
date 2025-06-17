-- Needed only during Private Preview
use role accountadmin;
use database ams_labs;
use schema data_engineering;
GRANT USAGE ON SCHEMA AMS_LABS.data_engineering TO ROLE Snowflake_intelligence_admin_rl;
GRANT USAGE ON DATABASE AMS_LABS                TO ROLE Snowflake_intelligence_admin_rl;
GRANT USAGE ON ALL SCHEMAS    IN DATABASE AMS_LABS  TO ROLE Snowflake_intelligence_admin_rl;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE AMS_LABS  TO ROLE Snowflake_intelligence_admin_rl;
-- Existing objects
GRANT USAGE ON SCHEMA AMS_LABS.data_engineering TO ROLE Snowflake_intelligence_admin_rl;
GRANT SELECT ON ALL TABLES      IN DATABASE AMS_LABS TO ROLE Snowflake_intelligence_admin_rl;
GRANT ALL PRIVILEGES ON ALL TABLES      IN DATABASE AMS_LABS TO ROLE Snowflake_intelligence_admin_rl;
GRANT ALL PRIVILEGES ON ALL VIEWS       IN DATABASE AMS_LABS TO ROLE Snowflake_intelligence_admin_rl;
GRANT USAGE ON CORTEX SEARCH SERVICE <your cortex search name> TO ROLE Snowflake_intelligence_admin_rl;
GRANT REFERENCES, SELECT ON FUTURE SEMANTIC VIEWS IN SCHEMA data_engineering TO ROLE Snowflake_intelligence_admin_rl;
GRANT REFERENCES, SELECT ON SEMANTIC VIEW <your semantic view name> TO ROLE Snowflake_intelligence_admin_rl;
use role SNOWFLAKE_INTELLIGENCE_ADMIN_RL;
grant select on table snowflake_intelligence.agents.config to role public;
grant update on table snowflake_intelligence.agents.config to role public;

--Then, I would recommend changing the default role to Snowflake_intelligence_admin_rl, you can do this by running the following command, 
alter user <your user name> set default_role = Snowflake_intelligence_admin_rl;