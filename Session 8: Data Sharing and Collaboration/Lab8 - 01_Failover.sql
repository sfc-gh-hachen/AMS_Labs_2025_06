/***************************************************************************************************
| A | M | S |   | L | A | B | S |   | C | U | S | T | O | M | E | R |   | D | E | M | O |

Demo:         AMS Labs Account Failover and Replication
Create Date:  2025-06-15
Purpose:      Demonstrate Snowflake account failover and database replication for business continuity
Data Source:  AMS_LABS, HR_ANALYTICS (replicated databases)
Customer:     Business Continuity and Disaster Recovery
****************************************************************************************************

/*----------------------------------------------------------------------------------
This script demonstrates Snowflake's account failover and database replication
capabilities for business continuity. It covers multi-account setup, failover
group configuration, and automated replication scheduling to ensure data
availability and disaster recovery across different regions.

Key Concepts:
  • Account Replication: Multi-account database synchronization
  • Failover Groups: Automated failover configuration for business continuity
  • Cross-Region Replication: Geographic distribution of data for disaster recovery
  • Organization Management: Multi-account governance and administration
  • Business Continuity: Automated backup and recovery processes
----------------------------------------------------------------------------------*/

-- View the list of the accounts in your organization
-- Note the organization name and account name for each account for which you are enabling replication

USE ROLE ORGADMIN;

SHOW ACCOUNTS;
-- Run this code in your PRIMARY Account

-- Use the same name and email for all accounts
set email_var = 'harley.chen@snowflake.com';
-- Use the same password for users in all accounts
set pwd_var = '<password>';

-- Create a secondary account in the same region (default!):
USE ROLE orgadmin;

CREATE ACCOUNT hol_account1
  admin_name = datashare_admin
  admin_password = $pwd_var
  email = $email_var
  must_change_password = false
  edition = BUSINESS_CRITICAL;

CREATE ACCOUNT hol_account2
  admin_name = failover_admin
  admin_password = $pwd_var
  email = $email_var
  must_change_password = false
  edition = BUSINESS_CRITICAL
  region = azure_westeurope;
-- https://docs.snowflake.com/en/user-guide/admin-account-identifier#label-snowflake-region-ids
  
SHOW ACCOUNTS;
-- https://docs.snowflake.com/en/user-guide/account-replication-config#prerequisite-enable-replication-for-accounts-in-the-organization
SELECT SYSTEM$GLOBAL_ACCOUNT_SET_PARAMETER('<org_name>.<current_account_id>', 'ENABLE_ACCOUNT_DATABASE_REPLICATION', 'true');
SELECT SYSTEM$GLOBAL_ACCOUNT_SET_PARAMETER('<org_name>.HOL_ACCOUNT2', 'ENABLE_ACCOUNT_DATABASE_REPLICATION', 'true');

USE ROLE ACCOUNTADMIN;
CREATE FAILOVER GROUP myfg
  OBJECT_TYPES = USERS, ROLES, WAREHOUSES, RESOURCE MONITORS, DATABASES
  ALLOWED_DATABASES = HR_ANALYTICS, AMS_LABS
  ALLOWED_ACCOUNTS = MJDLKQJ.HOL_ACCOUNT2
  REPLICATION_SCHEDULE = '60 MINUTE';

-- Run the following in your target/replication account
-- CREATE FAILOVER GROUP myfg
--   AS REPLICA OF MJDLKQJ.OWB41017.myfg;
