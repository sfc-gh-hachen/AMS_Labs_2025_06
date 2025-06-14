-- View the list of the accounts in your organization
-- Note the organization name and account name for each account for which you are enabling replication

USE ROLE ORGADMIN;

SHOW ACCOUNTS;
-- Run this code in your PRIMARY Account

-- Use the same name and email for all accounts
set email_var = 'harley.chen@snowflake.com';
-- Use the same password for users in all accounts
set pwd_var = 'ejh3rvn@QBY1cye-prf';

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

  
SHOW ACCOUNTS;
-- https://docs.snowflake.com/en/user-guide/account-replication-config#prerequisite-enable-replication-for-accounts-in-the-organization
SELECT SYSTEM$GLOBAL_ACCOUNT_SET_PARAMETER('MJDLKQJ.OWB41017', 'ENABLE_ACCOUNT_DATABASE_REPLICATION', 'true');
SELECT SYSTEM$GLOBAL_ACCOUNT_SET_PARAMETER('MJDLKQJ.HOL_ACCOUNT2', 'ENABLE_ACCOUNT_DATABASE_REPLICATION', 'true');

USE ROLE ACCOUNTADMIN;
CREATE FAILOVER GROUP myfg
  OBJECT_TYPES = USERS, ROLES, WAREHOUSES, RESOURCE MONITORS, DATABASES
  ALLOWED_DATABASES = HR_ANALYTICS, AMS_LABS
  ALLOWED_ACCOUNTS = MJDLKQJ.HOL_ACCOUNT2
  REPLICATION_SCHEDULE = '60 MINUTE';

-- Run the following in your target/replication account
-- CREATE FAILOVER GROUP myfg
--   AS REPLICA OF MJDLKQJ.OWB41017.myfg;
