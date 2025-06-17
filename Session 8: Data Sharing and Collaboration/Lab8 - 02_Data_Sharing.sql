/***************************************************************************************************
| A | M | S |   | L | A | B | S |   | C | U | S | T | O | M | E | R |   | D | E | M | O |

Demo:         AMS Labs Secure Data Sharing with Policies
Create Date:  2025-06-15
Purpose:      Demonstrate Snowflake secure data sharing with row-level security and dynamic masking
Data Source:  AMS_LABS.DATA_ENGINEERING.GOLD_CUSTOMER_HIRING_METRICS
Customer:     Secure Data Collaboration and Governance
****************************************************************************************************

/*----------------------------------------------------------------------------------
This script demonstrates Snowflake's secure data sharing capabilities with advanced
governance controls. It implements row-level security policies and dynamic masking
to ensure sensitive data is protected while enabling collaboration between
different accounts and organizations.

Key Concepts:
  • Row Access Policies: Account-specific data filtering for secure sharing
  • Dynamic Data Masking: Real-time data protection during sharing
  • Secure Data Sharing: Cross-account data collaboration with governance
  • Account-Level Security: Conditional access based on consuming account
  • Data Governance: Policy management and compliance controls
----------------------------------------------------------------------------------*/

use role accountadmin;

use database ams_labs;
use schema data_engineering;


CREATE OR REPLACE ROW ACCESS POLICY AMS_LABS.DATA_ENGINEERING.GOLD_CUSTOMER_HIRING_METRICS_RLS
AS (sourcing_channel_name_clean VARCHAR) RETURNS BOOLEAN ->
    CASE 
        WHEN CURRENT_ACCOUNT() = '<account_locator>' THEN 
            sourcing_channel_name_clean = 'TalentLink'
        ELSE 
            TRUE  -- Allow all rows for other accounts
    END;

CREATE OR REPLACE MASKING POLICY AMS_LABS.DATA_ENGINEERING.CANDIDATE_COUNTRY_MASK
AS (val VARCHAR) RETURNS VARCHAR ->
    CASE 
        WHEN CURRENT_ACCOUNT() = '<account_locator>' THEN 
            '***MASKED***'
        ELSE 
            val  -- Show actual value for other accounts
    END;

-- Apply the row-level security policy
ALTER TABLE AMS_LABS.DATA_ENGINEERING.GOLD_CUSTOMER_HIRING_METRICS 
ADD ROW ACCESS POLICY AMS_LABS.DATA_ENGINEERING.GOLD_CUSTOMER_HIRING_METRICS_RLS 
ON (sourcing_channel_name_clean);

-- Apply the dynamic masking policy to the candidate country column
ALTER TABLE AMS_LABS.DATA_ENGINEERING.GOLD_CUSTOMER_HIRING_METRICS 
MODIFY COLUMN candidate_country_clean 
SET MASKING POLICY AMS_LABS.DATA_ENGINEERING.CANDIDATE_COUNTRY_MASK;


-- Clean-ups
-- 1. Remove the row-level security policy from the table
ALTER TABLE AMS_LABS.DATA_ENGINEERING.GOLD_CUSTOMER_HIRING_METRICS 
DROP ROW ACCESS POLICY AMS_LABS.DATA_ENGINEERING.GOLD_CUSTOMER_HIRING_METRICS_RLS;

-- 2. Remove the dynamic masking policy from the column
ALTER TABLE AMS_LABS.DATA_ENGINEERING.GOLD_CUSTOMER_HIRING_METRICS 
MODIFY COLUMN candidate_country_clean 
UNSET MASKING POLICY;

-- 3. Drop the masking policy
DROP MASKING POLICY IF EXISTS AMS_LABS.DATA_ENGINEERING.CANDIDATE_COUNTRY_MASK;

-- 4. Drop the row access policy
DROP ROW ACCESS POLICY IF EXISTS AMS_LABS.DATA_ENGINEERING.GOLD_CUSTOMER_HIRING_METRICS_RLS;