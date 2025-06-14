use role accountadmin;

use database ams_labs;
use schema data_engineering;


CREATE OR REPLACE ROW ACCESS POLICY AMS_LABS.DATA_ENGINEERING.GOLD_CUSTOMER_HIRING_METRICS_RLS
AS (sourcing_channel_name_clean VARCHAR) RETURNS BOOLEAN ->
    CASE 
        WHEN CURRENT_ACCOUNT() = 'SCB92838' THEN 
            sourcing_channel_name_clean = 'TalentLink'
        ELSE 
            TRUE  -- Allow all rows for other accounts
    END;

CREATE OR REPLACE MASKING POLICY AMS_LABS.DATA_ENGINEERING.CANDIDATE_COUNTRY_MASK
AS (val VARCHAR) RETURNS VARCHAR ->
    CASE 
        WHEN CURRENT_ACCOUNT() = 'SCB92838' THEN 
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