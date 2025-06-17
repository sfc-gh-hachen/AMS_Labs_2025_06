/***************************************************************************************************
| A | M | S |   | L | A | B | S |   | C | U | S | T | O | M | E | R |   | D | E | M | O |

Demo:         AMS Labs Semi-Structured Data Capabilities Showcase
Create Date:  2025-06-15
Purpose:      Comprehensive demonstration of Snowflake's semi-structured data processing capabilities
Data Source:  JSON Resume Files (Structured Resumes Dataset)
Customer:     Organizations with Heavy Semi-Structured Data Use Cases
****************************************************************************************************

/*----------------------------------------------------------------------------------
This script showcases Snowflake's powerful semi-structured data capabilities using
real-world JSON resume data. It demonstrates how Snowflake eliminates traditional
barriers to working with JSON, XML, and other semi-structured formats.

Key Capabilities Demonstrated:
  • Schema-on-Read: Query JSON directly without predefined schemas or ETL
  • VARIANT Data Type: Native storage and processing of any JSON structure
  • Dot Notation Access: Intuitive SQL-like navigation of nested objects
  • Array Processing: Safe indexing and flattening of JSON arrays
  • LATERAL FLATTEN: Convert nested arrays to relational rows for analytics
  • JSON Construction: Build new JSON objects from SQL data with OBJECT_CONSTRUCT
  • Schema Evolution: Handle new fields instantly without schema changes
  • AI Integration: Process semi-structured data with Cortex AI functions
  • Advanced Analytics: Complex aggregations and analytics on nested data
  • Full-Text Search: Search across entire JSON documents seamlessly
  • Data Marketplace: Enrich semi-structured data with external datasets

Business Value:
  • Faster Time-to-Insight: Query JSON immediately without complex ETL
  • Developer Productivity: SQL expertise applies directly to JSON data
  • Schema Flexibility: Adapt to changing data structures without downtime
  • Cost Efficiency: Single platform for structured and semi-structured analytics
  • AI-Ready: Built-in AI functions for data enhancement and transformation
----------------------------------------------------------------------------------*/

USE ROLE ACCOUNTADMIN;
USE DATABASE AMS_LABS;
USE SCHEMA DATA_ENGINEERING;
-- SNOWFLAKE CAPABILITY: Native JSON file format support
-- No external tools needed - Snowflake handles JSON parsing automatically
CREATE OR REPLACE FILE FORMAT JSON_FILE_FORMAT
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE
  SKIP_BYTE_ORDER_MARK = TRUE;

desc file format JSON_FILE_FORMAT;
  
-- SNOWFLAKE CAPABILITY: VARIANT data type stores JSON natively
-- No need to pre-define schema - JSON structure is preserved and queryable
CREATE OR REPLACE TABLE AMS_LABS.DATA_ENGINEERING.RESUME_JSON AS
SELECT 
  METADATA$FILENAME as FILE_NAME,                -- File lineage tracking
  METADATA$FILE_ROW_NUMBER as ROW_NUMBER,        -- Row-level lineage
  $1 as RAW_RESUME                               -- VARIANT column - stores any JSON structure
FROM @AMS_LABS.DATA_ENGINEERING.STRUCTURED_RESUMES
(FILE_FORMAT => AMS_LABS.DATA_ENGINEERING.JSON_FILE_FORMAT);

select top 10 * from AMS_LABS.DATA_ENGINEERING.RESUME_JSON;

DESC TABLE RESUME_JSON;

create view if not exists vw_resume_summary as 
-- SNOWFLAKE CAPABILITY: Schema-on-read with dot notation
-- No ETL required - query JSON structure directly with SQL
SELECT 
  r.FILE_NAME,
  r.ROW_NUMBER,
  -- JSON path navigation using colon notation (like table.column)
  r.RAW_RESUME:ResumeParserData.Name.FullName::STRING as full_name,
  r.RAW_RESUME:ResumeParserData.Name.FirstName::STRING as first_name,
  r.RAW_RESUME:ResumeParserData.Name.LastName::STRING as last_name,
  
  -- Array access with index notation [0] - safe even if array is empty
  r.RAW_RESUME:ResumeParserData.Email[0].EmailAddress::STRING as email,
  r.RAW_RESUME:ResumeParserData.PhoneNumber[0].FormattedNumber::STRING as phone,
  
  -- Nested object navigation through multiple levels
  r.RAW_RESUME:ResumeParserData.Address[0].City::STRING as city,
  r.RAW_RESUME:ResumeParserData.Address[0].State::STRING as state,
  r.RAW_RESUME:ResumeParserData.Address[0].Country::STRING as country,
  
  -- Direct field access at any nesting level
  r.RAW_RESUME:ResumeParserData.Summary::STRING as summary,
  r.RAW_RESUME:ResumeParserData.JobProfile::STRING as current_job_profile,
  r.RAW_RESUME:ResumeParserData.CurrentEmployer::STRING as current_employer,
  r.RAW_RESUME:ResumeParserData.Category::STRING as category,
  r.RAW_RESUME:ResumeParserData.WorkedPeriod.TotalExperienceInYear::STRING as total_experience_years

FROM AMS_LABS.DATA_ENGINEERING.RESUME_JSON r;

SELECT * FROM vw_resume_summary;

create view if not exists vw_resume_full_data as  
-- SNOWFLAKE CAPABILITY: Complex JSON transformation and construction
-- Build new JSON from flattened semi-structured data
WITH skills_aggregated AS (
  SELECT 
    r.FILE_NAME,
    r.ROW_NUMBER,
    -- OBJECT_CONSTRUCT: Build JSON objects from SQL data
    OBJECT_CONSTRUCT(
      'skills', 
      -- ARRAY_AGG: Convert rows back to JSON arrays
      ARRAY_AGG(
        OBJECT_CONSTRUCT(
          'skill_name', s.value:Skill::STRING,
          'skill_formatted_name', s.value:FormattedName::STRING,
          'skill_type', s.value:Type::STRING,
          'experience_months', s.value:ExperienceInMonths::NUMBER,
          'last_used', s.value:LastUsed::STRING,
          'evidence', s.value:Evidence::STRING
        )
      ) WITHIN GROUP (ORDER BY s.value:ExperienceInMonths::NUMBER DESC)
    ) as skills_json
  FROM AMS_LABS.DATA_ENGINEERING.RESUME_JSON r,
    -- LATERAL FLATTEN: Converts JSON arrays to rows for SQL processing
    LATERAL FLATTEN(input => r.RAW_RESUME:ResumeParserData.SegregatedSkill) s
  GROUP BY r.FILE_NAME, r.ROW_NUMBER
)

SELECT 
  r.FILE_NAME,
  r.ROW_NUMBER,
  -- Basic information
  r.RAW_RESUME:ResumeParserData.Name.FullName::STRING as full_name,
  r.RAW_RESUME:ResumeParserData.Name.FirstName::STRING as first_name,
  r.RAW_RESUME:ResumeParserData.Name.LastName::STRING as last_name,
  
  -- Contact information
  r.RAW_RESUME:ResumeParserData.Email[0].EmailAddress::STRING as email,
  r.RAW_RESUME:ResumeParserData.PhoneNumber[0].FormattedNumber::STRING as phone,
  
  -- Address information
  r.RAW_RESUME:ResumeParserData.Address[0].City::STRING as city,
  r.RAW_RESUME:ResumeParserData.Address[0].State::STRING as region,
  r.RAW_RESUME:ResumeParserData.Address[0].CountryCode.IsoAlpha2::STRING as country_code,
  r.RAW_RESUME:ResumeParserData.Address[0].FormattedAddress::STRING as full_address,
  
  -- Professional information
  r.RAW_RESUME:ResumeParserData.Summary::STRING as summary,
  r.RAW_RESUME:ResumeParserData.JobProfile::STRING as current_job_profile,
  r.RAW_RESUME:ResumeParserData.CurrentEmployer::STRING as current_employer,
  r.RAW_RESUME:ResumeParserData.Category::STRING as category,
  r.RAW_RESUME:ResumeParserData.WorkedPeriod.TotalExperienceInYear::STRING as total_experience_years,
  
  -- Skills as JSON object
  sa.skills_json,
  -- Website/Social (first one)
  r.RAW_RESUME:ResumeParserData.WebSite[0].Type::STRING as social_network,
  r.RAW_RESUME:ResumeParserData.WebSite[0].Url::STRING as social_url

FROM AMS_LABS.DATA_ENGINEERING.RESUME_JSON r
LEFT JOIN skills_aggregated sa ON r.FILE_NAME = sa.FILE_NAME AND r.ROW_NUMBER = sa.ROW_NUMBER;

SELECT * FROM AMS_LABS.DATA_ENGINEERING.VW_RESUME_FULL_DATA;

-- SNOWFLAKE CAPABILITY: AI Integration with Cortex
-- Process semi-structured data with AI directly in SQL
SELECT ai_complete(
    model => 'claude-4-sonnet',
    prompt => CONCAT('Read the provided column and return a json that contains First Name and Last Name only. Some rows has first name last name city all together. Output only json that has the 2 attributes. If any of the 2 attribute should be left empty, leave it as empty. Do not return anything else <data>', FULL_NAME, '</data>'),
    response_format => {
    'type': 'json',
    'schema': {
        'type': 'object',
        'properties': {
            'first_name': {'type': 'string'},
            'last_name': {'type': 'string'}
        },
        'required': ['first_name', 'last_name']
    }
}
) FROM vw_resume_full_data LIMIT 10;

-- SNOWFLAKE CAPABILITY:
-- CREATE OR REPLACE TABLE resume_name_cleaned AS
-- WITH ai_names AS (
--   SELECT 
--     *,
--     -- PARSE_JSON: Convert AI response back to queryable JSON
--     PARSE_JSON(
--       -- AI_COMPLETE: Built-in AI function for data enhancement
--       ai_complete(
--         model => 'claude-4-sonnet',
--         prompt => CONCAT('Read the provided column and return a json that contains First Name and Last Name only. Some rows has first name last name city all together.<data>', full_name, '</data>'),
--         -- Structured response format ensures consistent JSON output
--         response_format => {
--           'type': 'json',
--           'schema': {
--             'type': 'object',
--             'properties': {
--               'first_name': {'type': 'string'},
--               'last_name': {'type': 'string'}
--             },
--             'required': ['first_name', 'last_name']
--           }
--         }
--       )
--     ) as ai_name_json
--   FROM vw_resume_full_data
-- )
-- SELECT 
--   FILE_NAME,
--   ROW_NUMBER,
--   full_name,
--   -- Extract both names from the single AI response
--   ai_name_json:first_name::STRING as ai_extracted_first_name,
--   ai_name_json:last_name::STRING as ai_extracted_last_name,
--   -- Keep all other columns from the original view
--   email,
--   phone,
--   city,
--   region,
--   country_code,
--   full_address,
--   summary,
--   current_job_profile,
--   current_employer,
--   category,
--   total_experience_years,
--   skills_json,
--   social_network,
--   social_url
-- FROM ai_names;
-- Test the enhanced table
-- SELECT * FROM resume_name_cleaned;

-- SNOWFLAKE CAPABILITY: Multi-level data flattening 
-- Convert nested JSON arrays into relational rows for analytics
CREATE OR REPLACE TABLE candidate_skills AS
SELECT
    raw_resume:ResumeParserData.Name.FullName::STRING AS candidate_name,
    f.value:Skill::STRING AS skill,                          -- Extract skill name from nested structure
    f.value:ExperienceInMonths::INT AS experience_in_months  -- Extract numeric values with automatic casting
FROM
    RESUME_JSON,
    -- LATERAL FLATTEN turns each array element into a separate row
    LATERAL FLATTEN(input => raw_resume:ResumeParserData.SegregatedSkill) f;

-- View the structured skills data
SELECT * FROM candidate_skills;

-- ============================================================================
-- KEY SNOWFLAKE SEMI-STRUCTURED SHOWCASE ENHANCEMENTS
-- ============================================================================

-- Schema Evolution Demo - Show how Snowflake handles new fields seamlessly
INSERT INTO AMS_LABS.DATA_ENGINEERING.RESUME_JSON (FILE_NAME, ROW_NUMBER, RAW_RESUME)
SELECT 
    'schema_evolution_demo.json',
    999,
    PARSE_JSON('{
        "ResumeParserData": {
            "Name": {"FullName": "Jane Evolution"},
            "Email": [{"EmailAddress": "jane@newformat.com"}],
            "NewField_2024": "This field did not exist in original schema",
            "SocialMedia": {
                "LinkedIn": "linkedin.com/in/jane",
                "GitHub": "github.com/jane"
            },
            "SkillsV2": ["Python", "Snowflake", "AI"]
        }
    }');

-- new fields accessible immediately
    select * from AMS_LABS.DATA_ENGINEERING.RESUME_JSON

    WHERE file_name = 'schema_evolution_demo.json';
SELECT 
    file_name,
    raw_resume:ResumeParserData.Name.FullName::STRING as name,
    raw_resume:ResumeParserData.Email[0].EmailAddress::STRING as email,
    -- NEW fields are accessible immediately without schema changes
    raw_resume:ResumeParserData.NewField_2024::STRING as new_field,
    raw_resume:ResumeParserData.SocialMedia.LinkedIn::STRING as linkedin,
    raw_resume:ResumeParserData.SkillsV2 as new_skills_format
FROM AMS_LABS.DATA_ENGINEERING.RESUME_JSON
WHERE file_name = 'schema_evolution_demo.json';

-- Clean up demo record
DELETE FROM AMS_LABS.DATA_ENGINEERING.RESUME_JSON 
WHERE file_name = 'schema_evolution_demo.json';

-- Advanced Analytics Example - Skills Market Intelligence
-- Demonstrates complex analytics on semi-structured data
WITH skills_analytics AS (
    SELECT 
        r.raw_resume:ResumeParserData.Category::STRING as job_category,
        skill.value:Skill::STRING as skill_name,
        skill.value:ExperienceInMonths::NUMBER as skill_experience,
        COUNT(*) OVER (PARTITION BY skill.value:Skill::STRING) as market_demand
    FROM AMS_LABS.DATA_ENGINEERING.RESUME_JSON r,
    LATERAL FLATTEN(input => r.raw_resume:ResumeParserData.SegregatedSkill) skill
    WHERE skill.value:Skill::STRING IS NOT NULL
)
SELECT 
    skill_name,
    COUNT(DISTINCT job_category) as categories_using_skill,
    market_demand,
    ROUND(AVG(skill_experience), 1) as avg_experience_months,
    -- Advanced aggregation showing Snowflake's analytical power
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY skill_experience) as median_experience
FROM skills_analytics
GROUP BY skill_name, market_demand
HAVING market_demand > 1
ORDER BY market_demand DESC, avg_experience_months DESC
LIMIT 10;

-- Full-Text Search Capability
-- Show how Snowflake can search across entire JSON documents
SELECT 
    raw_resume:ResumeParserData.Name.FullName::STRING as candidate_name,
    'Python Developer Match' as search_type
FROM AMS_LABS.DATA_ENGINEERING.RESUME_JSON
WHERE CONTAINS(TO_VARCHAR(raw_resume), 'Python') 
   OR CONTAINS(TO_VARCHAR(raw_resume), 'programming');




-- ============================================================================
-- DATA MARKETPLACE INTEGRATION SHOWCASE
-- ============================================================================
-- PREREQUISITE: Access Snowflake Data Marketplace
-- 1. Navigate to Snowflake Data Marketplace in your account
-- 2. Search for "Global Government Essentials" by Cybersyn
-- 3. Get the dataset (free) to access GLOBAL_GOVERNMENT database
-- 4. This enables enrichment of semi-structured data with external datasets
-- ============================================================================

-- SNOWFLAKE CAPABILITY: Data Marketplace Integration
-- Seamlessly combine semi-structured internal data with external marketplace data
SELECT
    a.variable_name,
    a.date,
    a.value,
    i.geo_name,
    i.iso_alpha2
  FROM GLOBAL_GOVERNMENT.CYBERSYN.WORLD_BANK_TIMESERIES AS a
  LEFT JOIN GLOBAL_GOVERNMENT.CYBERSYN.GEOGRAPHY_INDEX AS i
    ON a.geo_id = i.geo_id
  WHERE
    i.level = 'Country'
    AND a.variable_name = 'Gross National Income per capita, PPP (current international $)'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.geo_id ORDER BY a.DATE DESC) = 1;


CREATE OR REPLACE VIEW AMS_LABS.DATA_ENGINEERING.VW_RESUME_WITH_GNI AS
WITH latest_gni_per_country AS (
  -- This is your query to get the latest GNI for each country
  SELECT
    a.variable_name,
    a.date,
    a.value,
    i.iso_alpha2
  FROM GLOBAL_GOVERNMENT.CYBERSYN.WORLD_BANK_TIMESERIES AS a
  LEFT JOIN GLOBAL_GOVERNMENT.CYBERSYN.GEOGRAPHY_INDEX AS i
    ON a.geo_id = i.geo_id
  WHERE
    i.level = 'Country'
    AND a.variable_name = 'Gross National Income per capita, PPP (current international $)'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.geo_id ORDER BY a.DATE DESC) = 1
)

-- Now, join your resume data to the GNI data
SELECT
  res.*, -- Selects all columns from your original resume view
  gni.value AS gni_per_capita,
  gni.date AS gni_report_date
FROM AMS_LABS.DATA_ENGINEERING.VW_RESUME_FULL_DATA AS res
LEFT JOIN latest_gni_per_country AS gni
  ON res.Country_Code = gni.iso_alpha2;

  -- See the data!
select * from AMS_LABS.DATA_ENGINEERING.VW_RESUME_WITH_GNI;
  
