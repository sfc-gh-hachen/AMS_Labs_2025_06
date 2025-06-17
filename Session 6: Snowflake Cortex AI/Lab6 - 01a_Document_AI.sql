use role accountadmin;
use database ams_labs;
use schema data_engineering;

create or replace table ams_labs.data_engineering.doc_ai_resume as
SELECT
    RELATIVE_PATH,
    '@DATA_ENGINGEERING.CVS/Product Managers' AS STAGE,
    AMS_LABS.DATA_ENGINEERING.AMS_RESUME!PREDICT(
        GET_PRESIGNED_URL(
            '@DATA_ENGINEERING.CVS',
            RELATIVE_PATH
        ),
        1
    ) METADATA
FROM
    DIRECTORY ('@CVS')
    where startswith(relative_path, 'Product Managers/');

select * from ams_labs.data_engineering.doc_ai_resume;
