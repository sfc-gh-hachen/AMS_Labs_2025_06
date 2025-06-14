use role accountadmin;
select current_account();
create warehouse test_wh;
select * from TEST.DATA_ENGINEERING.GOLD_CUSTOMER_HIRING_METRICS;

select sourcing_channel_name_clean, candidate_country_clean from TEST.DATA_ENGINEERING.GOLD_CUSTOMER_HIRING_METRICS;

