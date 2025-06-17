/***************************************************************************************************
| A | M | S |   | L | A | B | S |   | C | U | S | T | O | M | E | R |   | D | E | M | O |

Demo:         AMS Labs Data Sharing Consumer Script
Create Date:  2025-06-15
Purpose:      Demonstrate data consumption from shared datasets with applied security policies
Data Source:  TEST.DATA_ENGINEERING.GOLD_CUSTOMER_HIRING_METRICS (shared data)
Customer:     Data Consumer and Collaboration
****************************************************************************************************

/*----------------------------------------------------------------------------------
This script demonstrates the consumer side of Snowflake's secure data sharing.

USE THIS IN THE CONSUMER ACCOUNT

It shows how shared data appears to consuming accounts with applied row-level
security and masking policies, highlighting the seamless yet secure nature
of cross-account data collaboration.

Key Concepts:
  • Data Sharing Consumption: Accessing shared datasets from other accounts
  • Applied Security Policies: Automatic enforcement of row-level security and masking
  • Cross-Account Collaboration: Seamless data access across organizational boundaries
  • Shared Database Access: Working with externally shared data sources
  • Policy Enforcement: Automatic application of security controls during consumption
----------------------------------------------------------------------------------*/

use role accountadmin;
select current_account();
create warehouse test_wh;
select * from <database_name>.DATA_ENGINEERING.GOLD_CUSTOMER_HIRING_METRICS;

select sourcing_channel_name_clean, candidate_country_clean from <database_name>.DATA_ENGINEERING.GOLD_CUSTOMER_HIRING_METRICS;

