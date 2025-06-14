# 🎯 AMS Labs - Lab1 Snowflake Evaluation Guide
## **Talent Acquisition Analytics: Snowflake vs Databricks Decision Framework**

---

## 📋 **Project Overview**

### **Mission Statement**
This Lab1 demonstration showcases Snowflake's comprehensive capabilities for organizations evaluating Snowflake against Databricks for their data platform purchase decision. The project uses real-world Talent Acquisition (TA) data to demonstrate enterprise-grade features through hands-on experience.

### **Target Audience**
- **Decision Makers** who have never used Snowflake before
- **Technical Evaluators** comparing Snowflake vs Databricks capabilities  
- **Business Stakeholders** needing to see practical ROI and business value

### **Dataset Context**
- **Domain**: Talent Acquisition and Recruitment Analytics
- **Size**: ~576MB of application data (`temp_ultimate_clean.csv`)
- **Scope**: Global recruitment pipeline with hiring metrics, candidate journeys, and sourcing analytics
- **Business Value**: Real hiring KPIs including time-to-hire, success rates, and regional performance

---

## 🏗️ **Architecture: Bronze → Silver → Gold → Platinum**

### **Modern Data Lakehouse Pattern**
```
📊 PLATINUM LAYER (Executive/Decision Making)
├── Executive Dashboards
├── Automated Reporting  
├── Real-time KPIs
└── C-Level Analytics

🏆 GOLD LAYER (Business-Ready Analytics)
├── Customer Hiring Metrics
├── Application Trends Analysis
├── Funnel Performance Metrics
└── Geographic/Regional Insights

🥈 SILVER LAYER (Enriched & Standardized)
├── AI-Powered Country Classification
├── Working Days Calculations (UDFs)
├── Job Function Categorization
├── Composite Unique IDs
└── Date Standardization

🥉 BRONZE LAYER (Raw Data Foundation)
├── CSV Data Ingestion
├── Automatic Deduplication
├── Schema Detection
└── Data Quality Monitoring
```

---

## 🚀 **Snowflake Capabilities Demonstrated**

### **Core Platform Features**
1. **🔐 Role-Based Access Control (RBAC)** - Enterprise security and governance
2. **📊 Data Quality Monitoring** - Built-in DMFs + Custom business rule validation
3. **⚡ Dynamic Tables** - Real-time data transformation and refresh
4. **🤖 AI_CLASSIFY Function** - AI-powered geographic region classification
5. **🔧 User-Defined Functions** - Python & SQL UDFs for business logic
6. **📈 Streamlit Integration** - Native app development and dashboards
7. **🔄 Tasks & Stored Procedures** - Workflow automation and orchestration

### **Differentiators vs Databricks**
- **Zero-Copy Cloning** - Instant data duplication without storage costs
- **Automatic Scaling** - Compute scales up/down automatically without management
- **SQL-First Approach** - Familiar SQL interface vs Spark complexity  
- **Built-in Governance** - Native RBAC and data quality monitoring
- **Integrated Analytics** - Streamlit apps run natively in Snowflake

---

## 📝 **Step-by-Step Execution Guide**

### **Prerequisites**
- Snowflake account with ACCOUNTADMIN privileges
- Compute warehouse (COMPUTE_WH) provisioned
- Database: `AMS_LABS`, Schema: `DATA_ENGINEERING`
- Dataset uploaded to Snowflake stage

---

## **🥉 STEP 1: Bronze Layer - Data Ingestion**
**File:** `Lab1 - 00_data_ingestion.sql`

### **What This Step Does:**
- Creates robust CSV file format handling semicolon delimiters
- Sets up automated data pipeline with deduplication
- Implements real-time data ingestion using Snowpipes
- Creates deduplicated dynamic table that refreshes every minute

### **Key Features Demonstrated:**
- **File Format Handling** - Custom CSV parsing with error handling
- **Data Pipelines** - Automated ingestion with pattern matching
- **Dynamic Tables** - Real-time deduplication using ROW_NUMBER() window functions
- **Data Profiling** - Row counts, status distributions, and data quality checks

### **Expected Outcome:**
- `TA_APPLICATION_DATA_BRONZE` - Raw talent acquisition data
- `TA_APPLICATION_DATA_BRONZE_DEDUP` - Automatically deduplicated data
- ~500+ application records ready for transformation

---

## **🔍 STEP 2: Data Quality & RBAC Setup**
**File:** `Lab1 - 01_data_quality.sql`

### **What This Step Does:**
- Establishes enterprise-grade Role-Based Access Control
- Implements comprehensive data quality monitoring
- Creates customer-specific business rule validations
- Sets up automated monitoring with 5-minute refresh cycles

### **Data Metric Functions (DMFs) Implemented:**
1. **System DMFs:**
   - `ROW_COUNT` - Volume monitoring baseline
   - `NULL_COUNT` - Critical field completeness
   - `UNIQUE_COUNT` / `DUPLICATE_COUNT` - Data integrity checks

2. **Custom Business Rule DMFs:**
   - `INVALID_JOBNUMBER_FORMAT_COUNT` - JobNumber must start with "AMS"
   - `INVALID_WORKDAYID_FORMAT_COUNT` - WorkdayID must match "JR-#####" pattern
   - `INVALID_JOBSTATUS_COUNT` - Enum validation for job statuses
   - `INVALID_CANDIDATESTATUS_COUNT` - Enum validation for candidate statuses

### **Key Features Demonstrated:**
- **Enterprise Security** - RBAC with custom roles and access patterns
- **Data Governance** - Automated quality monitoring and alerting
- **Custom Business Logic** - DMFs tailored to customer requirements
- **Real-time Monitoring** - Scheduled quality checks with result tracking

### **Expected Outcome:**
- Comprehensive data quality dashboard
- Real-time monitoring of business rule violations
- Enterprise-ready security framework

---

## **📊 STEP 2b: Data Quality Dashboard**
**File:** `Lab1 - 01b_ams_data_quality_dashboard.py`

### **What This Step Does:**
- Provides interactive data quality monitoring interface
- Enables drill-down analysis of data quality issues
- Shows real-time quality score calculations
- Offers problematic record identification and export

### **Dashboard Features:**
- **Quality Score** - Overall data health percentage
- **Metric Cards** - Visual status indicators (🟢🟡🔴)
- **Drill-down Analysis** - Interactive investigation of quality issues
- **Trend Analysis** - Data quality metrics over time
- **Export Functionality** - Download problematic records for remediation

---

## **🥈 STEP 3: Silver Layer - Data Transformation**
**File:** `Lab1 - 02_data_transformation.sql`

### **What This Step Does:**
- Implements advanced data transformations using UDFs and AI
- Creates enriched, business-ready datasets
- Establishes customer-specific analytics foundations
- Builds scalable, reusable data architecture patterns

### **Customer-Specific UDFs Implemented:**

#### **Python UDFs (Business Logic):**
1. **`CALCULATE_WORKING_DAYS`** - Accurate business day calculations excluding weekends
   - Powers time-to-hire and time-to-rejection analytics
   - Handles null values and date edge cases

#### **SQL UDFs (Data Processing):**
2. **`GENERATE_UNIQUE_APPLICATION_ID`** - Composite unique key generation
   - Format: JobNumber-WorkdayID-CandidateID
   - Enables reliable data integration and deduplication

3. **`CATEGORIZE_JOB_FUNCTION`** - Advanced job title classification
   - Maps complex job titles to 5 business function categories
   - Supports regional hiring analysis and reporting

4. **`CATEGORIZE_WORKING_DAYS_BUCKET`** - Time bucket standardization
   - Creates consistent time-to-hire benchmark categories
   - Enables comparative analysis and trend reporting

### **AI-Powered Geographic Mapping:**
- **`AI_CLASSIFY` Function** - Snowflake's cutting-edge AI capabilities
- **Purpose**: Intelligent country-to-region classification (APAC/EMEA/Americas)
- **Benefits**: Self-updating classifications, transparent AI reasoning, scalable to new countries

### **Dynamic Tables Created:**
1. **`SILVER_APPLICATIONS_CLEANED`** - Standardized and cleaned data
2. **`SILVER_APPLICATIONS_ENRICHED`** - UDF-enhanced with calculated fields
3. **`COUNTRY_REGION_MAPPING`** - AI-powered geographic classifications

### **Key Features Demonstrated:**
- **Advanced UDFs** - Python and SQL functions for complex business logic
- **AI Integration** - Snowflake's AI_CLASSIFY for intelligent data processing
- **Real-time Processing** - Dynamic tables with 1-minute refresh cycles
- **Performance Optimization** - JOIN-based architecture vs UDF calls

---

## **🏆 STEP 4: Gold Layer - Business Analytics**
**File:** `Lab1 - 02_data_transformation.sql` (continued)

### **What This Step Does:**
- Creates customer-specific analytics tables for business insights
- Implements comprehensive hiring funnel analysis
- Builds time-series trend analysis capabilities
- Establishes executive-ready KPI calculations

### **Gold Tables Created:**

#### **1. `GOLD_CUSTOMER_HIRING_METRICS`**
**Purpose:** Customer-specific KPIs and hiring performance analytics

**Key Metrics Included:**
- **Working Days Analysis:** Average, median, min, max time-to-hire/rejection
- **Funnel Progression:** Applied → In Process → Offered → Hired conversion rates
- **Geographic Performance:** Success rates by region and country
- **Sourcing Effectiveness:** Channel performance and ROI analysis
- **Success Rates:** Overall conversion percentages and benchmarking

#### **2. `GOLD_APPLICATION_TRENDS`**
**Purpose:** Time-series analysis and trend identification

**Analytics Provided:**
- Daily application volumes and success rates
- 30-day rolling averages for trend smoothing
- Regional performance comparisons over time
- Application aging analysis (fresh vs aged applications)
- Top sourcing channels by day/region

### **Advanced Analytics Features:**
- **Complex CTEs** - Multi-stage analytical processing
- **Window Functions** - Rolling calculations and rankings
- **Statistical Functions** - Median, percentiles, and distributions
- **Business Logic** - Funnel conversion and drop-off analysis

---

## **💎 STEP 5: Platinum Layer - Executive Insights**
**File:** `Lab1 - 02_data_transformation.sql` (continued)

### **What This Step Does:**
- Implements automated executive reporting
- Creates C-level dashboard data feeds
- Establishes workflow automation with Tasks
- Provides production-ready stored procedures

### **Stored Procedures & Automation:**
1. **`GENERATE_EXECUTIVE_DASHBOARD()`** - Automated executive summary generation
2. **`DAILY_PIPELINE_SUMMARY_TASK`** - Scheduled daily reporting
3. **`WEEKLY_EXECUTIVE_REPORT_TASK`** - Weekly executive dashboard updates

### **Executive Views Created:**
- **`PLATINUM_EXECUTIVE_DASHBOARD`** - C-level KPI summary
- **`ANALYST_APPLICATION_SUMMARY`** - Detailed analytical views
- **`REALTIME_PIPELINE_STATUS`** - Operational health monitoring
- **`FUNNEL_METRICS_SUMMARY`** - Recruitment funnel performance

---

## **📈 STEP 6: Analytics Dashboard**
**File:** `Lab1 - 03_analytics_dashboard.py`

### **What This Step Does:**
- Delivers comprehensive interactive analytics experience
- Provides customer-specific visualizations and insights
- Enables self-service analytics for business users
- Demonstrates Snowflake's native Streamlit integration

### **Dashboard Tabs & Features:**

#### **📊 Executive Overview**
- **Key Metrics Cards:** Total applications, success rates, time-to-hire, unique candidates
- **Performance Insights:** Top performing regions, hiring efficiency analysis
- **Trend Visualization:** Application volume trends with 30-day rolling averages

#### **🔄 Application Funnel Flow**
- **Interactive Sankey Diagram:** Visual application journey through recruitment stages
- **Conversion Analysis:** Stage-by-stage progression rates and drop-off points
- **Process Insights:** Screening success, interview conversion, offer acceptance rates

#### **⏱️ Time-to-Hire Analysis** 
- **Working Days Trends:** 13-month trend analysis as requested by customer
- **Regional Comparisons:** Time-to-hire performance by geographic region
- **Bucket Analysis:** Distribution across time categories (1-5 days, 6-30 days, etc.)

#### **❌ Rejection Analysis**
- **Stage Analysis:** Where candidates are rejected in the process
- **Time-to-Rejection:** How quickly rejection decisions are made
- **Improvement Opportunities:** Identifies bottlenecks and optimization areas

#### **📢 Sourcing Performance**
- **Channel Effectiveness:** Performance analysis by sourcing channel type
- **Drill-through Capability:** Interactive exploration of channel performance
- **ROI Analysis:** Cost-effectiveness and volume analysis by source

#### **🗺️ Geographic Distribution**
- **Map Visualization:** Global candidate distribution by country
- **Regional Performance:** Success rates and volume by APAC/EMEA/Americas
- **Market Insights:** Talent acquisition performance by geographic market

### **Interactive Features:**
- **Dynamic Filtering:** Real-time filters by region, job function, sourcing channel
- **Drill-down Analysis:** Click-through capability for detailed investigation
- **Export Functionality:** Download data and insights for further analysis
- **Refresh Controls:** Real-time data updates and cache management

---

## **📊 STEP 7: Operational Dashboards**
**Files:** `streamlit_analytics_dashboard.py` & `Lab1 - 01b_ams_data_quality_dashboard.py`

### **Secondary Analytics Dashboard** 
**Features:** Pipeline health monitoring, funnel analysis, task monitoring, analyst views

### **Data Quality Dashboard**
**Features:** Real-time quality monitoring, drill-down analysis, problematic record identification

---

## 🎯 **Customer Requirements Fulfillment**

### **✅ Data Integrity Requirements Met:**
- JobNumber PREFIX validation ("AMS" mandatory)
- WorkdayID PREFIX validation ("JR" mandatory)  
- JobStatus enum validation
- CandidateStatus enum validation
- All implemented via custom DMFs with real-time monitoring

### **✅ Data Enhancement Requirements Met:**
- Regional mapping (APAC/EMEA/Americas) via AI_CLASSIFY
- Unique application ID generation via UDF
- Working days calculations for accurate business metrics

### **✅ Visual Requirements Met:**
- Time-to-hire trend analysis (13-month line charts)
- Time-to-rejection trend analysis (13-month line charts)
- Rejection by stage analysis (pie charts with LastCompletedStep)
- Sourcing channel performance (pie charts with drill-through)
- Geographic distribution (map visualization by country)

### **✅ Interactive Filtering Met:**
- All customer-requested filter fields implemented
- Real-time filtering across all visualizations
- Cross-dashboard filter consistency

---

## ⚡ **Quick Start Instructions**

### **1. Environment Setup**
```sql
-- Set up database and schema
USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS AMS_LABS;
CREATE SCHEMA IF NOT EXISTS AMS_LABS.DATA_ENGINEERING;
USE DATABASE AMS_LABS;
USE SCHEMA DATA_ENGINEERING;
```

### **2. Execute Scripts in Order**
1. **Data Ingestion:** Run `Lab1 - 00_data_ingestion.sql`
2. **Data Quality:** Run `Lab1 - 01_data_quality.sql`  
3. **Transformations:** Run `Lab1 - 02_data_transformation.sql`
4. **Dashboards:** Deploy Streamlit apps

### **3. Launch Dashboards**
- **Main Analytics:** `Lab1 - 03_analytics_dashboard.py`
- **Data Quality:** `Lab1 - 01b_ams_data_quality_dashboard.py`
- **Operational:** `streamlit_analytics_dashboard.py`

---

## 📦 **Dependencies**

### **Python Requirements** (requirements.txt)
```
plotly>=5.15.0
pandas>=1.5.0  
numpy>=1.24.0
```

### **Snowflake Features Required**
- ACCOUNTADMIN role access
- Compute warehouse provisioned
- Streamlit in Snowflake enabled
- AI_CLASSIFY function access

---

## 🏆 **Expected Business Outcomes**

### **For Decision Makers:**
- **Hands-on Experience:** Direct interaction with Snowflake's capabilities
- **Real-world Context:** Practical talent acquisition analytics use case
- **ROI Demonstration:** Clear business value and immediate applicability
- **Comparison Framework:** Direct feature comparison with Databricks

### **For Technical Evaluators:**
- **Architecture Patterns:** Modern data lakehouse implementation
- **Performance Insights:** Real-time processing and auto-scaling capabilities  
- **Governance Demonstration:** Enterprise-grade security and data quality
- **Integration Capabilities:** Native AI, visualization, and workflow automation

### **For Business Users:**
- **Self-Service Analytics:** Interactive dashboards and drill-down capabilities
- **Real-time Insights:** Live data processing and automated refresh
- **Actionable Intelligence:** Data-driven recruitment optimization opportunities
- **Executive Reporting:** C-level dashboard and automated insights

---

## 🔧 **Troubleshooting**

### **Common Issues:**
1. **Data Loading:** Ensure CSV file is uploaded to Snowflake stage with correct format
2. **Permissions:** Verify ACCOUNTADMIN access for DMF and UDF creation
3. **Dependencies:** Confirm Streamlit and required Python packages are available
4. **AI_CLASSIFY:** Ensure AI functions are enabled in your Snowflake account

### **Performance Optimization:**
- Monitor warehouse usage during Dynamic Table refreshes
- Adjust TARGET_LAG settings based on business requirements
- Consider query optimization for large datasets

---

## 📞 **Support & Next Steps**

### **Immediate Actions:**
1. Execute the step-by-step guide
2. Explore interactive dashboards  
3. Compare capabilities with current data platform
4. Identify specific use cases for your organization

### **Advanced Exploration:**
- Customize UDFs for your business logic
- Extend DMFs for your data quality requirements
- Build additional dashboards for your specific needs
- Explore Snowflake's advanced features (data sharing, marketplace, etc.)

---

**🎯 Ready to transform your data platform decision-making process with hands-on Snowflake experience!** 