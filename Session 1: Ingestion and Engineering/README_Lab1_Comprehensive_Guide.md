# 🎯 AMS Labs - Lab1 Snowflake Demo Guide
## **Talent Acquisition Analytics with Snowflake**

---

## 📋 **Project Overview**

### **Mission Statement**
This Lab1 demonstration showcases Snowflake's comprehensive data platform capabilities using real-world Talent Acquisition (TA) data to demonstrate enterprise-grade features through hands-on experience.

### **Target Audience**
- **Decision Makers** evaluating modern data platforms
- **Technical Teams** exploring Snowflake capabilities  
- **Business Stakeholders** seeking practical ROI and business value

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

### **Key Snowflake Advantages**
- **Zero-Copy Cloning** - Instant data duplication without storage costs
- **Automatic Scaling** - Compute scales up/down automatically without management
- **SQL-First Approach** - Familiar SQL interface for all users
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
- Creates robust CSV file format handling with custom delimiters
- Sets up automated data pipeline with real-time ingestion using Snowpipes
- Implements dynamic table with automatic deduplication

### **Implementation Approach:**
1. **Initial Upload:** First CSV file uploaded via UI (as shown in Quick Start)
2. **Script Execution:** Run the ingestion script to create Stage and Snowpipe infrastructure
3. **Automated Pipeline:** Upload remaining CSV chunk files to demonstrate automated ingestion
4. **Real-time Processing:** Snowpipe automatically processes new files as they arrive

### **Key Features:**
- **File Format Handling** - Custom CSV parsing with error handling
- **Data Pipelines** - Automated ingestion using Snowpipes with pattern matching
- **Dynamic Tables** - Real-time deduplication (1-minute refresh)
- **Hybrid Approach** - Demonstrates both manual upload and automated pipeline capabilities

### **Expected Outcome:**
- `TA_APPLICATION_DATA_BRONZE` - Raw talent acquisition data (initially from UI upload)
- `TA_APPLICATION_DATA_BRONZE_DEDUP` - Deduplicated data ready for transformation
- Fully automated pipeline ready for additional file uploads

---

## **🔍 STEP 2: Data Quality & RBAC Setup**
**File:** `Lab1 - 01_data_quality.sql`

### **What This Step Does:**
- Establishes Role-Based Access Control (RBAC)
- Implements comprehensive data quality monitoring
- Creates custom business rule validations

### **Data Metric Functions (DMFs):**
- **System DMFs:** ROW_COUNT, NULL_COUNT, UNIQUE_COUNT, DUPLICATE_COUNT
- **Custom Business Rules:** JobNumber format (AMS prefix), WorkdayID format (JR prefix), status validations

### **Key Features:**
- **Enterprise Security** - RBAC with custom roles
- **Data Governance** - Automated quality monitoring (5-minute refresh)
- **Custom Validation** - Business-specific data quality rules

### **Expected Outcome:**
- Real-time data quality monitoring
- Automated business rule validation

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
- Establishes analytics foundations with real-time processing

### **User-Defined Functions (UDFs):**
- **Python UDF:** `CALCULATE_WORKING_DAYS` - Business day calculations for hiring metrics
- **SQL UDFs:** 
  - `GENERATE_UNIQUE_APPLICATION_ID` - Composite key generation
  - `CATEGORIZE_JOB_FUNCTION` - Job title classification
  - `CATEGORIZE_WORKING_DAYS_BUCKET` - Time bucket standardization

### **AI-Powered Features:**
- **`AI_CLASSIFY` Function** - Intelligent country-to-region classification (APAC/EMEA/Americas)
- **Benefits**: Self-updating classifications, scalable to new countries

### **Dynamic Tables Created:**
- `SILVER_APPLICATIONS_CLEANED` - Standardized data
- `SILVER_APPLICATIONS_ENRICHED` - UDF-enhanced with calculated fields
- `COUNTRY_REGION_MAPPING` - AI-powered geographic classifications

### **Key Features:**
- **Advanced UDFs** - Python and SQL functions for business logic
- **AI Integration** - Snowflake's AI_CLASSIFY for data processing
- **Real-time Processing** - Dynamic tables with 1-minute refresh

---

## **🏆 STEP 4: Gold Layer - Business Analytics**
**File:** `Lab1 - 02_data_transformation.sql` (continued)

### **What This Step Does:**
- Creates analytics tables for business insights
- Implements hiring funnel analysis and KPI calculations
- Builds time-series trend analysis capabilities

### **Gold Tables Created:**
- **`GOLD_CUSTOMER_HIRING_METRICS`** - Hiring performance KPIs, funnel analysis, geographic performance
- **`GOLD_APPLICATION_TRENDS`** - Time-series analysis, daily volumes, rolling averages

### **Key Analytics Features:**
- Working days analysis (average, median, min, max time-to-hire/rejection)
- Funnel progression rates (Applied → In Process → Offered → Hired)
- Geographic and sourcing channel performance
- 30-day rolling averages and trend identification

---

## **💎 STEP 5: Platinum Layer - Executive Insights**
**File:** `Lab1 - 02_data_transformation.sql` (continued)

### **What This Step Does:**
- Implements automated executive reporting and workflow automation
- Creates executive dashboard data feeds and operational monitoring

### **Automation Features:**
- **Stored Procedures:** `GENERATE_EXECUTIVE_DASHBOARD()` - Automated reporting
- **Tasks:** Daily and weekly scheduled reporting tasks

### **Executive Views:**
- `PLATINUM_EXECUTIVE_DASHBOARD` - C-level KPI summary
- `ANALYST_APPLICATION_SUMMARY` - Detailed analytics
- `REALTIME_PIPELINE_STATUS` - Operational health monitoring

---

## **📈 STEP 6: Analytics Dashboard**
**File:** `Lab1 - 03_analytics_dashboard.py`

### **What This Step Does:**
- Delivers interactive analytics experience using Snowflake's native Streamlit integration
- Provides comprehensive visualizations and self-service analytics

### **Dashboard Features:**
- **📊 Executive Overview** - Key metrics, performance insights, trend visualization
- **🔄 Application Funnel Flow** - Interactive Sankey diagram showing recruitment journey
- **⏱️ Time-to-Hire Analysis** - Working days trends and regional comparisons
- **❌ Rejection Analysis** - Stage analysis and improvement opportunities
- **📢 Sourcing Performance** - Channel effectiveness with drill-through capability
- **🗺️ Geographic Distribution** - Global candidate distribution and regional performance

### **Interactive Features:**
- Dynamic filtering by region, job function, sourcing channel
- Drill-down analysis and export functionality
- Real-time data updates and refresh controls

---

## **📊 STEP 7: Operational Dashboards**
**Files:** `streamlit_analytics_dashboard.py` & `Lab1 - 01b_ams_data_quality_dashboard.py`

### **Secondary Analytics Dashboard** 
**Features:** Pipeline health monitoring, funnel analysis, task monitoring, analyst views

### **Data Quality Dashboard**
**Features:** Real-time quality monitoring, drill-down analysis, problematic record identification

---

## 🎯 **Key Features Delivered**

### **✅ Data Quality & Integrity:**
- Custom business rule validation (JobNumber, WorkdayID, status fields)
- Real-time monitoring with automated alerts
- Interactive quality dashboard with drill-down capabilities

### **✅ Advanced Analytics:**
- AI-powered geographic region classification
- Working days calculations for hiring metrics
- Comprehensive funnel analysis and KPI tracking

### **✅ Interactive Visualization:**
- Time-to-hire and rejection trend analysis
- Interactive Sankey diagrams for recruitment flow
- Geographic distribution with regional performance
- Real-time filtering and drill-down capabilities

---

## ⚡ **Quick Start Instructions**

### **1. Environment Setup**
Run the provided setup script to create the database, schema, and initial configuration:
- Execute the environment setup script (creates `AMS_LABS` database and `DATA_ENGINEERING` schema)
- Ensure you have ACCOUNTADMIN privileges for full functionality

### **2. Initial Data Upload (UI Method)**
Before running the ingestion script, we'll use Snowflake's UI to upload the first CSV file and demonstrate the table creation process:

![Data Upload Interface](image-1.png)

**Steps:**
1. **Upload First File:** Use Snowflake's "Load Data into Table" interface to upload `TA_Applications_Data_For_Workshops v3_chunk_001.csv`
2. **Table Configuration:** 
   - Select schema: `AMS_LABS.DATA_ENGINEERING`
   - **Important:** Name the table `TA_APPLICATION_DATA_BRONZE` (exactly as shown)
   - Configure CSV format settings (semicolon delimiter, headers, etc.)
3. **Verify Upload:** Confirm the table is created with correct structure and initial data

### **3. Execute Scripts in Order**
1. **Data Ingestion:** Run `Lab1 - 00_data_ingestion.sql`
   - **Note:** This script creates the Stage and Snowpipe for automated ingestion
   - After running the script, upload the remaining CSV chunk files locally
   - This demonstrates both manual upload and automated pipeline capabilities
2. **Data Quality:** Run `Lab1 - 01_data_quality.sql`  
3. **Transformations:** Run `Lab1 - 02_data_transformation.sql`
4. **Dashboards:** Deploy Streamlit apps

### **4. Launch Dashboards**
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

## 🏆 **Expected Outcomes**

### **For Decision Makers:**
- Hands-on experience with Snowflake's enterprise capabilities
- Real-world business context with practical talent acquisition use case
- Clear ROI demonstration and immediate applicability

### **For Technical Teams:**
- Modern data lakehouse architecture patterns
- Real-time processing and auto-scaling insights
- Enterprise security, data quality, and AI integration examples

### **For Business Users:**
- Self-service interactive analytics and dashboards
- Real-time insights and automated reporting
- Data-driven recruitment optimization opportunities

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

## 📞 **Next Steps**

### **Get Started:**
1. Execute the step-by-step guide
2. Explore the interactive dashboards
3. Identify specific use cases for your organization

### **Advanced Exploration:**
- Customize UDFs and DMFs for your business needs
- Build additional dashboards for specific requirements
- Explore Snowflake's advanced features (data sharing, marketplace, etc.)

---

**🎯 Ready to experience Snowflake's modern data platform capabilities!** 