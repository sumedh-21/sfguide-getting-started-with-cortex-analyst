/*--
• Database, schema, warehouse, and stage creation
--*/

USE ROLE SECURITYADMIN;

CREATE ROLE cortex_user_role;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE cortex_user_role;

GRANT ROLE cortex_user_role TO USER APURVPALIWAL99;

USE ROLE sysadmin;

-- Create demo database
CREATE OR REPLACE DATABASE cortex_analyst_healthcare;

-- Create schema
CREATE OR REPLACE SCHEMA cortex_analyst_healthcare.sales_data;

-- Create warehouse
CREATE OR REPLACE WAREHOUSE cortex_analyst_wh
    WAREHOUSE_SIZE = 'large'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'Warehouse for Cortex Analyst';

GRANT USAGE ON WAREHOUSE cortex_analyst_wh TO ROLE cortex_user_role;
GRANT OPERATE ON WAREHOUSE cortex_analyst_wh TO ROLE cortex_user_role;

GRANT OWNERSHIP ON SCHEMA cortex_analyst_healthcare.sales_data TO ROLE cortex_user_role;
GRANT OWNERSHIP ON DATABASE cortex_analyst_healthcare TO ROLE cortex_user_role;

-- Use the created warehouse
USE WAREHOUSE cortex_analyst_wh;

USE DATABASE cortex_analyst_healthcare;
USE SCHEMA cortex_analyst_healthcare.sales_data;



-- Create stage for raw data
CREATE OR REPLACE STAGE raw_data DIRECTORY = (ENABLE = TRUE);

/*--
• Fact and Dimension Table Creation
--*/

CREATE OR REPLACE TABLE cortex_analyst_healthcare.sales_data.hcp_analytics_summary (
    hcp_id STRING,
    hcp_name STRING,
    specialty STRING,
    territory_id STRING,
    region_id STRING,
    
    product_id STRING,
    product_name STRING,
    market STRING,

    trx INT,
    nrx INT,
    nbrx INT,
    total_sales_usd FLOAT,

    market_avg_trx INT,
    trx_vs_market_pct FLOAT,        -- (TRx - Market Avg) / Market Avg
    trx_share FLOAT,                -- TRx Share of Market
    trx_share_change_pct FLOAT,     -- MoM Change
    market_rank INT,                -- Rank within Market

    call_count INT,                 -- In-person rep visits
    digital_engagements INT,        -- Email, Veeva CLM, eDetail
    speaker_programs_attended INT,
    engagement_score FLOAT,         -- Weighted score of all interactions
    reach INT,                      -- Unique rep touches
    frequency INT,                  -- Avg. touches/month
    preferred_channel STRING,       -- 'Digital', 'Face-to-face', 'Hybrid'
    last_rep_engagement_date DATE,

    coverage_status STRING,         -- Full, Partial, Closed
    is_targeted BOOLEAN,
    is_engaged BOOLEAN,
    
    snapshot_date DATE              -- Date of snapshot (monthly roll-up)
);

CREATE OR REPLACE TABLE sales_data.rep_product_performance (
    rep_id STRING,
    rep_name STRING,
    manager_id STRING,
    manager_name STRING,
    region_id STRING,
    territory_id STRING,

    product_id STRING,
    product_name STRING,
    market STRING,

    call_count INT,                      -- Calls for the product
    call_goal INT,                       -- Monthly goal
    call_attainment_pct FLOAT,          -- call_count / call_goal

    speaker_events_hosted INT,
    avg_engagement_score FLOAT,         -- Weighted from HCP engagement inputs
    digital_engagements INT,
    field_days INT,                     -- Active selling days

    product_focus_score FLOAT,          -- Custom metric: product alignment vs. plan
    brand_priority STRING,              -- High, Medium, Low
    is_core_product BOOLEAN,

    percent_target_hcps_engaged FLOAT,  -- Out of assigned target list
    channel_mix_pct_digital FLOAT,

    coverage_type STRING,               -- Full, partial, vacant
    is_high_priority_territory BOOLEAN,

    snapshot_month DATE
);


CREATE OR REPLACE TABLE sales_data.elite_club_scores (
    rep_id STRING,
    rep_name STRING,
    manager_name STRING,
    region_id STRING,
    territory_id STRING,

    performance_score FLOAT,       -- Weighted composite score
    goal_attainment_pct FLOAT,
    product_focus_score FLOAT,
    engagement_score FLOAT,
    reach_score FLOAT,

    ranking_national INT,
    ranking_region INT,
    ranking_territory INT,

    snapshot_quarter DATE          -- e.g., '2024-03-31' for Q1
);


CREATE GIT REPOSITORY "cortex_healthcare_analyst" 
	ORIGIN = 'https://github.com/sumedh-21/sfguide-getting-started-with-cortex-analyst.git' 
	API_INTEGRATION = 'GITHUB_CORTEX';

    -- Grant USAGE and OPERATE privileges on the Git repo to your role
GRANT USAGE, READ ON GIT REPOSITORY CORTEX_ANALYST_HEALTHCARE.SALES_DATA."cortex_healthcare_analyst"
TO ROLE CORTEX_USER_ROLE;


  
SHOW GIT REPOSITORIES IN SCHEMA SALES_DATA;

SHOW GRANTS ON GIT REPOSITORY "cortex_healthcare_analyst";


