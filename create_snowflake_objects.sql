/*--
• Database, schema, warehouse, and stage creation
--*/

USE ROLE SECURITYADMIN;

CREATE ROLE cortex_user_role;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE cortex_user_role;

GRANT ROLE cortex_user_role TO USER '<USER>';

USE ROLE sysadmin;

-- Create database
CREATE OR REPLACE DATABASE pharma_analyst;

-- Create schema
CREATE OR REPLACE SCHEMA pharma_analyst.prescription_timeseries;

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

GRANT OWNERSHIP ON SCHEMA pharma_analyst.prescription_timeseries TO ROLE cortex_user_role;
GRANT OWNERSHIP ON DATABASE pharma_analyst TO ROLE cortex_user_role;


-- Use the created warehouse
USE WAREHOUSE cortex_analyst_wh;

USE DATABASE pharma_analyst;
USE SCHEMA pharma_analyst.prescription_timeseries;

-- Create stage for raw data
CREATE OR REPLACE STAGE raw_data DIRECTORY = (ENABLE = TRUE);

/*--
• Fact and Dimension Table Creation
--*/

CREATE OR REPLACE TABLE pharma_analyst.prescription_timeseries.prescription (
    PHARMACY VARCHAR,
    HEALTHCARE_FACILITY VARCHAR,
    CITY VARCHAR,
    COUNTRY VARCHAR,
    LATITUDE NUMBER(38,4),
    LONGITUDE NUMBER(38,4),
    CHANNEL VARCHAR,
    SUB_CHANNEL VARCHAR,
    PRODUCT_NAME VARCHAR,
    PRODUCT_CLASS VARCHAR,
    QUANTITY NUMBER(38,0),
    PRICE NUMBER(38,0),
    SALES NUMBER(38,0),
    MONTH VARCHAR,
    YEAR NUMBER(38,0),
    SALES_REP VARCHAR,
    MANAGER VARCHAR,
    TRX NUMBER(38,0),
    NRX NUMBER(38,0)
);

-- CREATE GIT REPOSITORY "cortex_healthcare_analyst" 
-- 	ORIGIN = 'https://github.com/sumedh-21/sfguide-getting-started-with-cortex-analyst.git' 
-- 	API_INTEGRATION = 'GITHUB_CORTEX';

--     -- Grant USAGE and OPERATE privileges on the Git repo to your role
-- GRANT USAGE, READ ON GIT REPOSITORY CORTEX_ANALYST_HEALTHCARE.SALES_DATA."cortex_healthcare_analyst"
-- TO ROLE CORTEX_USER_ROLE;


  
-- SHOW GIT REPOSITORIES IN SCHEMA SALES_DATA;

-- SHOW GRANTS ON GIT REPOSITORY "cortex_healthcare_analyst";


