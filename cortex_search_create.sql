-- Step 1: Set the correct context
USE DATABASE cortex_analyst_healthcare;
USE SCHEMA sales_data;

-- Step 2: Create hcp_search_service
CREATE OR REPLACE CORTEX SEARCH SERVICE hcp_search_service
ON HCP_ANALYTICS_SUMMARY
WAREHOUSE = cortex_analyst_wh
TARGET_LAG = '1 hour'
AS (
    SELECT DISTINCT hcp_name, specialty
    FROM HCP_ANALYTICS_SUMMARY
);

-- Step 3: Create rep_search_service
CREATE OR REPLACE CORTEX SEARCH SERVICE rep_search_service
ON REP_PRODUCT_PERFORMANCE
WAREHOUSE = cortex_analyst_wh
TARGET_LAG = '1 hour'
AS (
    SELECT DISTINCT rep_name
    FROM REP_PRODUCT_PERFORMANCE
);

-- Step 4: Create elite_club_search_service
CREATE OR REPLACE CORTEX SEARCH SERVICE elite_club_search_service
ON ELITE_CLUB_SCORES
WAREHOUSE = cortex_analyst_wh
TARGET_LAG = '1 hour'
AS (
    SELECT DISTINCT rep_name
    FROM ELITE_CLUB_SCORES
);
