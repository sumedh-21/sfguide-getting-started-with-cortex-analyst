USE ROLE cortex_user_role;
USE DATABASE cortex_analyst_healthcare;
USE SCHEMA sales_data;


CREATE OR REPLACE CORTEX SEARCH SERVICE hcp_search_service
ON hcp_name
ATTRIBUTES specialty
WAREHOUSE = cortex_analyst_wh
TARGET_LAG = '1 hour'
AS (
  SELECT DISTINCT
    specialty,
    hcp_name
  FROM HCP_ANALYTICS_SUMMARY
);

CREATE OR REPLACE CORTEX SEARCH SERVICE rep_search_service
ON rep_name
WAREHOUSE = cortex_analyst_wh
TARGET_LAG = '1 hour'
AS (
  SELECT DISTINCT rep_name
  FROM REP_PRODUCT_PERFORMANCE
);


CREATE OR REPLACE CORTEX SEARCH SERVICE rep_search_service
ON rep_name
WAREHOUSE = cortex_analyst_wh
TARGET_LAG = '1 hour'
AS (
  SELECT DISTINCT rep_name
  FROM REP_PRODUCT_PERFORMANCE
);
