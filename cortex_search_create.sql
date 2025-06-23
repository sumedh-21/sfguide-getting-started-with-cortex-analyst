-- Step 1: Set the correct context
USE DATABASE pharma_analyst;
USE SCHEMA prescription_timeseries;

-- Step 2: Create the prescription_facility_search_service
CREATE OR REPLACE CORTEX SEARCH SERVICE prescription_facility_search_service
ON HEALTHCARE_FACILITY
WAREHOUSE = cortex_analyst_wh
TARGET_LAG = '1 hour'
AS (
    SELECT DISTINCT HEALTHCARE_FACILITY
    FROM PRESCRIPTION
);

