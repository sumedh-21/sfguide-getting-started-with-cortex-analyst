CREATE OR REPLACE TABLE pharma_analyst.prescription_timeseries.query_log (
    request_id STRING,
    timestamp TIMESTAMP,
    user_question STRING,
    sql_query STRING,
    result_summary STRING,
    user_id STRING
);


CREATE OR REPLACE TABLE pharma_analyst.prescription_timeseries.query_feedback (
    request_id STRING,
    timestamp TIMESTAMP,
    positive BOOLEAN,
    feedback_message STRING,
    user_id STRING
);