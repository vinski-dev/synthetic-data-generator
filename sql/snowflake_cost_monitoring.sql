/* ============================================================================
   Snowflake Cost Monitoring Queries
   Project: Synthetic Sales ELT Pipeline

   Purpose:
   Monitor Snowflake warehouse credit usage, query volume, expensive queries,
   and dbt workload cost patterns.

   Main project warehouses:
   - WH_DBT_DEV
   - WH_DBT_TRANSFORM
   - WH_DBT_MARTS

   Notes:
   - Uses SNOWFLAKE.ACCOUNT_USAGE views.
   - ACCOUNT_USAGE data may have latency.
   - Requires privileges to query SNOWFLAKE.ACCOUNT_USAGE.
============================================================================ */


/* ============================================================================
   1. Warehouse credit usage by day
============================================================================ */

SELECT
    DATE_TRUNC('day', START_TIME)::DATE AS usage_date,
    WAREHOUSE_NAME,
    SUM(CREDITS_USED) AS total_credits_used,
    SUM(CREDITS_USED_COMPUTE) AS compute_credits_used,
    SUM(CREDITS_USED_CLOUD_SERVICES) AS cloud_services_credits_used
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP())
  AND WAREHOUSE_NAME IN (
      'WH_DBT_DEV',
      'WH_DBT_TRANSFORM',
      'WH_DBT_MARTS'
  )
GROUP BY
    usage_date,
    WAREHOUSE_NAME
ORDER BY
    usage_date DESC,
    total_credits_used DESC;


/* ============================================================================
   2. Warehouse credit usage summary for last 30 days
============================================================================ */

SELECT
    WAREHOUSE_NAME,
    SUM(CREDITS_USED) AS total_credits_used,
    SUM(CREDITS_USED_COMPUTE) AS compute_credits_used,
    SUM(CREDITS_USED_CLOUD_SERVICES) AS cloud_services_credits_used,
    COUNT(DISTINCT DATE_TRUNC('day', START_TIME)) AS active_days,
    ROUND(
        SUM(CREDITS_USED)
        / NULLIF(COUNT(DISTINCT DATE_TRUNC('day', START_TIME)), 0),
        4
    ) AS avg_credits_per_active_day
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP())
  AND WAREHOUSE_NAME IN (
      'WH_DBT_DEV',
      'WH_DBT_TRANSFORM',
      'WH_DBT_MARTS'
  )
GROUP BY
    WAREHOUSE_NAME
ORDER BY
    total_credits_used DESC;


/* ============================================================================
   3. Hourly warehouse usage pattern
   Useful for checking when scheduled jobs consume compute.
============================================================================ */

SELECT
    DATE_TRUNC('hour', START_TIME) AS usage_hour,
    WAREHOUSE_NAME,
    SUM(CREDITS_USED) AS credits_used
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND WAREHOUSE_NAME IN (
      'WH_DBT_DEV',
      'WH_DBT_TRANSFORM',
      'WH_DBT_MARTS'
  )
GROUP BY
    usage_hour,
    WAREHOUSE_NAME
ORDER BY
    usage_hour DESC,
    credits_used DESC;


/* ============================================================================
   4. Top expensive queries by execution time
   This is a proxy for expensive queries when exact query-level cost is not used.
============================================================================ */

SELECT
    QUERY_ID,
    USER_NAME,
    ROLE_NAME,
    WAREHOUSE_NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    QUERY_TYPE,
    EXECUTION_TIME / 1000 AS execution_seconds,
    TOTAL_ELAPSED_TIME / 1000 AS total_elapsed_seconds,
    BYTES_SCANNED,
    ROWS_PRODUCED,
    START_TIME,
    QUERY_TEXT
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND WAREHOUSE_NAME IN (
      'WH_DBT_DEV',
      'WH_DBT_TRANSFORM',
      'WH_DBT_MARTS'
  )
ORDER BY
    EXECUTION_TIME DESC
LIMIT 25;


/* ============================================================================
   5. Queries scanning the most data
   Useful for identifying models that need pruning, clustering, or incremental logic.
============================================================================ */

SELECT
    QUERY_ID,
    USER_NAME,
    ROLE_NAME,
    WAREHOUSE_NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    QUERY_TYPE,
    BYTES_SCANNED,
    ROUND(BYTES_SCANNED / POWER(1024, 3), 2) AS gb_scanned,
    EXECUTION_TIME / 1000 AS execution_seconds,
    START_TIME,
    QUERY_TEXT
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND WAREHOUSE_NAME IN (
      'WH_DBT_DEV',
      'WH_DBT_TRANSFORM',
      'WH_DBT_MARTS'
  )
ORDER BY
    BYTES_SCANNED DESC
LIMIT 25;


/* ============================================================================
   6. dbt query monitoring by query tag
   Works if dbt_project.yml sets +query_tag by layer.
============================================================================ */

SELECT
    QUERY_TAG,
    WAREHOUSE_NAME,
    COUNT(*) AS query_count,
    SUM(EXECUTION_TIME) / 1000 AS total_execution_seconds,
    AVG(EXECUTION_TIME) / 1000 AS avg_execution_seconds,
    SUM(BYTES_SCANNED) AS total_bytes_scanned,
    ROUND(SUM(BYTES_SCANNED) / POWER(1024, 3), 2) AS total_gb_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND QUERY_TAG IS NOT NULL
  AND WAREHOUSE_NAME IN (
      'WH_DBT_DEV',
      'WH_DBT_TRANSFORM',
      'WH_DBT_MARTS'
  )
GROUP BY
    QUERY_TAG,
    WAREHOUSE_NAME
ORDER BY
    total_execution_seconds DESC;


/* ============================================================================
   7. dbt model query monitoring
   This looks for queries generated by dbt.
============================================================================ */

SELECT
    WAREHOUSE_NAME,
    QUERY_TAG,
    COUNT(*) AS query_count,
    SUM(EXECUTION_TIME) / 1000 AS total_execution_seconds,
    AVG(EXECUTION_TIME) / 1000 AS avg_execution_seconds,
    ROUND(SUM(BYTES_SCANNED) / POWER(1024, 3), 2) AS total_gb_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND (
      QUERY_TEXT ILIKE '%dbt%'
      OR QUERY_TAG ILIKE '%dbt%'
  )
  AND WAREHOUSE_NAME IN (
      'WH_DBT_DEV',
      'WH_DBT_TRANSFORM',
      'WH_DBT_MARTS'
  )
GROUP BY
    WAREHOUSE_NAME,
    QUERY_TAG
ORDER BY
    total_execution_seconds DESC;


/* ============================================================================
   8. Warehouse auto-suspend configuration check
   Helps verify cost-control settings.
============================================================================ */

SHOW WAREHOUSES;


/* After running SHOW WAREHOUSES, inspect these fields:
   - name
   - size
   - state
   - auto_suspend
   - auto_resume
   - running
   - queued
   - is_default
*/


/* ============================================================================
   9. Warehouse usage by user
============================================================================ */

SELECT
    USER_NAME,
    WAREHOUSE_NAME,
    COUNT(*) AS query_count,
    SUM(EXECUTION_TIME) / 1000 AS total_execution_seconds,
    ROUND(SUM(BYTES_SCANNED) / POWER(1024, 3), 2) AS total_gb_scanned,
    MIN(START_TIME) AS first_query_time,
    MAX(START_TIME) AS latest_query_time
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND WAREHOUSE_NAME IN (
      'WH_DBT_DEV',
      'WH_DBT_TRANSFORM',
      'WH_DBT_MARTS'
  )
GROUP BY
    USER_NAME,
    WAREHOUSE_NAME
ORDER BY
    total_execution_seconds DESC;


/* ============================================================================
   10. Daily account-level usage summary
   This gives account-level service usage, not only warehouse usage.
============================================================================ */

SELECT
    USAGE_DATE,
    SERVICE_TYPE,
    SUM(CREDITS_USED) AS credits_used,
    SUM(CREDITS_USED_COMPUTE) AS compute_credits_used,
    SUM(CREDITS_USED_CLOUD_SERVICES) AS cloud_services_credits_used
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
WHERE USAGE_DATE >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY
    USAGE_DATE,
    SERVICE_TYPE
ORDER BY
    USAGE_DATE DESC,
    credits_used DESC;


/* ============================================================================
   11. Warehouse cost threshold check
   Change threshold as needed.
============================================================================ */

WITH warehouse_daily_usage AS (

    SELECT
        DATE_TRUNC('day', START_TIME)::DATE AS usage_date,
        WAREHOUSE_NAME,
        SUM(CREDITS_USED) AS credits_used
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE START_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP())
      AND WAREHOUSE_NAME IN (
          'WH_DBT_DEV',
          'WH_DBT_TRANSFORM',
          'WH_DBT_MARTS'
      )
    GROUP BY
        usage_date,
        WAREHOUSE_NAME

)

SELECT
    usage_date,
    warehouse_name,
    credits_used,
    CASE
        WHEN credits_used >= 5 THEN 'HIGH'
        WHEN credits_used >= 1 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS usage_risk_level
FROM warehouse_daily_usage
ORDER BY
    usage_date DESC,
    credits_used DESC;


/* ============================================================================
   12. Recommended warehouse sizing check
   Finds warehouses used by project and their current sizes.
============================================================================ */

SELECT
    NAME AS warehouse_name,
    SIZE AS warehouse_size,
    STATE AS warehouse_state,
    AUTO_SUSPEND,
    AUTO_RESUME,
    MIN_CLUSTER_COUNT,
    MAX_CLUSTER_COUNT,
    SCALING_POLICY
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));