/********************************************************************************
*
* Project: Partner Usage Reporting for AI/ML Services
* Author: Taha Rafiq
* Description:
* This script generates a weekly summary of AI model usage data for a specific
* technology partner. It aggregates key performance metrics and calculates
* percentiles for call volumes to provide insights into usage patterns.
* Customer identifiers are hashed to ensure privacy.
*
* Disclaimer:
* All identifiers (tables, columns, etc.) and specific values have been
* anonymized to protect proprietary information. The query's logical
* structure is representative of a real-world production script.
*
********************************************************************************/


-- CTE 1: Define the reporting time window.
-- This logic dynamically selects all dates from the previous calendar month
-- and calculates their corresponding week-start date for weekly aggregation.
WITH eligible_dates AS (
  SELECT
    d.date,
    -- This calculation snaps any date to its corresponding Monday of the week.
    DATE_ADD(
      d.first_day_of_month,
      INTERVAL CAST(FLOOR(DATE_DIFF(d.date, d.first_day_of_month, DAY) / 7) AS INT64) * 7 DAY
    ) AS first_day_of_week
  FROM `shared_utils.dim_date` AS d 
  WHERE
    -- Set the reporting period to the last full calendar month.
    d.date BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
           AND LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
),

-- CTE 2: Calculate hourly API call volume.
-- This aggregates raw logs into hourly chunks, forming the basis for percentile calculations.
hourly_volume AS (
  SELECT
    d.first_day_of_week AS week_start_date,
    -- Calculate the week_end_date, ensuring it doesn't cross into the next month.
    DATE(LEAST(
      DATE_ADD(d.first_day_of_week, INTERVAL 6 DAY),
      LAST_DAY(d.first_day_of_week)
    )) AS week_end_date,
    -- Hashing the account ID with a unique salt to create a privacy-preserving customer key.
    TO_HEX(SHA256(CONCAT('a_secret_salt_for_reporting', CAST(p.account_id AS STRING)))) AS customer_uuid,
    p.model_name AS model,
    c.industry_vertical,
    c.billing_country,
    DATETIME_TRUNC(p.event_timestamp, HOUR) AS hour_bucket,
    -- The source data is sampled, so we SUM the sample weight to reconstruct total call volume.
    SUM(p.call_weight_from_sampling) AS hourly_api_calls
  FROM `product_db.fact_llm_inference_logs` AS p
  INNER JOIN eligible_dates AS d
    ON p.event_date = d.date
  -- Join to a customer dimension table to enrich with segmentation data.
  LEFT JOIN `data_warehouse.dim_customer_accounts` AS c
    ON p.account_id = c.account_id
  WHERE
    -- Filter for successful API calls for a specific partner's models.
    (p.model_name LIKE 'partner-a/model-family-x-%' OR p.model_name LIKE 'partner-a/model-family-y-%')
    AND p.error_code = 0
  GROUP BY 1, 2, 3, 4, 5, 6, 7
),

-- CTE 3: Calculate weekly call volume percentiles per customer and model.
-- This helps the partner understand the distribution of usage, not just the total.
percentile_summary AS (
  SELECT
    week_start_date,
    customer_uuid,
    model,
    industry_vertical,
    billing_country,
    -- Using approximate quantiles is highly efficient for large datasets.
    APPROX_QUANTILES(hourly_api_calls, 100)[OFFSET(20)] AS p20_hourly_call_volume,
    APPROX_QUANTILES(hourly_api_calls, 100)[OFFSET(50)] AS p50_hourly_call_volume, -- Median
    APPROX_QUANTILES(hourly_api_calls, 100)[OFFSET(80)] AS p80_hourly_call_volume,
    MAX(hourly_api_calls) AS peak_hourly_call_volume
  FROM hourly_volume
  GROUP BY 1, 2, 3, 4, 5
),

-- CTE 4: Calculate weekly aggregate performance metrics.
main_summary AS (
  SELECT
    d.first_day_of_week AS week_start_date,
    DATE(LEAST(
      DATE_ADD(d.first_day_of_week, INTERVAL 6 DAY),
      LAST_DAY(d.first_day_of_week)
    )) AS week_end_date,
    TO_HEX(SHA256(CONCAT('a_secret_salt_for_reporting', CAST(p.account_id AS STRING)))) AS customer_uuid,
    p.model_name AS model,
    SUM(p.input_tokens) AS total_input_tokens,
    SUM(p.output_tokens) AS total_output_tokens,
    SUM(p.call_weight_from_sampling) AS total_call_volume,
    AVG(p.time_to_first_token_ms) AS avg_ttft_ms,
    AVG(p.inference_duration_ms) AS avg_inference_time_ms
  FROM `product_db.fact_llm_inference_logs` AS p
  INNER JOIN eligible_dates AS d
    ON p.event_date = d.date
  WHERE
    (p.model_name LIKE 'partner-a/model-family-x-%' OR p.model_name LIKE 'partner-a/model-family-y-%')
    AND p.error_code = 0
  GROUP BY 1, 2, 3, 4
)

-- Final SELECT: Join the aggregate metrics and percentile data for the final report.
SELECT
  LOWER(FORMAT_DATE('%B_%Y', m.week_start_date)) AS report_month,
  m.week_start_date AS report_week_start,
  m.week_end_date AS report_week_end,
  'cloud_hosted' AS deployment_type,
  m.model,
  ps.industry_vertical,
  ps.billing_country,
  m.customer_uuid,
  m.total_call_volume,
  m.total_input_tokens,
  m.total_output_tokens,
  ps.p20_hourly_call_volume,
  ps.p50_hourly_call_volume,
  ps.p80_hourly_call_volume,
  ps.peak_hourly_call_volume,
  m.avg_ttft_ms,
  m.avg_inference_time_ms
FROM main_summary AS m
LEFT JOIN percentile_summary AS ps
  ON m.week_start_date = ps.week_start_date
  AND m.customer_uuid = ps.customer_uuid
  AND m.model = ps.model
ORDER BY
  report_week_start,
  customer_uuid;