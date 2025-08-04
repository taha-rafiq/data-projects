/********************************************************************************
*
* Project: Sales Performance Dashboard for a Key Industry Vertical
* Author: Taha Rafiq
* Description:
* This query powers a dashboard that reports on sales performance against
* targets for the Financial Services vertical. It aggregates opportunity data
* from a CRM system (e.g., Salesforce) and compares it to predefined sales
* targets. The query calculates metrics for multiple timeframes: previous year,
* year-to-date (YTD), quarter-to-date (QTD), and month-to-date (MTD).
*
* Disclaimer:
* All table/column names and specific values have been anonymized. The logic,
* especially the dynamic date calculations, is representative of my work in
* creating scalable and maintainable BI solutions.
*
********************************************************************************/

-- CTE 1: Define the customer cohort for this analysis.
-- This combines a broad list of all customers in the vertical with a manually
-- curated list of high-priority, strategic accounts.
WITH strategic_customer_cohort AS (
  -- Tier 2: All customers within the specified vertical
  SELECT DISTINCT
    crm_account_id,
    account_name,
    'Tier 2' AS priority_level
  FROM `data_warehouse.dim_customer`
  WHERE
    industry_vertical = 'Financial Services'
    AND crm_account_id IS NOT NULL

  UNION ALL

  -- Tier 1: A curated list of high-priority, named accounts for the GTM strategy
  SELECT
    crm_account_id,
    account_name,
    'Tier 1' AS priority_level
  FROM `sales_planning.strategic_accounts`
  WHERE
    fiscal_year = EXTRACT(YEAR FROM CURRENT_DATE())
),

-- CTE 2: Create a dynamic date reference table.
-- Using a date dimension and CURRENT_DATE() makes the query reusable over time
-- without needing to hardcode new dates each year.
date_references AS (
  SELECT
    d.date AS as_of_date,
    d.first_day_of_month AS first_day_of_current_month,
    d.first_day_of_quarter AS first_day_of_current_quarter,
    DATE_TRUNC(d.date, YEAR) AS first_day_of_current_year,
    DATE_TRUNC(DATE_SUB(d.date, INTERVAL 1 YEAR), YEAR) AS first_day_of_prior_year,
    DATE_ADD(DATE_TRUNC(DATE_SUB(d.date, INTERVAL 1 YEAR), YEAR), INTERVAL 1 YEAR) - INTERVAL 1 DAY AS last_day_of_prior_year,
    EXTRACT(QUARTER FROM d.date) AS current_quarter_number
  FROM `shared_utils.dim_date` AS d
  WHERE
    -- Run the report based on yesterday's data to ensure all data is settled.
    d.date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
),

-- CTE 3: Aggregate all opportunity metrics from the CRM.
-- This pre-calculates all actuals (pipeline created, deals won) per account
-- across all relevant timeframes.
opportunity_actuals AS (
  SELECT
    o.crm_account_id,

    -- Previous Full Year Metrics
    SUM(CASE WHEN o.pipeline_creation_date BETWEEN (SELECT first_day_of_prior_year FROM date_references) AND (SELECT last_day_of_prior_year FROM date_references) THEN o.annual_contract_value ELSE 0 END) AS prior_year_pipeline_acv,
    SUM(CASE WHEN o.stage_name = 'Closed Won' AND o.close_date BETWEEN (SELECT first_day_of_prior_year FROM date_references) AND (SELECT last_day_of_prior_year FROM date_references) THEN o.annual_contract_value ELSE 0 END) AS prior_year_closed_won_acv,

    -- Current Year-to-Date (YTD) Metrics
    SUM(CASE WHEN o.pipeline_creation_date >= (SELECT first_day_of_current_year FROM date_references) THEN o.annual_contract_value ELSE 0 END) AS ytd_pipeline_acv,
    SUM(CASE WHEN o.stage_name = 'Closed Won' AND o.close_date >= (SELECT first_day_of_current_year FROM date_references) THEN o.annual_contract_value ELSE 0 END) AS ytd_closed_won_acv,

    -- Current Quarter-to-Date (QTD) Metrics
    SUM(CASE WHEN o.pipeline_creation_date >= (SELECT first_day_of_current_quarter FROM date_references) THEN o.annual_contract_value ELSE 0 END) AS qtd_pipeline_acv,
    SUM(CASE WHEN o.stage_name = 'Closed Won' AND o.close_date >= (SELECT first_day_of_current_quarter FROM date_references) THEN o.annual_contract_value ELSE 0 END) AS qtd_closed_won_acv,

    -- Current Month-to-Date (MTD) Metrics
    SUM(CASE WHEN o.pipeline_creation_date >= (SELECT first_day_of_current_month FROM date_references) THEN o.annual_contract_value ELSE 0 END) AS mtd_pipeline_acv,
    SUM(CASE WHEN o.stage_name = 'Closed Won' AND o.close_date >= (SELECT first_day_of_current_month FROM date_references) THEN o.annual_contract_value ELSE 0 END) AS mtd_closed_won_acv

  FROM `crm_raw.opportunity` AS o
  WHERE
    -- Filter for relevant opportunity types that contribute to sales goals.
    o.opportunity_type IN ('New Business', 'Upsell', 'Cross-sell', 'Renewal')
  GROUP BY
    1
),

-- CTE 4: Aggregate sales targets.
-- This pulls the pre-defined sales goals for the vertical.
vertical_targets AS (
  SELECT
    SUM(target_acv) AS total_annual_target_acv,
    SUM(CASE WHEN EXTRACT(QUARTER FROM target_month) = (SELECT current_quarter_number FROM date_references) THEN target_acv ELSE 0 END) AS current_quarter_target_acv,
    SUM(CASE WHEN DATE_TRUNC(target_month, MONTH) = (SELECT first_day_of_current_month FROM date_references) THEN target_acv ELSE 0 END) AS current_month_target_acv
  FROM `sales_planning.vertical_targets`
  WHERE
    target_vertical = 'Financial Services'
    AND EXTRACT(YEAR FROM target_month) = EXTRACT(YEAR FROM CURRENT_DATE())
)

-- Final SELECT: Combine actuals and targets for the final dashboard output.
SELECT
  (SELECT as_of_date FROM date_references) AS report_date,
  c.priority_level,

  -- Actuals (Aggregated from opportunity data)
  SUM(o.prior_year_pipeline_acv) AS total_prior_year_pipeline_acv,
  SUM(o.prior_year_closed_won_acv) AS total_prior_year_closed_won_acv,
  SUM(o.ytd_pipeline_acv) AS total_ytd_pipeline_acv,
  SUM(o.ytd_closed_won_acv) AS total_ytd_closed_won_acv,
  SUM(o.qtd_pipeline_acv) AS total_qtd_pipeline_acv,
  SUM(o.qtd_closed_won_acv) AS total_qtd_closed_won_acv,
  SUM(o.mtd_pipeline_acv) AS total_mtd_pipeline_acv,
  SUM(o.mtd_closed_won_acv) AS total_mtd_closed_won_acv,

  -- Targets (CROSS JOINed to apply to all rows)
  t.total_annual_target_acv,
  t.current_quarter_target_acv,
  t.current_month_target_acv,

  -- Performance vs. Target Calculations
  SAFE_DIVIDE(SUM(o.ytd_closed_won_acv), t.total_annual_target_acv) AS ytd_percent_to_annual_target,
  SAFE_DIVIDE(SUM(o.qtd_closed_won_acv), t.current_quarter_target_acv) AS qtd_percent_to_quarter_target,
  SAFE_DIVIDE(SUM(o.mtd_closed_won_acv), t.current_month_target_acv) AS mtd_percent_to_month_target

FROM strategic_customer_cohort AS c
LEFT JOIN opportunity_actuals AS o
  ON c.crm_account_id = o.crm_account_id
-- CROSS JOIN is used intentionally here to apply the single row of overall targets
-- to each customer segment being aggregated.
CROSS JOIN vertical_targets AS t
GROUP BY
  1, 2, t.total_annual_target_acv, t.current_quarter_target_acv, t.current_month_target_acv
ORDER BY
  report_date, priority_level;