/********************************************************************************
*
* Project: Hierarchical Business Metrics Reporting for BI
* Author: Taha Rafiq
* Description:
* This query aggregates key business metrics (ACV, MRR, Customer Count) across
* a multi-level product hierarchy (Category > Group > Product). It calculates
* year-over-year (YoY) growth and transforms the data into a long, "unpivoted"
* format, which is ideal for creating flexible and interactive dashboards in BI
* tools like Tableau or Power BI.
*
* This approach avoids repeating logic by creating a single base CTE and
* aggregating from it, which is more efficient and maintainable.
*
* Disclaimer:
* All table/column names and specific values have been anonymized.
*
********************************************************************************/

-- CTE 1: Create a single, clean base table with all necessary data.
-- This avoids reading the large underlying fact table multiple times.
WITH monthly_revenue_base AS (
  SELECT
    r.month_end_date,
    r.customer_id,
    h.product_category,
    h.product_group,
    h.product_name,
    r.acv,
    r.mrr
  FROM `data_warehouse.fact_monthly_customer_revenue` AS r
  LEFT JOIN `data_warehouse.dim_product_hierarchy` AS h
    ON r.product_id = h.product_id
  WHERE
    -- Filter for the relevant business unit for this analysis.
    h.product_category = 'Analytics'
),

-- CTE 2: Aggregate metrics by Product Group.
product_group_agg AS (
  SELECT
    month_end_date,
    product_group,
    COUNT(DISTINCT customer_id) AS num_customers,
    SUM(acv) AS total_acv,
    SUM(mrr) AS total_mrr
  FROM monthly_revenue_base
  GROUP BY 1, 2
),

-- CTE 3: Aggregate metrics by Product Name.
product_name_agg AS (
  SELECT
    month_end_date,
    product_name,
    COUNT(DISTINCT customer_id) AS num_customers,
    SUM(acv) AS total_acv,
    SUM(mrr) AS total_mrr
  FROM monthly_revenue_base
  GROUP BY 1, 2
),

-- CTE 4: Aggregate metrics by the top-level Product Category.
product_category_agg AS (
  SELECT
    month_end_date,
    product_category,
    COUNT(DISTINCT customer_id) AS num_customers,
    SUM(acv) AS total_acv,
    SUM(mrr) AS total_mrr
  FROM monthly_revenue_base
  GROUP BY 1, 2
),

-- CTE 5: Unpivot the aggregated data and calculate Year-over-Year (YoY) metrics.
-- This UNION ALL approach transforms the data into a long format suitable for BI tools.
metrics_unpivoted AS (
  SELECT
    month_end_date,
    'Product Group' AS aggregation_level,
    product_group AS aggregation_object,
    num_customers,
    total_acv,
    total_mrr,
    -- Use LAG window function to get the value from 12 months prior for YoY comparison.
    LAG(num_customers, 12) OVER (PARTITION BY product_group ORDER BY month_end_date) AS num_customers_prior_year,
    LAG(total_acv, 12) OVER (PARTITION BY product_group ORDER BY month_end_date) AS total_acv_prior_year,
    LAG(total_mrr, 12) OVER (PARTITION BY product_group ORDER BY month_end_date) AS total_mrr_prior_year
  FROM product_group_agg

  UNION ALL

  SELECT
    month_end_date,
    'Product Name' AS aggregation_level,
    product_name AS aggregation_object,
    num_customers,
    total_acv,
    total_mrr,
    LAG(num_customers, 12) OVER (PARTITION BY product_name ORDER BY month_end_date) AS num_customers_prior_year,
    LAG(total_acv, 12) OVER (PARTITION BY product_name ORDER BY month_end_date) AS total_acv_prior_year,
    LAG(total_mrr, 12) OVER (PARTITION BY product_name ORDER BY month_end_date) AS total_mrr_prior_year
  FROM product_name_agg

  UNION ALL

  SELECT
    month_end_date,
    'Product Category' AS aggregation_level,
    product_category AS aggregation_object,
    num_customers,
    total_acv,
    total_mrr,
    LAG(num_customers, 12) OVER (PARTITION BY product_category ORDER BY month_end_date) AS num_customers_prior_year,
    LAG(total_acv, 12) OVER (PARTITION BY product_category ORDER BY month_end_date) AS total_acv_prior_year,
    LAG(total_mrr, 12) OVER (PARTITION BY product_category ORDER BY month_end_date) AS total_mrr_prior_year
  FROM product_category_agg
)

-- Final SELECT: Calculate YoY growth percentages and present the final table.
SELECT
  m.month_end_date,
  m.aggregation_level,
  m.aggregation_object,
  m.num_customers,
  m.total_acv,
  m.total_mrr,
  m.num_customers_prior_year,
  m.total_acv_prior_year,
  m.total_mrr_prior_year,
  -- Calculate YoY growth rates, using SAFE_DIVIDE to prevent division-by-zero errors.
  SAFE_DIVIDE(m.num_customers - m.num_customers_prior_year, m.num_customers_prior_year) AS yoy_growth_customers,
  SAFE_DIVIDE(m.total_acv - m.total_acv_prior_year, m.total_acv_prior_year) AS yoy_growth_acv,
  SAFE_DIVIDE(m.total_mrr - m.total_mrr_prior_year, m.total_mrr_prior_year) AS yoy_growth_mrr
FROM metrics_unpivoted AS m
ORDER BY
  m.month_end_date DESC,
  m.aggregation_level,
  m.aggregation_object;
