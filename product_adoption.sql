/********************************************************************************
*
* Project: Product Adoption Metrics for Security Features
* Author: Taha Rafiq
* Description:
* This query calculates daily product adoption metrics for a suite of technology
* features. It measures adoption at three entity levels - user, account and 
* organization, comparing the number of adopting entities to the total number of
* active entities.
*
* This is a simplified version of a much larger production query. It has been
* refactored to demonstrate the core logic and advanced SQL techniques (CTEs,
* window functions, complex joins, data unpivoting) in a more concise
* and readable format.
*
* Disclaimer:
* All table/column names and specific values have been anonymized.
*
********************************************************************************/

-- CTE 1: Define a dynamic date for the reporting period.
WITH date_references AS (
  SELECT
    DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS report_date
),

-- CTE 2: Establish the base population of all active users, accounts, and orgs.
-- This serves as the denominator for our adoption percentage calculation.
base_active_entities AS (
  SELECT
    z.subscription_plan,
    z.user_id,
    a.account_id,
    COALESCE(c.crm_organization_id, CAST(a.account_id AS STRING)) AS organization_id
  FROM `data_warehouse.dim_users` AS z
  INNER JOIN `data_warehouse.dim_accounts` AS a
    ON z.account_id = a.account_id AND a.is_active = TRUE
  LEFT JOIN `data_warehouse.dim_orgs` AS c
    ON a.organization_id = c.organization_id
  WHERE
    z.is_active = TRUE
),

-- CTE 3: Determine feature adoption flags for each user.
-- This CTE joins the base population to various feature usage tables to create
-- a single wide table with a boolean flag for each feature.
feature_adoption_flags AS (
  SELECT
    b.subscription_plan,
    b.user_id,
    b.account_id,
    b.organization_id,

    -- Feature Flag 1: Adoption of a specific heuristic firewall rule.
    IFNULL(rules.uses_heuristic_rule, FALSE) AS is_adopted_ai_powered_waf_rule,

    -- Feature Flag 2: Adoption of volumetric traffic protection (proxied traffic).
    IF(proxy.user_id IS NOT NULL, TRUE, FALSE) AS is_adopted_volumetric_protection,

    -- Feature Flag 3: Adoption of a specific client-side analysis setting.
    IF(settings.is_js_challenge_enabled, TRUE, FALSE) AS is_adopted_client_side_analysis,

    -- Feature Flag 4: User interaction with a specific UI dashboard.
    IF(interactions.user_id IS NOT NULL, TRUE, FALSE) AS is_adopted_security_dashboard_ui

  FROM base_active_entities AS b

  -- Join for heuristic rule adoption
  LEFT JOIN (
    SELECT user_id, LOGICAL_OR(uses_heuristic_rule) AS uses_heuristic_rule
    FROM `product_db.fact_firewall_rules_daily`
    WHERE event_date = (SELECT report_date FROM date_references)
    GROUP BY 1
  ) AS rules ON b.user_id = rules.user_id

  -- Join for analytics dashboard interaction
  LEFT JOIN (
    SELECT DISTINCT user_id
    FROM `product_db.fact_analytics_records_daily`
    WHERE is_proxied = TRUE
      AND event_date = (SELECT report_date FROM date_references)
  ) AS proxy ON b.user_id = proxy.user_id

  -- Join for company settings adoption
  LEFT JOIN `product_db.comany_settings` AS settings
    ON b.user_id = settings.user_id
    AND settings.is_js_challenge_enabled = TRUE

  -- Join for UI interaction adoption (past 7 days)
  LEFT JOIN (
    SELECT DISTINCT properties.user_id
    FROM `analytics_db.dashboard_interactions`
    WHERE event_date BETWEEN DATE_SUB((SELECT report_date FROM date_references), INTERVAL 7 DAY) AND (SELECT report_date FROM date_references)
      AND properties.page_url LIKE '%/security/analytics'
  ) AS interactions ON b.user_id = interactions.user_id
),

-- CTE 4: Unpivot the data to calculate adoption counts for each feature.
-- This transforms the wide flag table into a long, clean format, which is
-- much more scalable than having hundreds of columns in the final output.
unpivoted_adoption_counts AS (
  SELECT 'Heuristic Rule' AS metric_name, subscription_plan, COUNT(DISTINCT user_id) AS adopted_users, COUNT(DISTINCT account_id) AS adopted_accounts, COUNT(DISTINCT organization_id) AS adopted_orgs FROM feature_adoption_flags WHERE is_adopted_ai_powered_waf_rule GROUP BY 1, 2
  UNION ALL
  SELECT 'Analytics Dashboard' AS metric_name, subscription_plan, COUNT(DISTINCT user_id) AS adopted_users, COUNT(DISTINCT account_id) AS adopted_accounts, COUNT(DISTINCT organization_id) AS adopted_orgs FROM feature_adoption_flags WHERE is_adopted_volumetric_protection GROUP BY 1, 2
  UNION ALL
  SELECT 'Company Setting' AS metric_name, subscription_plan, COUNT(DISTINCT user_id) AS adopted_users, COUNT(DISTINCT account_id) AS adopted_accounts, COUNT(DISTINCT organization_id) AS adopted_orgs FROM feature_adoption_flags WHERE is_adopted_client_side_analysis GROUP BY 1, 2
  UNION ALL
  SELECT 'UI Interactions' AS metric_name, subscription_plan, COUNT(DISTINCT user_id) AS adopted_users, COUNT(DISTINCT account_id) AS adopted_accounts, COUNT(DISTINCT organization_id) AS adopted_orgs FROM feature_adoption_flags WHERE is_adopted_security_dashboard_ui GROUP BY 1, 2
),

-- CTE 5: Calculate the total counts of active entities.
-- The ROLLUP creates a subtotal for each subscription_plan and a grand total ('All').
total_counts AS (
  SELECT
    CASE WHEN GROUPING(subscription_plan) = 1 THEN 'All' ELSE subscription_plan END AS subscription_plan,
    COUNT(DISTINCT user_id) AS total_users,
    COUNT(DISTINCT account_id) AS total_accounts,
    COUNT(DISTINCT organization_id) AS total_orgs
  FROM base_active_entities
  GROUP BY ROLLUP(subscription_plan)
)

-- Final SELECT: Join the adoption counts with total counts to produce the final report.
SELECT
  (SELECT report_date FROM date_references) AS report_date,
  'Adopted vs. Total Active' AS scenario,
  totals.subscription_plan,
  counts.metric_name,
  -- Adopted Counts
  IFNULL(counts.adopted_users, 0) AS adopted_users,
  IFNULL(counts.adopted_accounts, 0) AS adopted_accounts,
  IFNULL(counts.adopted_orgs, 0) AS adopted_orgs,
  -- Total Counts
  totals.total_users,
  totals.total_accounts,
  totals.total_orgs,
  -- Adoption Percentages
  SAFE_DIVIDE(IFNULL(counts.adopted_users, 0), totals.total_users) AS pct_adoption_users,
  SAFE_DIVIDE(IFNULL(counts.adopted_accounts, 0), totals.total_accounts) AS pct_adoption_accounts,
  SAFE_DIVIDE(IFNULL(counts.adopted_orgs, 0), totals.total_orgs) AS pct_adoption_orgs
FROM total_counts AS totals
LEFT JOIN unpivoted_adoption_counts AS counts
  ON totals.subscription_plan = counts.subscription_plan
ORDER BY
  totals.subscription_plan,
  counts.metric_name;