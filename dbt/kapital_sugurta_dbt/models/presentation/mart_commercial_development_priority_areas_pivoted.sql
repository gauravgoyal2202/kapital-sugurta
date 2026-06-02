{{ config(materialized='table') }}

/*
  Dashboard 11 — Commercial Development: Priority Areas (PIVOTED)
  --------------------------------------------------------------
  Passes all base columns through without pivoting or filtering by priority_area.
  Slicing by priority area (Motor / Banking / Property) is done at the DAX level.
*/

SELECT
    report_month,
    report_year,
    priority_area,
    channels,
    co_premium,
    co_claims,
    co_expenses,
    co_fifty,
    co_ras,
    profitability_pct,
    mkt_prem_prev_year_bn,
    mkt_prem_curr_year_bn,
    company_market_share_pct,
    'Actual' AS scenario
FROM {{ ref('mart_commercial_development_priority_areas_base') }}
ORDER BY report_month DESC, priority_area
