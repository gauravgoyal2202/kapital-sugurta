{{ config(materialized='table') }}

/*
  Dashboard 15 — Claims Settlement Speed (Bottom Chart)
  -----------------------------------------------------
  Aggregates the duration (in days) required to settle a claim.
*/

WITH monthly_settlement AS (
    SELECT
        EXTRACT(YEAR FROM report_month)::INT as report_year,
        EXTRACT(QUARTER FROM report_month)::INT as report_quarter,
        report_month,
        insurance_type,
        product_category,
        product_name,
        ROUND(AVG(settlement_days)::NUMERIC, 2) as avg_settlement_days,
        ROUND(MIN(settlement_days)::NUMERIC, 2) as min_settlement_days,
        ROUND(MAX(settlement_days)::NUMERIC, 2) as max_settlement_days,
        COUNT(claim_event_id) as total_claims_settled,
        'Actual' as scenario
    FROM {{ ref('curated_claims_settlement') }}
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT * FROM monthly_settlement
ORDER BY report_month DESC
