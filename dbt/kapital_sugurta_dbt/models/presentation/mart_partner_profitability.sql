{{ config(materialized='table') }}

/*
  Dashboard 14 — Partner Profitability
  ------------------------------------
  Aggregates performance by specific partner and adds 
  profitability ratios. Supported in mln UZS.
*/

WITH monthly_partner_agg AS (
    SELECT
        partner_id,
        partner_full_name,
        partner_category,
        DATE_TRUNC('month', report_date)::DATE as report_month,
        insurance_type,
        product_category,
        product_name,
        SUM(premium_amount) as premium_uzs,
        SUM(commission_amount) as commission_uzs,
        SUM(claims_amount) as claims_uzs,
        SUM(reinsurance_amount) as reinsurance_uzs
    FROM {{ ref('curated_partner_indicators') }}
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT
    partner_id,
    partner_full_name as partner,
    partner_category,
    report_month,
    EXTRACT(YEAR FROM report_month)::INT as report_year,
    EXTRACT(QUARTER FROM report_month)::INT as report_quarter,
    insurance_type,
    product_category,
    product_name,
    
    -- Metrics in mln UZS
    ROUND((premium_uzs / 1000000.0)::NUMERIC, 2) as insurance_premium_volume_mln,
    ROUND((commission_uzs / 1000000.0)::NUMERIC, 2) as agency_commission_volume_mln,
    ROUND((claims_uzs / 1000000.0)::NUMERIC, 2) as insurance_claims_volume_mln,
    ROUND((reinsurance_uzs / 1000000.0)::NUMERIC, 2) as other_payments_volume_mln,
    
    -- Profitability Calculation
    CASE 
        WHEN premium_uzs > 0 
        THEN ROUND(((premium_uzs - commission_uzs - claims_uzs - reinsurance_uzs) / premium_uzs * 100)::NUMERIC, 2)
        ELSE 0 
    END as profitability_pct

FROM monthly_partner_agg
ORDER BY report_month DESC, premium_uzs DESC
