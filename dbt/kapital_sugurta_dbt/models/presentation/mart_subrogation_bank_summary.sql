{{ config(materialized='table') }}

/*
  Dashboard 8 — Subrogation and Banks Summary
  -----------------------------------------------------
  Pre-pivoted table to exactly match the requested Power BI table visual
  comparing Year-over-Year Bank metrics.
*/

/*
  Dashboard 8 — Subrogation and Banks Summary
  -----------------------------------------------------
  Rolling Year-over-Year Bank metrics.
  For every month, it provides the current value and the value from the same month in the prior year.
  This allows Power BI to slice by any Year/Month and see the correct comparison.
*/

WITH base_data AS (
    SELECT 
        bank_partner_name,
        product_category,
        customer_segment,
        report_month,
        EXTRACT(YEAR FROM report_month) as year_num,
        EXTRACT(MONTH FROM report_month) as month_num,
        insurance_premium_volume_uzs / 1000000.0 as premium,
        agency_commission_volume_uzs / 1000000.0 as commission,
        insurance_claims_volume_uzs / 1000000.0 as claims,
        insurance_profit_volume_uzs / 1000000.0 as profit,
        recovery_from_bank_uzs / 1000000.0 as recovery,
        avg_recovery_processing_time_days as avg_processing_time
    FROM {{ ref('mart_bank_partner_performance_monthly') }}
    -- Filter out obvious data anomalies
    WHERE EXTRACT(YEAR FROM report_month) BETWEEN 2019 AND 2026
)

SELECT
    curr.bank_partner_name AS bank_name,
    curr.product_category,
    curr.customer_segment,
    curr.report_month,
    
    ROUND(curr.premium::NUMERIC, 3) AS insurance_premium_volume_curr_year_mln_uzs,
    ROUND(prev.premium::NUMERIC, 3) AS insurance_premium_volume_prev_year_mln_uzs,
    
    ROUND(curr.commission::NUMERIC, 3) AS agency_commission_volume_curr_year_mln_uzs,
    ROUND(prev.commission::NUMERIC, 3) AS agency_commission_volume_prev_year_mln_uzs,
    
    ROUND(curr.claims::NUMERIC, 3) AS insurance_claims_volume_curr_year_mln_uzs,
    ROUND(prev.claims::NUMERIC, 3) AS insurance_claims_volume_prev_year_mln_uzs,
    
    ROUND(curr.profit::NUMERIC, 3) AS insurance_profit_volume_curr_year_mln_uzs,
    ROUND(prev.profit::NUMERIC, 3) AS insurance_profit_volume_prev_year_mln_uzs,
    
    ROUND(curr.recovery::NUMERIC, 3) AS recovery_amount_from_bank_curr_year_mln_uzs,
    ROUND(prev.recovery::NUMERIC, 3) AS recovery_amount_from_bank_prev_year_mln_uzs,
    
    ROUND(curr.avg_processing_time::NUMERIC, 0) AS avg_recovery_processing_time_from_debt_occurrence_days

FROM base_data curr
LEFT JOIN base_data prev
  ON prev.bank_partner_name = curr.bank_partner_name
 AND prev.product_category = curr.product_category
 AND prev.customer_segment = curr.customer_segment
 AND prev.year_num = curr.year_num - 1
 AND prev.month_num = curr.month_num

ORDER BY curr.report_month DESC, insurance_premium_volume_curr_year_mln_uzs DESC
