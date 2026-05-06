{{ config(materialized='table') }}

/*
  Dashboard 11 — Commercial Development: Priority Areas
  ------------------------------------------------------
  Refactored version: Built from Curated Layer.
  Groups metrics by Area: Auto, Banking, Property.
*/

WITH month_params AS (
    SELECT 
        DATE_TRUNC('month', d)::DATE as report_month,
        EXTRACT(YEAR FROM d)::INT as report_year
    FROM generate_series('2024-01-01'::DATE, '2025-12-31'::DATE, '1 month'::interval) d
),

-- 1. Area Mapping (from Curated)
mapping AS (
    SELECT * FROM {{ ref('curated_priority_area_mapping') }}
),

-- 2. Company Premiums (from Curated)
co_prem AS (
    SELECT
        DATE_TRUNC('month', payment_date)::DATE as report_month,
        m.priority_area,
        SUM(premium_amount) as prem_uzs
    FROM {{ ref('curated_insurance_premium') }} p
    JOIN mapping m ON m.pturi_id = p.pturi_id
    GROUP BY 1, 2
),

-- 3. Company Claims (from Curated)
co_claims AS (
    SELECT
        DATE_TRUNC('month', payment_date)::DATE as report_month,
        m.priority_area,
        SUM(claim_amount) as claims_uzs
    FROM {{ ref('curated_insurance_claims') }} c
    JOIN mapping m ON m.pturi_id = c.pturi_id
    GROUP BY 1, 2
),

-- 4. Company Expenses (from Curated)
co_expenses_ytd AS (
    SELECT
        DATE_TRUNC('month', report_date)::DATE as report_month,
        period_expenses as ytd_expenses_uzs
    FROM {{ ref('curated_financial_performance') }}
),

co_expenses AS (
    SELECT
        report_month,
        ytd_expenses_uzs - LAG(ytd_expenses_uzs, 1, 0) OVER (PARTITION BY EXTRACT(YEAR FROM report_month) ORDER BY report_month) as total_expenses_uzs
    FROM co_expenses_ytd
),

-- 5. Total Company Premium (from Curated - for allocation denominator)
co_total_prem AS (
    SELECT
        DATE_TRUNC('month', payment_date)::DATE as report_month,
        SUM(premium_amount) as total_prem_uzs
    FROM {{ ref('curated_insurance_premium') }}
    GROUP BY 1
),

-- 6. Market Data (from Raw - No curated layer for regulatory Excel yet)
mkt_agg AS (
    SELECT
        CASE 
            WHEN insurance_type_name ILIKE '%3-klass%' OR insurance_type_name ILIKE '%10-klass%' 
                 OR insurance_type_name ILIKE '%Avtofuqarolik%' THEN 'Motor'
            WHEN insurance_type_name ILIKE '%14-klass%' OR insurance_type_name ILIKE '%15-klass%' 
                 OR insurance_type_name ILIKE '%16-klass%' OR insurance_type_name ILIKE '%3,14 klass%' 
                 THEN 'Banking'
            WHEN insurance_type_name ILIKE '%8-klass%' OR insurance_type_name ILIKE '%9-klass%' 
                 OR insurance_type_name ILIKE '%qurilish-montaj%' THEN 'Property'
            ELSE 'Other'
        END as priority_area,
        -- Mapping Excel's fixed columns to relative periods
        SUM(COALESCE(total_premium_2024, 0)) / 1000.0 as mkt_prem_prev_year_bn,
        SUM(COALESCE(total_premium_2025, 0)) / 1000.0 as mkt_prem_curr_year_bn
    FROM {{ source('raw', 'market_share_insurance_class_stats') }}
    GROUP BY 1
),

-- 7. Combine All Company data
co_daily AS (
    SELECT
        mp.report_month,
        mp.report_year,
        a.area as priority_area,
        COALESCE(pr.prem_uzs, 0) as prem_uzs,
        COALESCE(cl.claims_uzs, 0) as claims_uzs,
        COALESCE(tp.total_prem_uzs, 0) as total_company_prem_month,
        ex.total_expenses_uzs
    FROM month_params mp
    CROSS JOIN (SELECT 'Motor' as area UNION ALL SELECT 'Banking' UNION ALL SELECT 'Property') a
    LEFT JOIN co_prem pr ON pr.report_month = mp.report_month AND pr.priority_area = a.area
    LEFT JOIN co_claims cl ON cl.report_month = mp.report_month AND cl.priority_area = a.area
    LEFT JOIN co_expenses ex ON ex.report_month = mp.report_month
    LEFT JOIN co_total_prem tp ON tp.report_month = mp.report_month
),

-- 8. Final Calculations
final AS (
    SELECT
        d.report_month,
        d.report_year,
        d.priority_area,
        ROUND((d.prem_uzs / 1e9)::NUMERIC, 3) as co_premium_bn,
        ROUND((d.claims_uzs / 1e9)::NUMERIC, 3) as co_claims_bn,
        
        CASE 
            WHEN d.prem_uzs = 0 THEN NULL
            ELSE ROUND(
                (d.claims_uzs + (CASE WHEN d.total_company_prem_month = 0 THEN 0 ELSE (d.prem_uzs / d.total_company_prem_month) * COALESCE(d.total_expenses_uzs, 0) END)) 
                / d.prem_uzs * 100
                , 2)
        END as combined_ratio_pct,
        
        m.mkt_prem_prev_year_bn,
        m.mkt_prem_curr_year_bn,
        
        CASE 
            WHEN d.report_year = EXTRACT(YEAR FROM CURRENT_DATE) - 1 AND m.mkt_prem_prev_year_bn > 0 THEN ROUND((d.prem_uzs / 1e9) / m.mkt_prem_prev_year_bn * 100, 2)
            WHEN d.report_year = EXTRACT(YEAR FROM CURRENT_DATE) AND m.mkt_prem_curr_year_bn > 0 THEN ROUND((d.prem_uzs / 1e9) / m.mkt_prem_curr_year_bn * 100, 2)
            ELSE NULL
        END as company_market_share_pct
        
    FROM co_daily d
    LEFT JOIN mkt_agg m ON m.priority_area = d.priority_area
)

SELECT * FROM final
ORDER BY report_month DESC, priority_area
