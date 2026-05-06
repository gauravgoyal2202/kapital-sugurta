{{ config(materialized='table') }}

/*
  Dashboard 12 — Bottom Section: Sales Channel Summary
  ---------------------------------------------------
  Normalized yearly/quarterly comparison for Sales Channels,
  including filter dimensions.
*/

WITH base AS (
    SELECT
        sales_channel,
        report_year,
        report_quarter,
        report_month,
        insurance_type,
        product_category,
        product_name,
        is_anor_bank,
        SUM(premium_uzs) as premium_uzs
    FROM {{ ref('mart_sales_channel_performance') }}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

-- Total per period for share calculation (also grouped by filters)
totals AS (
    SELECT
        report_year,
        report_quarter,
        report_month,
        insurance_type,
        product_category,
        product_name,
        is_anor_bank,
        SUM(premium_uzs) as total_premium_uzs
    FROM base
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

calculations AS (
    SELECT
        b.sales_channel,
        b.report_year,
        b.report_quarter,
        b.report_month,
        b.insurance_type,
        b.product_category,
        b.product_name,
        b.is_anor_bank,
        b.premium_uzs,
        CASE WHEN t.total_premium_uzs > 0 THEN (b.premium_uzs / t.total_premium_uzs) * 100 ELSE 0 END as premium_share_pct,
        
        -- YoY Comparison (Partitioned by channel, month, AND filters)
        LAG(b.premium_uzs) OVER (
            PARTITION BY b.sales_channel, EXTRACT(MONTH FROM b.report_month), b.insurance_type, b.product_category, b.product_name, b.is_anor_bank 
            ORDER BY b.report_year
        ) as prev_year_premium_uzs,
        
        LAG(CASE WHEN t.total_premium_uzs > 0 THEN (b.premium_uzs / t.total_premium_uzs) * 100 ELSE 0 END) OVER (
            PARTITION BY b.sales_channel, EXTRACT(MONTH FROM b.report_month), b.insurance_type, b.product_category, b.product_name, b.is_anor_bank 
            ORDER BY b.report_year
        ) as prev_year_share_pct
        
    FROM base b
    JOIN totals t 
        ON t.report_year = b.report_year 
        AND t.report_quarter = b.report_quarter
        AND t.report_month = b.report_month
        AND t.insurance_type = b.insurance_type
        AND t.product_category = b.product_category
        AND t.product_name = b.product_name
        AND t.is_anor_bank = b.is_anor_bank
)

SELECT
    sales_channel,
    report_year,
    report_quarter,
    report_month,
    insurance_type,
    product_category,
    product_name,
    is_anor_bank,
    ROUND(premium_uzs::NUMERIC, 2) as premium_uzs,
    ROUND(premium_share_pct::NUMERIC, 2) as premium_share_pct,
    
    CASE 
        WHEN report_year = (SELECT MAX(report_year) FROM calculations) THEN ROUND((premium_share_pct - prev_year_share_pct)::NUMERIC, 2)
        -- Also providing values for all years if needed, but retaining 2025 focus as before:
        WHEN prev_year_share_pct IS NOT NULL THEN ROUND((premium_share_pct - prev_year_share_pct)::NUMERIC, 2)
        ELSE NULL 
    END as share_change_pp,
    
    CASE 
        WHEN prev_year_premium_uzs > 0 
        THEN ROUND(((premium_uzs - prev_year_premium_uzs) / prev_year_premium_uzs * 100)::NUMERIC, 2)
        ELSE NULL 
    END as volume_growth_pct

FROM calculations
ORDER BY sales_channel, report_month DESC
