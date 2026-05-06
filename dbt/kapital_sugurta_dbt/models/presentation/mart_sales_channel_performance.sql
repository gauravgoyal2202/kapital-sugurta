{{ config(materialized='table') }}

/*
  Dashboard 12 & 13 — Sales Channel Performance
  --------------------------------------------
  Unified model supporting both Group level (Partner/Agency/Own) 
  and Detailed level (Banks/Web/Branch) metrics.
*/

WITH base AS (
    SELECT
        payment_date,
        DATE_TRUNC('month', payment_date)::DATE as report_month,
        EXTRACT(YEAR FROM payment_date)::INT as report_year,
        EXTRACT(QUARTER FROM payment_date)::INT as report_quarter,
        channel_group,
        sales_channel,
        insurance_type,
        product_category,
        product_name,
        is_anor_bank,
        premium_amount,
        commission_amount
    FROM {{ ref('curated_sales_channels') }}
),

-- Aggregate by Category & Channel
monthly_agg AS (
    SELECT
        report_month,
        report_year,
        report_quarter,
        channel_group,
        sales_channel,
        insurance_type,
        product_category,
        product_name,
        is_anor_bank,
        SUM(premium_amount) as premium_uzs,
        SUM(commission_amount) as expense_uzs
    FROM base
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

-- Calculation of Shares
total_monthly AS (
    SELECT
        report_month,
        insurance_type,
        product_category,
        product_name,
        is_anor_bank,
        SUM(premium_uzs) as total_company_premium_uzs
    FROM monthly_agg
    GROUP BY 1, 2, 3, 4, 5
),

-- Joining back for shares
final AS (
    SELECT
        m.*,
        t.total_company_premium_uzs,
        CASE 
            WHEN t.total_company_premium_uzs > 0 THEN (m.premium_uzs / t.total_company_premium_uzs) * 100 
            ELSE 0 
        END as premium_share_pct
    FROM monthly_agg m
    JOIN total_monthly t 
        ON t.report_month = m.report_month
        AND t.insurance_type = m.insurance_type
        AND t.product_category = m.product_category
        AND t.product_name = m.product_name
        AND t.is_anor_bank = m.is_anor_bank
)

SELECT * FROM final
ORDER BY report_month DESC, channel_group, sales_channel
