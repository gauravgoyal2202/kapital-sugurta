{{ config(materialized='table') }}

WITH date_bounds AS (
    -- Dynamically find the earliest date in your raw data
    SELECT 
        MIN(deposit_start_date) AS min_date
    FROM {{ ref('curated_oracle_deposits') }}
),

months AS (
    -- Dynamically generate an end-of-month date spine
    SELECT 
        (DATE_TRUNC('month', dt) + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS report_month
    FROM date_bounds,
    LATERAL generate_series(
        DATE_TRUNC('month', min_date), 
        (DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1 year' - INTERVAL '1 day')::DATE, 
        INTERVAL '1 month'
    ) AS dt
),

oracle_active_deposits AS (
    SELECT 
        m.report_month,
        'Deposits' AS portfolio_category,
        c.partner_name,
        SUM(c.deposit_amount) AS portfolio_amount
    FROM months m
    LEFT JOIN {{ ref('curated_oracle_deposits') }} c
        ON c.deposit_start_date <= m.report_month
        AND (c.deposit_end_date IS NULL OR c.deposit_end_date > m.report_month)
    WHERE c.partner_name IS NOT NULL
    GROUP BY 1, 2, 3
),

oracle_active_loans AS (
    SELECT 
        m.report_month,
        'Other' AS portfolio_category,
        c.client_name AS partner_name,
        SUM(c.loan_amount) AS portfolio_amount
    FROM months m
    LEFT JOIN {{ ref('curated_oracle_loans') }} c
        ON c.loan_start_date <= m.report_month
        AND (c.loan_end_date IS NULL OR c.loan_end_date > m.report_month)
    WHERE c.client_name IS NOT NULL
    GROUP BY 1, 2, 3
),

portfolio_combined AS (
    SELECT report_month, portfolio_category, partner_name, portfolio_amount FROM oracle_active_deposits
    UNION ALL
    SELECT report_month, portfolio_category, partner_name, portfolio_amount FROM oracle_active_loans
),

api_income_aggregated AS (
    SELECT
        (DATE_TRUNC('month', c.report_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS report_month,
        CASE 
            WHEN c.investment_type IN ('DEPOSIT', 'FX_DEPOSIT') THEN 'Deposits'
            WHEN c.investment_type = 'SHARE' THEN 'Shares'
            WHEN c.investment_type = 'BOND' THEN 'Bonds'
            WHEN c.investment_type = 'LOAN' THEN 'Other'
        END AS portfolio_category,
        c.partner_name,
        SUM(c.amount) AS income_amount
    FROM {{ ref('curated_investment_activity') }} c
    WHERE c.partner_name IS NOT NULL
    GROUP BY 1, 2, 3
),

-- Standardize names to ensure joining works if names slightly mismatch (Optional text cleaning)
partner_spine AS (
    SELECT report_month, portfolio_category, partner_name FROM portfolio_combined
    UNION
    SELECT report_month, portfolio_category, partner_name FROM api_income_aggregated
)

SELECT 
    s.report_month,
    s.portfolio_category,
    s.partner_name,
    COALESCE(p.portfolio_amount, 0) AS portfolio_amount_uzs,
    COALESCE(i.income_amount, 0) AS income_amount_uzs

FROM partner_spine s
LEFT JOIN portfolio_combined p 
    ON s.report_month = p.report_month 
    AND s.portfolio_category = p.portfolio_category 
    AND s.partner_name = p.partner_name
LEFT JOIN api_income_aggregated i 
    ON s.report_month = i.report_month 
    AND s.portfolio_category = i.portfolio_category 
    AND s.partner_name = i.partner_name
ORDER BY s.report_month DESC, s.portfolio_category, s.partner_name
