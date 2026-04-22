WITH date_bounds AS (
    -- Dynamically find the earliest date in your raw data
    SELECT 
        MIN(deposit_start_date) AS min_date
    FROM {{ ref('curated_oracle_deposits') }}
),

months AS (
    -- Dynamically generate an end-of-month date spine starting from your earliest data up to current year.
    -- This is required so PowerBI can display dynamic trend graphs across different historical months.
    SELECT 
        (DATE_TRUNC('month', dt) + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS report_month
    FROM date_bounds,
    LATERAL generate_series(
        DATE_TRUNC('month', min_date), 
        (DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1 year' - INTERVAL '1 day')::DATE, 
        INTERVAL '1 month'
    ) AS dt
),

active_deposits AS (
    SELECT 
        m.report_month,
        SUM(c.deposit_amount) AS total_deposits
    FROM months m
    LEFT JOIN {{ ref('curated_oracle_deposits') }} c
        -- Active logic: Deposit was opened on or before the end of this month
        ON c.deposit_start_date <= m.report_month
        -- And was active through this month (closure is null or after this month)
        AND (c.deposit_end_date IS NULL OR c.deposit_end_date > m.report_month)
    GROUP BY 1
),

active_loans AS (
    SELECT 
        m.report_month,
        SUM(c.loan_amount) AS total_loans
    FROM months m
    LEFT JOIN {{ ref('curated_oracle_loans') }} c
        ON c.loan_start_date <= m.report_month
        AND (c.loan_end_date IS NULL OR c.loan_end_date > m.report_month)
    GROUP BY 1
),

combined_monthly AS (
    SELECT 
        m.report_month,
        COALESCE(d.total_deposits, 0) AS total_deposits,
        COALESCE(l.total_loans, 0) AS total_loans
    FROM months m
    LEFT JOIN active_deposits d ON m.report_month = d.report_month
    LEFT JOIN active_loans l ON m.report_month = l.report_month
),

income_aggregated AS (
    SELECT
        (DATE_TRUNC('month', c.report_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS income_month,
        CASE 
            WHEN c.investment_type IN ('DEPOSIT', 'FX_DEPOSIT') THEN 'Deposits'
            WHEN c.investment_type = 'SHARE' THEN 'Shares'
            WHEN c.investment_type = 'BOND' THEN 'Bonds'
            WHEN c.investment_type = 'LOAN' THEN 'Other'
        END AS portfolio_category,
        SUM(c.amount) AS total_income
    FROM {{ ref('curated_investment_activity') }} c
    GROUP BY 1, 2
),

unnested_structure AS (
    -- Deposits
    SELECT report_month, 'Deposits' AS portfolio_category, total_deposits AS total_amount_uzs FROM combined_monthly
    UNION ALL
    -- Shares (Hardcoded 0 for volume as per requirements)
    SELECT report_month, 'Shares' AS portfolio_category, 0::NUMERIC AS total_amount_uzs FROM combined_monthly
    UNION ALL
    -- Bonds (Hardcoded 0 for volume as per requirements)
    SELECT report_month, 'Bonds' AS portfolio_category, 0::NUMERIC AS total_amount_uzs FROM combined_monthly
    UNION ALL
    -- Other (Mapping Loans)
    SELECT report_month, 'Other' AS portfolio_category, total_loans AS total_amount_uzs FROM combined_monthly
)

SELECT 
    u.report_month,
    u.portfolio_category,
    
    -- VOLUME METRICS (From Oracle Raw Data)
    SUM(u.total_amount_uzs) OVER (PARTITION BY u.report_month) AS total_investment_portfolio_volume_uzs,
    u.total_amount_uzs AS total_investment_category_amount_uzs,
    CASE 
        WHEN SUM(u.total_amount_uzs) OVER (PARTITION BY u.report_month) = 0 THEN 0
        ELSE (u.total_amount_uzs / SUM(u.total_amount_uzs) OVER (PARTITION BY u.report_month)) * 100.0
    END AS total_investment_category_pct,

    -- INCOME METRICS (From API Raw Data)
    SUM(COALESCE(i.total_income, 0)) OVER (PARTITION BY u.report_month) AS total_investment_income_portfolio_volume_uzs,
    COALESCE(i.total_income, 0) AS total_investment_income_category_amount_uzs,
    CASE 
        WHEN SUM(COALESCE(i.total_income, 0)) OVER (PARTITION BY u.report_month) = 0 THEN 0
        ELSE (COALESCE(i.total_income, 0) / SUM(COALESCE(i.total_income, 0)) OVER (PARTITION BY u.report_month)) * 100.0
    END AS total_investment_income_category_pct

FROM unnested_structure u
LEFT JOIN income_aggregated i
    ON u.report_month = i.income_month 
    AND u.portfolio_category = i.portfolio_category
ORDER BY u.report_month DESC, u.portfolio_category
