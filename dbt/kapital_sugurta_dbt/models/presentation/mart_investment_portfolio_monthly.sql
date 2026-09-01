WITH date_bounds AS (
    SELECT
        MIN(deposit_start_date) AS min_date
    FROM {{ ref('curated_oracle_deposits') }}
),

plan_horizon AS (
    -- Include full FM plan months (through Dec 2028) so Plan-vs-Fact charts
    -- are not truncated at the current Actual month.
    SELECT
        COALESCE(MAX(report_month), DATE '2028-12-31') AS max_plan_month
    FROM {{ ref('curated_fm_plan_financial_monthly') }}
),

months AS (
    SELECT
        (DATE_TRUNC('month', dt) + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS report_month
    FROM date_bounds
    CROSS JOIN plan_horizon,
    LATERAL generate_series(
        DATE_TRUNC('month', min_date),
        GREATEST(
            (DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1 year' - INTERVAL '1 day')::DATE,
            max_plan_month
        ),
        INTERVAL '1 month'
    ) AS dt
),

active_deposits AS (
    SELECT 
        m.report_month,
        SUM(
            CASE 
                WHEN c.val_type = 2 THEN c.deposit_amount * k.kurs_usd
                ELSE c.deposit_amount
            END
        ) AS total_deposits
    FROM months m
    LEFT JOIN {{ ref('curated_oracle_deposits') }} c
        ON c.deposit_start_date <= m.report_month
        AND (c.deposit_end_date IS NULL OR c.deposit_end_date > m.report_month)
    LEFT JOIN {{ source('raw', 'ins_kurs_oracle') }} k
        ON k.kurs_date::DATE = m.report_month
    GROUP BY 1
),

active_loans AS (
    SELECT 
        m.report_month,
        SUM(c.loan_amount) AS total_loans
    FROM months m
    LEFT JOIN {{ ref('curated_oracle_loans') }} c
        ON c.loan_start_date <= m.report_month
        AND (c.loan_end_date_actual IS NULL OR c.loan_end_date_actual >= m.report_month)
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
),

actual_output AS (    SELECT
        u.report_month,
        u.portfolio_category,
        SUM(u.total_amount_uzs) OVER (PARTITION BY u.report_month) AS total_investment_portfolio_volume_uzs,
        u.total_amount_uzs AS total_investment_category_amount_uzs,
        CASE
            WHEN SUM(u.total_amount_uzs) OVER (PARTITION BY u.report_month) = 0 THEN 0
            ELSE (u.total_amount_uzs / SUM(u.total_amount_uzs) OVER (PARTITION BY u.report_month)) * 100.0
        END AS total_investment_category_pct,
        SUM(COALESCE(i.total_income, 0)) OVER (PARTITION BY u.report_month) AS total_investment_income_portfolio_volume_uzs,
        COALESCE(i.total_income, 0) AS total_investment_income_category_amount_uzs,
        CASE
            WHEN SUM(COALESCE(i.total_income, 0)) OVER (PARTITION BY u.report_month) = 0 THEN 0
            ELSE (COALESCE(i.total_income, 0) / SUM(COALESCE(i.total_income, 0)) OVER (PARTITION BY u.report_month)) * 100.0
        END AS total_investment_income_category_pct,
        CASE WHEN EXTRACT(MONTH FROM u.report_month) = 12 THEN TRUE ELSE FALSE END AS is_last_month_of_year,
        CASE WHEN EXTRACT(MONTH FROM u.report_month) IN (3, 6, 9, 12) THEN TRUE ELSE FALSE END AS is_last_month_of_quarter,
        CASE WHEN u.report_month = MAX(u.report_month) OVER () THEN TRUE ELSE FALSE END AS is_latest_data_month,
        'Actual'::TEXT AS scenario
    FROM unnested_structure u
    LEFT JOIN income_aggregated i
        ON u.report_month = i.income_month
       AND u.portfolio_category = i.portfolio_category
),

plan_unnested AS (
    -- Plan from FM sheet Инвестиция: portfolio 109:111, income 65:68
    -- report_month = month-end (same grain as Actual deposits spine)
    SELECT
        report_month,
        'Deposits'::TEXT AS portfolio_category,
        deposits_portfolio_uzs AS total_amount_uzs,
        interest_income_uzs AS total_income
    FROM {{ ref('curated_fm_plan_financial_monthly') }}
    UNION ALL
    SELECT report_month, 'Bonds', securities_portfolio_uzs, 0::NUMERIC
    FROM {{ ref('curated_fm_plan_financial_monthly') }}
    UNION ALL
    SELECT report_month, 'Shares', 0::NUMERIC, dividend_income_uzs
    FROM {{ ref('curated_fm_plan_financial_monthly') }}
    UNION ALL
    SELECT report_month, 'Other', 0::NUMERIC, 0::NUMERIC
    FROM {{ ref('curated_fm_plan_financial_monthly') }}
),

plan_output AS (
    SELECT
        report_month,
        portfolio_category,
        SUM(total_amount_uzs) OVER (PARTITION BY report_month) AS total_investment_portfolio_volume_uzs,
        total_amount_uzs AS total_investment_category_amount_uzs,
        CASE
            WHEN SUM(total_amount_uzs) OVER (PARTITION BY report_month) = 0 THEN 0
            ELSE (total_amount_uzs / SUM(total_amount_uzs) OVER (PARTITION BY report_month)) * 100.0
        END AS total_investment_category_pct,
        SUM(total_income) OVER (PARTITION BY report_month) AS total_investment_income_portfolio_volume_uzs,
        total_income AS total_investment_income_category_amount_uzs,
        CASE
            WHEN SUM(total_income) OVER (PARTITION BY report_month) = 0 THEN 0
            ELSE (total_income / SUM(total_income) OVER (PARTITION BY report_month)) * 100.0
        END AS total_investment_income_category_pct,
        CASE WHEN EXTRACT(MONTH FROM report_month) = 12 THEN TRUE ELSE FALSE END AS is_last_month_of_year,
        CASE WHEN EXTRACT(MONTH FROM report_month) IN (3, 6, 9, 12) THEN TRUE ELSE FALSE END AS is_last_month_of_quarter,
        CASE WHEN report_month = MAX(report_month) OVER () THEN TRUE ELSE FALSE END AS is_latest_data_month,
        'Plan'::TEXT AS scenario
    FROM plan_unnested
)

SELECT * FROM actual_output
UNION ALL
SELECT * FROM plan_output
ORDER BY report_month DESC, scenario ASC, portfolio_category
