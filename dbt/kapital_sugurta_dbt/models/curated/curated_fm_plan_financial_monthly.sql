{{ config(
    materialized = 'table',
    tags         = ['fm_plan', 'financial', 'monthly']
) }}

/*
  FM Plan — Financial Monthly (Curated)
  -------------------------------------
  Pivots Phase 3 raw metrics (ФО extended, Регулятор, Инвестиция) plus
  company insurance claims from curated_fm_plan_insurance_monthly.

  Grain: month_start_date (plan period start); report_month = month-end date.
  Units: mln UZS in raw → UZS in output (× 1,000,000).
*/

WITH raw_metrics AS (
    SELECT
        period_start_date::DATE                                             AS month_start_date,
        section_name,
        metric_code,
        value_mln_uzs
    FROM {{ source('raw', 'fm_plan_metrics') }}
    WHERE section_name IN (
        'company_consolidated',
        'solvency_consolidated',
        'portfolio_consolidated'
    )
),

pivoted AS (
    SELECT
        month_start_date,
        (DATE_TRUNC('month', month_start_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE
                                                                            AS report_month,
        EXTRACT(YEAR  FROM month_start_date)::INT                           AS report_year,
        EXTRACT(MONTH FROM month_start_date)::INT                           AS report_month_num,

        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'insurance_service_revenue')
                                                                            AS insurance_service_revenue_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'selling_expenses')
                                                                            AS selling_expenses_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'admin_expenses')
                                                                            AS admin_expenses_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'other_operating_expenses')
                                                                            AS other_operating_expenses_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'other_operating_income')
                                                                            AS other_operating_income_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'dividend_income')
                                                                            AS dividend_income_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'interest_income')
                                                                            AS interest_income_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'fx_gain_loss')
                                                                            AS fx_gain_loss_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'other_financial_income')
                                                                            AS other_financial_income_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'net_profit')
                                                                            AS net_profit_mln,

        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'own_funds_sources')
                                                                            AS own_funds_sources_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'required_solvency_ratio_threshold')
                                                                            AS required_solvency_ratio_threshold,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'required_solvency_margin')
                                                                            AS required_solvency_margin_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'actual_solvency_margin')
                                                                            AS actual_solvency_margin_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'solvency_adequacy_ratio')
                                                                            AS solvency_adequacy_ratio,

        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'deposits_portfolio')
                                                                            AS deposits_portfolio_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'securities_portfolio')
                                                                            AS securities_portfolio_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'total_investment_portfolio')
                                                                            AS total_investment_portfolio_mln
    FROM raw_metrics
    GROUP BY 1
),

plan_claims AS (
    SELECT
        month_start_date,
        paid_amount_uzs                                                         AS total_claims_uzs
    FROM {{ ref('curated_fm_plan_insurance_monthly') }}
),

amounts AS (
    SELECT
        p.month_start_date,
        p.report_month,
        p.report_year,
        p.report_month_num,

        ROUND(COALESCE(p.insurance_service_revenue_mln, 0) * 1000000, 2)     AS operating_income_uzs,
        ROUND(
            (
                ABS(COALESCE(p.selling_expenses_mln, 0))
                + ABS(COALESCE(p.admin_expenses_mln, 0))
                + ABS(COALESCE(p.other_operating_expenses_mln, 0))
            ) * 1000000,
            2
        )                                                                       AS period_expenses_uzs,
        ROUND(COALESCE(p.other_operating_income_mln, 0) * 1000000, 2)         AS other_operating_income_uzs,
        ROUND(
            (
                COALESCE(p.dividend_income_mln, 0)
                + COALESCE(p.interest_income_mln, 0)
                + COALESCE(p.fx_gain_loss_mln, 0)
                + COALESCE(p.other_financial_income_mln, 0)
            ) * 1000000,
            2
        )                                                                       AS financial_income_uzs,
        ROUND(COALESCE(p.dividend_income_mln, 0) * 1000000, 2)                AS dividend_income_uzs,
        ROUND(COALESCE(p.interest_income_mln, 0) * 1000000, 2)                AS interest_income_uzs,

        ROUND(COALESCE(p.net_profit_mln, 0) * 1000000, 2)                     AS net_profit_uzs,
        ROUND(COALESCE(p.own_funds_sources_mln, 0) * 1000000, 2)             AS own_funds_sources_uzs,
        ROUND(COALESCE(p.total_investment_portfolio_mln, 0) * 1000000, 2)     AS total_assets_proxy_uzs,
        ROUND(COALESCE(p.deposits_portfolio_mln, 0) * 1000000, 2)            AS deposits_portfolio_uzs,
        ROUND(COALESCE(p.securities_portfolio_mln, 0) * 1000000, 2)           AS securities_portfolio_uzs,
        ROUND(COALESCE(p.total_investment_portfolio_mln, 0) * 1000000, 2)   AS total_investment_portfolio_uzs,

        COALESCE(p.solvency_adequacy_ratio, 0)::NUMERIC                         AS solvency_adequacy_ratio_actual,
        COALESCE(p.required_solvency_ratio_threshold, 1)::NUMERIC             AS solvency_adequacy_ratio_required,

        ROUND(COALESCE(c.total_claims_uzs, 0)::NUMERIC, 2)                    AS total_claims_uzs
    FROM pivoted p
    LEFT JOIN plan_claims c
        ON c.month_start_date = p.month_start_date
)

SELECT
    month_start_date,
    report_month,
    report_year,
    report_month_num,

    operating_income_uzs,
    period_expenses_uzs,
    other_operating_income_uzs,
    financial_income_uzs,
    dividend_income_uzs,
    interest_income_uzs,

    CASE
        WHEN operating_income_uzs + financial_income_uzs = 0 THEN 0
        ELSE ROUND(
            (period_expenses_uzs / (operating_income_uzs + financial_income_uzs) * 100)::NUMERIC,
            4
        )
    END                                                                         AS cir_pct,

    net_profit_uzs,
    own_funds_sources_uzs                                                         AS average_equity_uzs,
    total_assets_proxy_uzs                                                        AS average_assets_uzs,
    net_profit_uzs / NULLIF(own_funds_sources_uzs, 0)                           AS roe,
    net_profit_uzs / NULLIF(total_assets_proxy_uzs, 0)                          AS roa,

    deposits_portfolio_uzs,
    securities_portfolio_uzs,
    total_investment_portfolio_uzs,
    financial_income_uzs                                                          AS total_investment_income_uzs,

    total_claims_uzs,
    deposits_portfolio_uzs                                                        AS cash_and_equivalents_a410_uzs,
    CASE
        WHEN total_claims_uzs = 0 THEN 0
        ELSE ROUND((deposits_portfolio_uzs / total_claims_uzs * 100)::NUMERIC, 4)
    END                                                                         AS cash_to_claims_ratio_actual,
    2.5::NUMERIC                                                                AS cash_to_claims_ratio_required,
    solvency_adequacy_ratio_actual,
    solvency_adequacy_ratio_required,

    'Plan'::TEXT                                                                AS scenario

FROM amounts

ORDER BY month_start_date
