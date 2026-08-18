WITH financial_perf_monthly AS (
    SELECT
        DATE_TRUNC('month', report_date) AS report_month,
        scenario,
        SUM(period_expenses) AS operating_expenses_f090,
        SUM(operating_income) AS operating_income_f060,
        SUM(financial_income) AS financial_income_f140
    FROM {{ ref('curated_financial_performance') }}
    WHERE report_date IS NOT NULL
    GROUP BY 1, 2
),

actual_data AS (
    SELECT
        report_month,
        scenario,
        COALESCE(operating_expenses_f090, 0) AS operating_expenses_sum,
        COALESCE(financial_income_f140, 0) AS operating_income_sum,
        CASE
            WHEN COALESCE(operating_income_f060, 0) + COALESCE(financial_income_f140, 0) = 0 THEN 0
            ELSE (
                COALESCE(operating_expenses_f090, 0)
                / (COALESCE(operating_income_f060, 0) + COALESCE(financial_income_f140, 0))
            ) * 100.0
        END AS cir_pct,
        COALESCE(operating_expenses_f090, 0) AS total_expenses_f090,
        COALESCE(operating_income_f060, 0) AS total_revenue_f060,
        COALESCE(financial_income_f140, 0) AS financial_income_f140
    FROM financial_perf_monthly
),

plan_data AS (
    SELECT
        report_month,
        scenario,
        period_expenses_uzs AS operating_expenses_sum,
        financial_income_uzs AS operating_income_sum,
        cir_pct,
        period_expenses_uzs AS total_expenses_f090,
        operating_income_uzs AS total_revenue_f060,
        financial_income_uzs AS financial_income_f140
    FROM {{ ref('curated_fm_plan_financial_monthly') }}
)

SELECT * FROM actual_data
UNION ALL
SELECT * FROM plan_data
ORDER BY report_month DESC, scenario ASC
