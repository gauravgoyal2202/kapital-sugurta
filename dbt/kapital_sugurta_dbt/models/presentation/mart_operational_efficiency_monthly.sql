WITH financial_perf_monthly AS (
    SELECT
        DATE_TRUNC('month', report_date) AS report_month,
        scenario,
        SUM(period_expenses) AS operating_expenses_f090, 
        SUM(operating_income) AS operating_income_f060,  -- F060
        SUM(financial_income) AS financial_income_f140  -- F140
    FROM {{ ref('curated_financial_performance') }}
    WHERE report_date IS NOT NULL
    GROUP BY 1, 2
)

SELECT
    report_month,
    scenario,

    -- Intermediate components
    COALESCE(operating_expenses_f090, 0) AS operating_expenses_sum,
    COALESCE(operating_income_f060, 0) + COALESCE(financial_income_f140, 0) AS operating_income_sum,

    -- Final KPI: CIR %
    -- Formula: Operating Expenses (F090) / Operating Income (F060 + F140)
    CASE 
        WHEN COALESCE(operating_income_f060, 0) + COALESCE(financial_income_f140, 0) = 0 THEN 0 
        ELSE (COALESCE(operating_expenses_f090, 0) / (COALESCE(operating_income_f060, 0) + COALESCE(financial_income_f140, 0))) * 100.0 
    END AS cir_pct,

    -- Drill-down variables matching raw F-codes
    COALESCE(operating_expenses_f090, 0) AS total_expenses_f090,
    COALESCE(operating_income_f060, 0) AS total_revenue_f060,
    COALESCE(financial_income_f140, 0) AS financial_income_f140
    
FROM financial_perf_monthly
ORDER BY report_month DESC
