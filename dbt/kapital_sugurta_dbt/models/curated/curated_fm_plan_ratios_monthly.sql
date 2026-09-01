{{ config(
    materialized = 'table',
    tags         = ['fm_plan', 'ratios', 'monthly']
) }}

/*
  FM Plan — Official Ratio Coefficients (Curated)
  -----------------------------------------------
  Sheet 'Коэффиценты' rows 82–86 (monthly plan columns).
  Excel stores decimals (0.3675); output is percent (36.75).

  Definitions (client-confirmed):
    loss_ratio_pct                  row 82  Коэффициент убыточности
    commission_ratio_pct            row 83  Коэффициент комиссионных
    cost_ratio_pct                  row 84  Коэффициент себестоимости
    mgmt_commercial_expense_ratio_pct row 85  Коэффициент управленческих и коммерческих расходов
    total_expense_ratio_pct         row 86  Коэффициент расходов (= 82+83+84+85)

  Dashboard mapping guidance:
    • Visual "Коэффициент убыточности"  → loss_ratio_pct (row 82 ONLY)
    • Visual "Коэффициент расходов"     → total_expense_ratio_pct (row 86)
      OR rename the visual if using mgmt_commercial_expense_ratio_pct (row 85)
    • Do NOT plot 82+83+84 as loss ratio
*/

WITH raw_ratios AS (
    SELECT
        period_start_date::DATE                                             AS report_month,
        metric_code,
        value_mln_uzs
    FROM {{ source('raw', 'fm_plan_metrics') }}
    WHERE section_name = 'plan_ratios'
),

pivoted AS (
    SELECT
        report_month,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'loss_ratio')
                                                                            AS loss_ratio,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'commission_ratio')
                                                                            AS commission_ratio,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'cost_ratio')
                                                                            AS cost_ratio,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'mgmt_commercial_expense_ratio')
                                                                            AS mgmt_commercial_expense_ratio,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'total_expense_ratio')
                                                                            AS total_expense_ratio
    FROM raw_ratios
    GROUP BY 1
)

SELECT
    report_month,
    EXTRACT(YEAR FROM report_month)::INT                                    AS report_year,
    EXTRACT(MONTH FROM report_month)::INT                                   AS report_month_num,
    ROUND(COALESCE(loss_ratio, 0) * 100, 4)                                 AS loss_ratio_pct,
    ROUND(COALESCE(commission_ratio, 0) * 100, 4)                           AS commission_ratio_pct,
    ROUND(COALESCE(cost_ratio, 0) * 100, 4)                                 AS cost_ratio_pct,
    ROUND(COALESCE(mgmt_commercial_expense_ratio, 0) * 100, 4)            AS mgmt_commercial_expense_ratio_pct,
    ROUND(COALESCE(total_expense_ratio, 0) * 100, 4)                        AS total_expense_ratio_pct,
    'Plan'::TEXT                                                            AS scenario
FROM pivoted
ORDER BY report_month
