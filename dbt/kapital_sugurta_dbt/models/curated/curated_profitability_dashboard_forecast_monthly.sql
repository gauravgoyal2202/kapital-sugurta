{{ config(
    materialized = 'table',
    tags         = ['profitability', 'dashboard', 'forecast', 'monthly']
) }}

/*
  Profitability Dashboard — Monthly ZNU Forecast (Curated)
  --------------------------------------------------------
  Month-wise linear regression forecast for average ZNU / ZNL.

  Logic (extension of curated_profitability_dashboard_forecast):
    • Forecast horizon: 2021 through current calendar year + 2 years
    • Training window: previous 24 months ending before each forecast year
    • x = sequential month index (1..n) on historical monthly avg ZNU
    • Forecast month m of forecast year at x = n + m
      (Excel TREND / FORECAST.LINEAR equivalent on monthly points)
    • y = total_claimed_loss_uzs / total_events (same as avg_value_of_znl)
*/

WITH monthly AS (
    SELECT
        month_start_date,
        report_year,
        report_month,
        total_events,
        total_claimed_loss_uzs,
        CASE
            WHEN total_events > 0
            THEN total_claimed_loss_uzs / total_events
            ELSE NULL
        END                                                                 AS avg_znu_per_case_uzs
    FROM {{ ref('curated_profitability_dashboard_monthly') }}
),

forecast_years AS (
    SELECT generate_series(
        2021,
        EXTRACT(YEAR FROM CURRENT_DATE)::INT + 2
    )::INT                                                                  AS forecast_year
),

lookback AS (
    SELECT
        fy.forecast_year,
        MAKE_DATE(fy.forecast_year, 1, 1)::DATE                              AS forecast_start,
        (MAKE_DATE(fy.forecast_year, 1, 1) - INTERVAL '24 months')::DATE   AS rolling_24m_start
    FROM forecast_years fy
),

monthly_znu_history AS (
    SELECT
        lb.forecast_year,
        m.month_start_date,
        ROW_NUMBER() OVER (
            PARTITION BY lb.forecast_year
            ORDER BY m.month_start_date
        )                                                                   AS x_index,
        m.avg_znu_per_case_uzs
    FROM lookback lb
    INNER JOIN monthly m
        ON m.month_start_date >= lb.rolling_24m_start
       AND m.month_start_date < lb.forecast_start
    WHERE m.avg_znu_per_case_uzs IS NOT NULL
),

history_stats AS (
    SELECT
        forecast_year,
        MAX(x_index)::INT                                                   AS history_months
    FROM monthly_znu_history
    GROUP BY forecast_year
),

znu_regression AS (
    SELECT
        forecast_year,
        COUNT(*)::NUMERIC                                                   AS n,
        SUM(x_index)::NUMERIC                                               AS sum_x,
        SUM(avg_znu_per_case_uzs)::NUMERIC                                  AS sum_y,
        SUM(x_index * avg_znu_per_case_uzs)::NUMERIC                        AS sum_xy,
        SUM(x_index * x_index)::NUMERIC                                     AS sum_x2
    FROM monthly_znu_history
    GROUP BY forecast_year
),

regression_coefficients AS (
    SELECT
        r.forecast_year,
        r.n,
        hs.history_months,
        CASE
            WHEN r.n >= 2
                 AND (r.n * r.sum_x2 - r.sum_x * r.sum_x) <> 0
            THEN (r.n * r.sum_xy - r.sum_x * r.sum_y)
                 / (r.n * r.sum_x2 - r.sum_x * r.sum_x)
            ELSE NULL::NUMERIC
        END                                                                 AS slope,
        CASE
            WHEN r.n >= 2
                 AND (r.n * r.sum_x2 - r.sum_x * r.sum_x) <> 0
            THEN (
                r.sum_y
                - (
                    (r.n * r.sum_xy - r.sum_x * r.sum_y)
                    / (r.n * r.sum_x2 - r.sum_x * r.sum_x)
                ) * r.sum_x
            ) / r.n
            WHEN r.n = 1
            THEN (
                SELECT h.avg_znu_per_case_uzs
                FROM monthly_znu_history h
                WHERE h.forecast_year = r.forecast_year
                LIMIT 1
            )
            ELSE NULL::NUMERIC
        END                                                                 AS intercept
    FROM znu_regression r
    LEFT JOIN history_stats hs
        ON hs.forecast_year = r.forecast_year
),

forecast_months AS (
    SELECT
        fy.forecast_year,
        gs.report_month,
        MAKE_DATE(fy.forecast_year, gs.report_month, 1)::DATE               AS month_start_date,
        TO_CHAR(
            MAKE_DATE(fy.forecast_year, gs.report_month, 1)::DATE,
            'YYYY-MM'
        )                                                                   AS period_label
    FROM forecast_years fy
    CROSS JOIN generate_series(1, 12) AS gs(report_month)
),

znu_monthly_forecast AS (
    SELECT
        fm.forecast_year,
        fm.report_month,
        fm.month_start_date,
        fm.period_label,
        rc.history_months,
        COALESCE(rc.history_months, 0) + fm.report_month                    AS x_forecast,
        CASE
            WHEN rc.n >= 2 AND rc.slope IS NOT NULL AND rc.intercept IS NOT NULL
            THEN rc.intercept + rc.slope * (COALESCE(rc.history_months, 0) + fm.report_month)
            WHEN rc.n = 1
            THEN rc.intercept
            ELSE NULL::NUMERIC
        END                                                                 AS forecast_avg_znu_per_case_uzs
    FROM forecast_months fm
    LEFT JOIN regression_coefficients rc
        ON rc.forecast_year = fm.forecast_year
)

SELECT
    month_start_date,
    forecast_year                                                         AS report_year,
    report_month,
    period_label,
    ROUND(COALESCE(forecast_avg_znu_per_case_uzs, 0)::NUMERIC, 2)       AS forecast_avg_value_of_znl,
    history_months                                                        AS regression_input_months,
    x_forecast                                                            AS regression_x_index,
    'client_logic_applied'::TEXT                                          AS validation_status

FROM znu_monthly_forecast

ORDER BY month_start_date
