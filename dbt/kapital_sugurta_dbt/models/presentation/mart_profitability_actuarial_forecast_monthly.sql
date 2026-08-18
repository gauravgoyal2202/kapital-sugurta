{{ config(
    materialized = 'table',
    tags         = ['profitability', 'actuarial', 'forecast', 'monthly'],
    post_hook    = [
        "CREATE INDEX IF NOT EXISTS idx_mpafm_month  ON {{ this }} (month_start_date)",
        "CREATE INDEX IF NOT EXISTS idx_mpafm_period ON {{ this }} (report_year, report_month)"
    ]
) }}

/*
  Profitability Actuarial — Monthly Forecast (Mart)
  -------------------------------------------------
  Month-wise linear regression forecast for average ZNL (ZNU per case).

  Horizon: 2021 through current year + 2 (one row per month).

  Join to mart_profitability_actuarial_monthly on month_start_date:
    • avg_value_of_znl          → actual
    • forecast_avg_value_of_znl → regression plan / forecast line
*/

SELECT
    month_start_date,
    report_year,
    report_month,
    period_label,
    forecast_avg_value_of_znl,
    regression_input_months,
    regression_x_index,
    validation_status,
    'Forecast'::TEXT                                                        AS scenario

FROM {{ ref('curated_profitability_dashboard_forecast_monthly') }}

ORDER BY month_start_date
