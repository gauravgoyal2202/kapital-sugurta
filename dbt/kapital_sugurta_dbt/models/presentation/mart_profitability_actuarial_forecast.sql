{{ config(
    materialized = 'table',
    tags         = ['profitability', 'actuarial', 'forecast'],
    post_hook    = [
        "CREATE INDEX IF NOT EXISTS idx_mpaf_year ON {{ this }} (forecast_year)"
    ]
) }}

/*
  Profitability Actuarial — Forecast (Mart)
  -----------------------------------------
  Year-wise portfolio forecast for Power BI (no section split).

  One row per year from 2021 through current calendar year (auto-extending).
  Inputs computed dynamically in curated_profitability_dashboard_forecast.
*/

SELECT
    forecast_year,
    avg_insurance_premium_uzs,
    forecast_avg_znu_per_case_uzs,
    claim_frequency,
    payment_to_znu_share,
    forecast_avg_payout_uzs,
    forecast_loss_per_policy,
    validation_status,
    'Forecast'::TEXT                                                        AS scenario

FROM {{ ref('curated_profitability_dashboard_forecast') }}

ORDER BY forecast_year
