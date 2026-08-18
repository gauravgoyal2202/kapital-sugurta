{{ config(
    materialized = 'table',
    tags         = ['profitability', 'actuarial', 'monthly'],
    post_hook    = [
        "CREATE INDEX IF NOT EXISTS idx_mpam_month  ON {{ this }} (month_start_date)",
        "CREATE INDEX IF NOT EXISTS idx_mpam_period ON {{ this }} (report_year, report_month)"
    ]
) }}

/*
  Profitability Actuarial — Monthly Metrics (Mart)
  ------------------------------------------------
  Company-wide month grain aligned with client Postgres reference:
  docs/queries/postgres_earned_premium_claims_reinsurance.sql

  Grain: one row per month (company total only).

  Branch / product breakdown: mart_profitability_actuarial_monthly_detail
*/

SELECT
    month_start_date,
    report_year,
    report_month,
    report_quarter,
    period_label,

    earned_premium_uzs,
    earned_bonus_uzs,
    earned_commission_uzs,

    total_events,
    paid_events,
    total_claimed_loss_uzs,
    paid_amount_uzs,
    unpaid_claimed_loss_uzs,
    incurred_claims_amount_uzs,

    policy_exposure,

    ceded_earned_reinsurance_premium_uzs,

    claim_frequency,
    CASE
        WHEN total_claimed_loss_uzs > 0
        THEN ROUND((total_claimed_loss_uzs / NULLIF(total_events, 0))::NUMERIC, 4)
        ELSE 0
    END                                                                             AS avg_value_of_znl,
    CASE
        WHEN total_claimed_loss_uzs > 0
        THEN ROUND((paid_amount_uzs / total_claimed_loss_uzs * 100)::NUMERIC, 4)
        ELSE 0
    END                                                                             AS payment_to_znu_share,
    CASE
        WHEN earned_premium_uzs > 0
        THEN ROUND((paid_amount_uzs / earned_premium_uzs * 100)::NUMERIC, 4)
        ELSE 0
    END                                                                             AS actual_loss_ratio,

    validation_status,

    (
        earned_premium_uzs
        - earned_commission_uzs
        - earned_bonus_uzs
        - incurred_claims_amount_uzs
        - ceded_earned_reinsurance_premium_uzs
    )                                                                               AS profit_amount_uzs,

    CASE
        WHEN earned_premium_uzs > 0
        THEN ROUND(
            (
                (
                    earned_premium_uzs
                    - earned_commission_uzs
                    - earned_bonus_uzs
                    - incurred_claims_amount_uzs
                    - ceded_earned_reinsurance_premium_uzs
                )
                / earned_premium_uzs * 100
            )::NUMERIC,
            2
        )
        ELSE 0
    END                                                                             AS profitability_pct,

    'Actual'::TEXT                                                                  AS scenario

FROM {{ ref('curated_profitability_dashboard_monthly') }}

ORDER BY month_start_date
