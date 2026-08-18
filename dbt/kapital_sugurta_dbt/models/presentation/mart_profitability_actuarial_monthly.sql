{{ config(
    materialized = 'table',
    tags         = ['profitability', 'actuarial', 'monthly', 'fm_plan'],
    post_hook    = [
        "CREATE INDEX IF NOT EXISTS idx_mpam_month  ON {{ this }} (month_start_date)",
        "CREATE INDEX IF NOT EXISTS idx_mpam_period ON {{ this }} (report_year, report_month)",
        "CREATE INDEX IF NOT EXISTS idx_mpam_scenario ON {{ this }} (scenario)"
    ]
) }}

/*
  Profitability Actuarial — Monthly Metrics (Mart)
  ------------------------------------------------
  Actual: curated_profitability_dashboard_monthly
  Plan:   curated_fm_plan_insurance_monthly (FM Excel 2025–2028)
*/

WITH actual_data AS (
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
        actual_loss_ratio,
        validation_status,
        'Actual'::TEXT                                                                  AS scenario

    FROM {{ ref('curated_profitability_dashboard_monthly') }}
),

plan_data AS (
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
        avg_value_of_znl,
        payment_to_znu_share,
        actual_loss_ratio,
        validation_status,
        scenario

    FROM {{ ref('curated_fm_plan_insurance_monthly') }}
),

combined AS (
    SELECT * FROM actual_data
    UNION ALL
    SELECT * FROM plan_data
)

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
    avg_value_of_znl,
    payment_to_znu_share,
    actual_loss_ratio,
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

    scenario

FROM combined

ORDER BY month_start_date, scenario
