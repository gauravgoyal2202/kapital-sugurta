{{ config(
    materialized = 'table',
    tags         = ['profitability', 'dashboard', 'monthly'],
    post_hook    = [
        "CREATE INDEX IF NOT EXISTS idx_cpdm_month_start ON {{ this }} (month_start_date)"
    ]
) }}

/*
  Profitability Dashboard — Monthly Metrics (Curated, company-wide)
  ----------------------------------------------------------------
  Joins pre-computed earned, claims, and reinsurance monthly models.
  Heavy logic lives in upstream curated models (each computed once).
*/

WITH month_spine AS (
    SELECT
        DATE_TRUNC('month', d)::DATE                                      AS month_start,
        EXTRACT(YEAR    FROM d)::INT                                      AS report_year,
        EXTRACT(MONTH   FROM d)::INT                                      AS report_month,
        EXTRACT(QUARTER FROM d)::INT                                      AS report_quarter,
        TO_CHAR(d, 'YYYY-MM')                                             AS period_label
    FROM generate_series(
        '2021-01-01'::DATE,
        DATE_TRUNC('month', CURRENT_DATE)::DATE,
        '1 month'::INTERVAL
    ) AS d
)

SELECT
    ms.month_start                                                      AS month_start_date,
    ms.report_year,
    ms.report_month,
    ms.report_quarter,
    ms.period_label,

    COALESCE(e.earned_premium_uzs, 0)                                   AS earned_premium_uzs,
    COALESCE(e.earned_bonus_uzs, 0)                                     AS earned_bonus_uzs,
    COALESCE(e.earned_commission_uzs, 0)                                AS earned_commission_uzs,

    COALESCE(c.total_events, 0)                                           AS total_events,
    COALESCE(c.paid_events, 0)                                            AS paid_events,
    COALESCE(c.total_claimed_loss_uzs, 0)                                 AS total_claimed_loss_uzs,
    COALESCE(c.paid_amount_uzs, 0)                                        AS paid_amount_uzs,
    COALESCE(c.unpaid_claimed_loss_uzs, 0)                                AS unpaid_claimed_loss_uzs,
    COALESCE(c.incurred_claims_amount_uzs, 0)                             AS incurred_claims_amount_uzs,

    ROUND(COALESCE(e.policy_exposure, 0)::NUMERIC, 2)                   AS policy_exposure,

    COALESCE(r.ceded_earned_reinsurance_premium_uzs, 0)                   AS ceded_earned_reinsurance_premium_uzs,

    ROUND(
        (
            CASE WHEN COALESCE(e.policy_exposure, 0) > 0
                 THEN COALESCE(c.total_events, 0)::NUMERIC / e.policy_exposure::NUMERIC
                 ELSE 0::NUMERIC
            END
        )::NUMERIC, 4)                                                  AS claim_frequency,
    ROUND(
        (
            CASE WHEN COALESCE(c.total_claimed_loss_uzs, 0) > 0
                 THEN COALESCE(c.total_claimed_loss_uzs, 0)::NUMERIC
                      / NULLIF(c.total_events, 0)::NUMERIC
                 ELSE 0::NUMERIC
            END
        )::NUMERIC, 4)                                                  AS avg_value_of_znl,
    ROUND(
        (
            CASE WHEN COALESCE(c.total_claimed_loss_uzs, 0) > 0
                 THEN COALESCE(c.paid_amount_uzs, 0)::NUMERIC
                      / c.total_claimed_loss_uzs::NUMERIC
                 ELSE 0::NUMERIC
            END
        )::NUMERIC, 4)                                                  AS payment_to_znu_share,
    ROUND(
        (
            CASE WHEN COALESCE(e.earned_premium_uzs, 0) > 0
                 THEN COALESCE(c.paid_amount_uzs, 0)::NUMERIC
                      / e.earned_premium_uzs::NUMERIC
                 ELSE 0::NUMERIC
            END
        )::NUMERIC, 4)                                                  AS actual_loss_ratio,

    'client_logic_applied'::TEXT                                        AS validation_status

FROM month_spine ms
LEFT JOIN {{ ref('curated_profitability_earned_monthly_company') }} e
    ON e.report_month = ms.month_start
LEFT JOIN {{ ref('curated_profitability_claims_monthly') }} c
    ON c.report_month = ms.month_start
LEFT JOIN {{ ref('curated_profitability_reinsurance_monthly') }} r
    ON r.report_month = ms.month_start

ORDER BY ms.month_start
