{{ config(
    materialized = 'table',
    tags         = ['profitability', 'dashboard', 'monthly'],
    post_hook    = [
        "CREATE INDEX IF NOT EXISTS idx_cpdmd_month   ON {{ this }} (month_start_date)",
        "CREATE INDEX IF NOT EXISTS idx_cpdmd_branch  ON {{ this }} (branch_name)",
        "CREATE INDEX IF NOT EXISTS idx_cpdmd_product ON {{ this }} (product_name)"
    ]
) }}

/*
  Earned profitability dashboard by branch and product (monthly).
  Joins pre-computed earned + claims detail models (each computed once).
  Outgoing earned reinsurance is company-level; allocated to branch/product
  rows proportionally by earned premium share within each month.
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
),

dimension_keys AS (
    SELECT report_month AS month_start, branch_name, product_name, product_id, insurance_type, product_category
    FROM {{ ref('curated_profitability_earned_monthly_detail') }}
    UNION
    SELECT report_month, branch_name, product_name, product_id, insurance_type, product_category
    FROM {{ ref('curated_profitability_claims_monthly_detail') }}
),

detail_base AS (

SELECT
    ms.month_start                                                      AS month_start_date,
    ms.report_year,
    ms.report_month,
    ms.report_quarter,
    ms.period_label,

    dk.branch_name,
    dk.product_name,
    dk.product_id,
    dk.insurance_type,
    dk.product_category,

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

    ROUND(
        (
            CASE WHEN COALESCE(e.policy_exposure, 0) > 0
                 THEN COALESCE(c.total_events, 0)::NUMERIC / e.policy_exposure::NUMERIC
                 ELSE 0::NUMERIC
            END
        )::NUMERIC, 4)                                                  AS claim_frequency,

    'client_logic_applied'::TEXT                                        AS validation_status

FROM month_spine ms
INNER JOIN dimension_keys dk
    ON dk.month_start = ms.month_start
LEFT JOIN {{ ref('curated_profitability_earned_monthly_detail') }} e
    ON  e.report_month = dk.month_start
    AND e.branch_name = dk.branch_name
    AND e.product_name = dk.product_name
    AND e.product_id IS NOT DISTINCT FROM dk.product_id
    AND e.insurance_type = dk.insurance_type
    AND e.product_category = dk.product_category
LEFT JOIN {{ ref('curated_profitability_claims_monthly_detail') }} c
    ON  c.report_month = dk.month_start
    AND c.branch_name = dk.branch_name
    AND c.product_name = dk.product_name
    AND c.product_id IS NOT DISTINCT FROM dk.product_id
    AND c.insurance_type = dk.insurance_type
    AND c.product_category = dk.product_category

)

SELECT
    d.month_start_date,
    d.report_year,
    d.report_month,
    d.report_quarter,
    d.period_label,

    d.branch_name,
    d.product_name,
    d.product_id,
    d.insurance_type,
    d.product_category,

    d.earned_premium_uzs,
    d.earned_bonus_uzs,
    d.earned_commission_uzs,

    d.total_events,
    d.paid_events,
    d.total_claimed_loss_uzs,
    d.paid_amount_uzs,
    d.unpaid_claimed_loss_uzs,
    d.incurred_claims_amount_uzs,

    d.policy_exposure,

    CASE
        WHEN SUM(d.earned_premium_uzs) OVER (PARTITION BY d.month_start_date) = 0
        THEN ROUND(
            COALESCE(r.ceded_earned_reinsurance_premium_uzs, 0)
            / NULLIF(COUNT(*) OVER (PARTITION BY d.month_start_date), 0)::NUMERIC,
            2
        )
        ELSE ROUND(
            COALESCE(r.ceded_earned_reinsurance_premium_uzs, 0)
            * d.earned_premium_uzs
            / NULLIF(SUM(d.earned_premium_uzs) OVER (PARTITION BY d.month_start_date), 0)::NUMERIC,
            2
        )
    END                                                                 AS ceded_earned_reinsurance_premium_uzs,

    d.claim_frequency,

    ROUND(
        (
            CASE WHEN d.total_claimed_loss_uzs > 0
                 THEN d.total_claimed_loss_uzs::NUMERIC
                      / NULLIF(d.total_events, 0)::NUMERIC
                 ELSE 0::NUMERIC
            END
        )::NUMERIC, 4)                                                  AS avg_value_of_znl,

    ROUND(
        (
            CASE WHEN d.total_claimed_loss_uzs > 0
                 THEN d.paid_amount_uzs::NUMERIC
                      / d.total_claimed_loss_uzs::NUMERIC
                      * 100
                 ELSE 0::NUMERIC
            END
        )::NUMERIC, 4)                                                  AS payment_to_znu_share,

    ROUND(
        (
            CASE WHEN d.earned_premium_uzs > 0
                 THEN d.paid_amount_uzs::NUMERIC
                      / d.earned_premium_uzs::NUMERIC
                      * 100
                 ELSE 0::NUMERIC
            END
        )::NUMERIC, 4)                                                  AS actual_loss_ratio,

    d.validation_status

FROM detail_base d
LEFT JOIN {{ ref('curated_profitability_reinsurance_monthly') }} r
    ON r.report_month = d.month_start_date

ORDER BY d.month_start_date, d.branch_name, d.product_name
