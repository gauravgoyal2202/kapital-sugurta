{{ config(
    materialized = 'table',
    tags         = ['profitability', 'actuarial', 'monthly', 'detail', 'fm_plan'],
    post_hook    = [
        "CREATE INDEX IF NOT EXISTS idx_mpamd_month     ON {{ this }} (month_start_date)",
        "CREATE INDEX IF NOT EXISTS idx_mpamd_period    ON {{ this }} (report_year, report_month)",
        "CREATE INDEX IF NOT EXISTS idx_mpamd_branch    ON {{ this }} (branch_name)",
        "CREATE INDEX IF NOT EXISTS idx_mpamd_product   ON {{ this }} (product_name)",
        "CREATE INDEX IF NOT EXISTS idx_mpamd_scenario  ON {{ this }} (scenario)"
    ]
) }}

/*
  Profitability Actuarial — Monthly by Branch & Product (Mart)
  ------------------------------------------------------------
  Actual: Oracle branch × product detail (curated_profitability_dashboard_monthly_detail).
  Plan:   FM Excel product groups (6 groups) — gross direct premium, claims allocated
          by premium share, reinsurance allocated by premium share.
          No branch plan; claim events / avg ZNL not in FM.

  Company-wide KPIs: mart_profitability_actuarial_monthly
*/

WITH actual_base AS (
    SELECT
        month_start_date,
        report_year,
        report_month,
        report_quarter,
        period_label,

        branch_name,
        product_name,
        product_id,
        insurance_type,
        product_category,

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
        validation_status,

        'Actual'::TEXT                                                          AS scenario

    FROM {{ ref('curated_profitability_dashboard_monthly_detail') }}
),

plan_base AS (
    SELECT
        p.report_month                                                          AS month_start_date,
        p.report_year,
        EXTRACT(MONTH FROM p.report_month)::INT                                 AS report_month,
        EXTRACT(QUARTER FROM p.report_month)::INT                               AS report_quarter,
        TO_CHAR(p.report_month, 'YYYY-MM')                                      AS period_label,

        NULL::TEXT                                                              AS branch_name,
        p.product_name,
        NULL::BIGINT                                                            AS product_id,
        p.insurance_type,
        p.category                                                              AS product_category,

        p.co_prem                                                               AS earned_premium_uzs,
        0::NUMERIC                                                              AS earned_bonus_uzs,
        0::NUMERIC                                                              AS earned_commission_uzs,

        0::BIGINT                                                               AS total_events,
        0::BIGINT                                                               AS paid_events,
        p.co_claims                                                             AS total_claimed_loss_uzs,
        p.co_claims                                                             AS paid_amount_uzs,
        0::NUMERIC                                                              AS unpaid_claimed_loss_uzs,
        p.co_claims                                                             AS incurred_claims_amount_uzs,

        0::NUMERIC                                                              AS policy_exposure,
        CASE
            WHEN SUM(p.co_prem) OVER (PARTITION BY p.report_month) = 0
            THEN 0
            ELSE ROUND(
                COALESCE(r.ceded_earned_reinsurance_premium_uzs, 0)
                * p.co_prem
                / NULLIF(SUM(p.co_prem) OVER (PARTITION BY p.report_month), 0),
                2
            )
        END                                                                     AS ceded_earned_reinsurance_premium_uzs,
        0::NUMERIC                                                              AS claim_frequency,
        'fm_plan_loaded'::TEXT                                                  AS validation_status,

        'Plan'::TEXT                                                            AS scenario

    FROM {{ ref('curated_fm_plan_product_monthly') }} p
    LEFT JOIN {{ ref('curated_fm_plan_insurance_monthly') }} r
        ON r.month_start_date = p.report_month
),

combined AS (
    SELECT * FROM actual_base
    UNION ALL
    SELECT * FROM plan_base
)

SELECT
    month_start_date,
    report_year,
    report_month,
    report_quarter,
    period_label,

    branch_name,
    product_name,
    product_id,
    insurance_type,
    CASE insurance_type
        WHEN 'Compulsory' THEN 'Обязательное'
        WHEN 'Mandatory'  THEN 'Обязательное'
        ELSE 'Добровольное'
    END AS insurance_type_ru,
    CASE insurance_type
        WHEN 'Compulsory' THEN 'Мажбурий'
        WHEN 'Mandatory'  THEN 'Мажбурий'
        ELSE 'Ихтиёрий'
    END AS insurance_type_uz_cyrl,
    CASE insurance_type
        WHEN 'Compulsory' THEN 'Majburiy'
        WHEN 'Mandatory'  THEN 'Majburiy'
        ELSE 'Ixtiyoriy'
    END AS insurance_type_uz_latn,
    product_category,

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
        WHEN total_events > 0
        THEN ROUND((total_claimed_loss_uzs / total_events)::NUMERIC, 4)
        ELSE 0
    END                                                                             AS avg_unsettled_claims_uzs,
    CASE
        WHEN paid_events > 0
        THEN ROUND((paid_amount_uzs / paid_events)::NUMERIC, 4)
        ELSE 0
    END                                                                             AS avg_paid_uzs,
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

    scenario

FROM combined

ORDER BY month_start_date, scenario, branch_name, product_name
