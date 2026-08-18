{{ config(
    materialized = 'table',
    tags         = ['fm_plan', 'profitability', 'monthly']
) }}

/*
  FM Plan — Insurance Monthly (Curated)
  ------------------------------------
  Pivots raw.fm_plan_metrics (sheet 'ФО', company consolidated block) into
  monthly plan facts aligned with curated_profitability_dashboard_monthly.

  Source: client FM Excel (PwC model), scenario = Plan.
  Units: FM stores mln UZS → multiply by 1,000,000 for dashboard UZS.

  Phase 1 notes:
    • claims_and_commission is a combined FM line (payout + commission)
    • policy_exposure / claim counts are not in the FM consolidated block → 0
*/

WITH plan_metrics AS (
    SELECT
        period_start_date::DATE                                             AS month_start_date,
        metric_code,
        value_mln_uzs
    FROM {{ source('raw', 'fm_plan_metrics') }}
    WHERE section_name = 'company_consolidated'
),

pivoted AS (
    SELECT
        month_start_date,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'earned_premium')
                                                                AS earned_premium_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'gross_direct_premium')
                                                                AS gross_direct_premium_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'net_premium')
                                                                AS net_premium_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'claims_and_commission')
                                                                AS claims_and_commission_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'ceded_reinsurance_premium')
                                                                AS ceded_reinsurance_premium_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'insurance_activity_result')
                                                                AS insurance_activity_result_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'net_profit')
                                                                AS net_profit_mln
    FROM plan_metrics
    GROUP BY 1
),

amounts AS (
    SELECT
        month_start_date,
        EXTRACT(YEAR  FROM month_start_date)::INT                          AS report_year,
        EXTRACT(MONTH FROM month_start_date)::INT                          AS report_month,
        EXTRACT(QUARTER FROM month_start_date)::INT                        AS report_quarter,
        TO_CHAR(month_start_date, 'YYYY-MM')                               AS period_label,

        ROUND(COALESCE(earned_premium_mln, 0) * 1000000, 2)              AS earned_premium_uzs,
        ROUND(COALESCE(gross_direct_premium_mln, 0) * 1000000, 2)        AS gross_direct_premium_uzs,
        ROUND(COALESCE(net_premium_mln, 0) * 1000000, 2)                   AS net_premium_uzs,
        ROUND(ABS(COALESCE(claims_and_commission_mln, 0)) * 1000000, 2)  AS claims_and_commission_uzs,
        ROUND(ABS(COALESCE(ceded_reinsurance_premium_mln, 0)) * 1000000, 2)
                                                                            AS ceded_reinsurance_premium_uzs,
        ROUND(COALESCE(insurance_activity_result_mln, 0) * 1000000, 2)   AS insurance_activity_result_uzs,
        ROUND(COALESCE(net_profit_mln, 0) * 1000000, 2)                  AS net_profit_uzs
    FROM pivoted
)

SELECT
    month_start_date,
    report_year,
    report_month,
    report_quarter,
    period_label,

    earned_premium_uzs,
    0::NUMERIC                                                              AS earned_bonus_uzs,
    0::NUMERIC                                                              AS earned_commission_uzs,

    0::BIGINT                                                               AS total_events,
    0::BIGINT                                                               AS paid_events,
    claims_and_commission_uzs                                               AS total_claimed_loss_uzs,
    claims_and_commission_uzs                                               AS paid_amount_uzs,
    0::NUMERIC                                                              AS unpaid_claimed_loss_uzs,
    claims_and_commission_uzs                                               AS incurred_claims_amount_uzs,

    0::NUMERIC                                                              AS policy_exposure,
    ceded_reinsurance_premium_uzs                                           AS ceded_earned_reinsurance_premium_uzs,

    0::NUMERIC                                                              AS claim_frequency,
    0::NUMERIC                                                              AS avg_value_of_znl,
    0::NUMERIC                                                              AS payment_to_znu_share,
    CASE
        WHEN earned_premium_uzs > 0
        THEN ROUND((claims_and_commission_uzs / earned_premium_uzs * 100)::NUMERIC, 4)
        ELSE 0
    END                                                                     AS actual_loss_ratio,

    'fm_plan_loaded'::TEXT                                                  AS validation_status,

    gross_direct_premium_uzs,
    net_premium_uzs,
    insurance_activity_result_uzs,
    net_profit_uzs,

    'Plan'::TEXT                                                            AS scenario

FROM amounts

ORDER BY month_start_date
