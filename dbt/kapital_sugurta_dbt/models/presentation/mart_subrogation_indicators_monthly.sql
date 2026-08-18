{{ config(
    materialized = 'table',
    tags         = ['subrogation', 'monthly', 'fm_plan']
) }}

/*
  Subrogation Indicators — Monthly (Mart)
  ---------------------------------------
  Actual: curated_subrogation_recoveries (Oracle regress payments).
  Plan:   FM commercial initiatives — 'Доходы от регресса' (Jan 2026 – Dec 2028).

  Plan limitation: FM has no exact / undefined recovery split.
*/

WITH actual_subrogation AS (
    SELECT
        DATE_TRUNC('month', recovery_payment_date)::DATE                       AS report_month,
        SUM(total_recovery_amount)                                            AS total_subrogation_volume_uzs,
        SUM(exact_recovery_amount)                                            AS exact_subrogation_volume_uzs,
        SUM(undefined_recovery_amount)                                        AS undefined_subrogation_volume_uzs
    FROM {{ ref('curated_subrogation_recoveries') }}
    GROUP BY 1
),

combined AS (
    SELECT
        report_month,
        'Actual'::TEXT                                                        AS scenario,
        COALESCE(total_subrogation_volume_uzs, 0)                             AS total_subrogation_volume_uzs,
        COALESCE(exact_subrogation_volume_uzs, 0)                               AS exact_subrogation_volume_uzs,
        COALESCE(undefined_subrogation_volume_uzs, 0)                           AS undefined_subrogation_volume_uzs
    FROM actual_subrogation

    UNION ALL

    SELECT
        report_month,
        scenario,
        total_subrogation_volume_uzs,
        exact_subrogation_volume_uzs,
        undefined_subrogation_volume_uzs
    FROM {{ ref('curated_fm_plan_subrogation_monthly') }}
)

SELECT
    report_month,
    scenario,
    total_subrogation_volume_uzs,
    exact_subrogation_volume_uzs,
    undefined_subrogation_volume_uzs
FROM combined
ORDER BY report_month DESC, scenario ASC
