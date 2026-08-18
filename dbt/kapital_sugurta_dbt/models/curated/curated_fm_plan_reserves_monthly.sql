{{ config(
    materialized = 'table',
    tags         = ['fm_plan', 'reserves', 'monthly']
) }}

/*
  FM Plan — Insurance Reserves (Curated)
  --------------------------------------
  Sheet 'ФО', balance-sheet insurance reserve block (rows 311–316, 360).
  Grain: month_start_date; aligned with mart_insurance_reserves_monthly.
  Units: mln UZS → UZS.
*/

WITH raw_reserves AS (
    SELECT
        period_start_date::DATE                                             AS report_month,
        metric_code,
        value_mln_uzs
    FROM {{ source('raw', 'fm_plan_metrics') }}
    WHERE section_name = 'insurance_reserves'
),

pivoted AS (
    SELECT
        report_month,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'unearned_premium_reserve')
                                                                            AS upr_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'ibnr_reserve')
                                                                            AS ibnr_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'rbns_reserve')
                                                                            AS rbns_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'stabilization_reserve_base')
                                                                            AS stabilization_base_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'stabilization_reserve_additional')
                                                                            AS stabilization_additional_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'total_insurance_reserves')
                                                                            AS total_reserves_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'total_assets')
                                                                            AS total_assets_mln
    FROM raw_reserves
    GROUP BY 1
)

SELECT
    report_month,
    ROUND(COALESCE(upr_mln, 0) * 1000000, 2)                                AS upr,
    ROUND(COALESCE(ibnr_mln, 0) * 1000000, 2)                               AS ibnr,
    ROUND(COALESCE(rbns_mln, 0) * 1000000, 2)                                 AS rbns,
    ROUND(COALESCE(stabilization_base_mln, 0) * 1000000, 2)                 AS stabilization_reserve_base,
    ROUND(COALESCE(stabilization_additional_mln, 0) * 1000000, 2)           AS stabilization_reserve_additional,
    ROUND(COALESCE(total_assets_mln, 0) * 1000000, 2)                       AS total_assets,
    ROUND(COALESCE(total_reserves_mln, 0) * 1000000, 2)                     AS total_reserves_denominator,
    'Plan'::TEXT                                                            AS scenario
FROM pivoted
ORDER BY report_month
