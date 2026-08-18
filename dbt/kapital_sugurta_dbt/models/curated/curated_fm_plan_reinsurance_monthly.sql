{{ config(
    materialized = 'table',
    tags         = ['fm_plan', 'reinsurance', 'monthly']
) }}

/*
  FM Plan — Reinsurance Premiums (Curated)
  ----------------------------------------
  Sheet 'ФО', company consolidated block:
    row 66 — ceded / outgoing reinsurance premium
    row 67 — assumed / incoming reinsurance premium

  FM stores ceded amounts as negative — use ABS for dashboard UZS volumes.
  Grain: month_start_date (Jul 2025 – Dec 2028).
*/

WITH raw_reinsurance AS (
    SELECT
        period_start_date::DATE                                             AS report_month,
        metric_code,
        value_mln_uzs
    FROM {{ source('raw', 'fm_plan_metrics') }}
    WHERE section_name = 'company_consolidated'
      AND metric_code IN ('ceded_reinsurance_premium', 'assumed_reinsurance_premium')
),

pivoted AS (
    SELECT
        report_month,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'ceded_reinsurance_premium')
                                                                            AS ceded_mln,
        MAX(value_mln_uzs) FILTER (WHERE metric_code = 'assumed_reinsurance_premium')
                                                                            AS assumed_mln
    FROM raw_reinsurance
    GROUP BY 1
),

amounts AS (
    SELECT
        p.report_month,
        ROUND(ABS(COALESCE(p.ceded_mln, 0)) * 1000000, 2)                   AS outgoing_reinsurance_volume_uzs,
        ROUND(ABS(COALESCE(p.assumed_mln, 0)) * 1000000, 2)                 AS incoming_reinsurance_volume_uzs,
        i.gross_direct_premium_uzs
    FROM pivoted p
    LEFT JOIN {{ ref('curated_fm_plan_insurance_monthly') }} i
        ON i.month_start_date = p.report_month
)

SELECT
    report_month,
    incoming_reinsurance_volume_uzs,
    outgoing_reinsurance_volume_uzs,
    CASE
        WHEN COALESCE(gross_direct_premium_uzs, 0) > 0
        THEN ROUND(
            (outgoing_reinsurance_volume_uzs / gross_direct_premium_uzs * 100)::NUMERIC,
            4
        )
        ELSE 0
    END                                                                     AS reinsurance_level_pct,
    'Plan'::TEXT                                                            AS scenario
FROM amounts
ORDER BY report_month
