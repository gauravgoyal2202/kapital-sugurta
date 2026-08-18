{{ config(
    materialized = 'table',
    tags         = ['reinsurance', 'monthly', 'fm_plan']
) }}

/*
  Reinsurance — Monthly (Mart)
  -----------------------------
  Actual: incoming / outgoing portfolios from operational curated models.
  Plan:   FM Excel sheet 'ФО' rows 66–67 (Jul 2025 – Dec 2028).
*/

WITH incoming AS (
    SELECT
        DATE_TRUNC('month', contract_conclusion_date)::DATE                   AS report_month,
        SUM(total_accrued_premium_uzs)                                        AS incoming_volume_uzs
    FROM {{ ref('curated_reinsurance_incoming_portfolio') }}
    WHERE contract_conclusion_date IS NOT NULL
    GROUP BY 1
),

outgoing AS (
    SELECT
        DATE_TRUNC('month', premium_accrual_date)::DATE                       AS report_month,
        SUM(total_accrued_premium_uzs)                                        AS outgoing_volume_uzs
    FROM {{ ref('curated_reinsurance_outgoing_portfolio') }}
    WHERE premium_accrual_date IS NOT NULL
    GROUP BY 1
),

actual_timeline AS (
    SELECT DISTINCT report_month FROM incoming
    UNION
    SELECT DISTINCT report_month FROM outgoing
),

actual_data AS (
    SELECT
        t.report_month,
        'Actual'::TEXT                                                        AS scenario,
        COALESCE(i.incoming_volume_uzs, 0)                                    AS incoming_reinsurance_volume_uzs,
        COALESCE(o.outgoing_volume_uzs, 0)                                    AS outgoing_reinsurance_volume_uzs,
        CASE
            WHEN COALESCE(ops.insurance_premium_volume_uzs, 0) > 0
            THEN (COALESCE(o.outgoing_volume_uzs, 0) / ops.insurance_premium_volume_uzs) * 100
            ELSE 0
        END                                                                   AS reinsurance_level_pct
    FROM actual_timeline t
    LEFT JOIN incoming i
        ON i.report_month = t.report_month
    LEFT JOIN outgoing o
        ON o.report_month = t.report_month
    LEFT JOIN {{ ref('mart_insurance_operations_monthly') }} ops
        ON ops.report_month = t.report_month
       AND ops.scenario = 'Actual'
),

plan_data AS (
    SELECT
        report_month,
        scenario,
        incoming_reinsurance_volume_uzs,
        outgoing_reinsurance_volume_uzs,
        reinsurance_level_pct
    FROM {{ ref('curated_fm_plan_reinsurance_monthly') }}
)

SELECT * FROM actual_data
UNION ALL
SELECT * FROM plan_data
ORDER BY report_month DESC, scenario ASC
