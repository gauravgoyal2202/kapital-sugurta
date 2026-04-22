{{ config(materialized='table') }}

WITH incoming AS (
    SELECT
        DATE_TRUNC('month', contract_conclusion_date)::DATE AS report_month,
        SUM(total_accrued_premium_uzs) AS incoming_volume_uzs
    FROM {{ ref('curated_reinsurance_incoming_portfolio') }}
    WHERE contract_conclusion_date IS NOT NULL
    GROUP BY 1
),

outgoing AS (
    SELECT
        DATE_TRUNC('month', contract_conclusion_date)::DATE AS report_month,
        SUM(total_accrued_premium_uzs) AS outgoing_volume_uzs
    FROM {{ ref('curated_reinsurance_outgoing_portfolio') }}
    WHERE contract_conclusion_date IS NOT NULL
    GROUP BY 1
),

timeline AS (
    SELECT DISTINCT report_month FROM incoming
    UNION
    SELECT DISTINCT report_month FROM outgoing
)

SELECT 
    t.report_month,
    'Actual' AS scenario,
    
    COALESCE(i.incoming_volume_uzs, 0) AS incoming_reinsurance_volume_uzs,
    COALESCE(o.outgoing_volume_uzs, 0) AS outgoing_reinsurance_volume_uzs,
    
    -- Reinsurance Level = (Outgoing / Total Gross Premium) * 100
    -- Pulling denominator securely from the Insurance Operations mart.
    CASE 
        WHEN COALESCE(ops.insurance_premium_volume_uzs, 0) > 0 
        THEN (COALESCE(o.outgoing_volume_uzs, 0) / ops.insurance_premium_volume_uzs) * 100 
        ELSE 0 
    END AS reinsurance_level_pct

FROM timeline t
LEFT JOIN incoming i 
    ON i.report_month = t.report_month
LEFT JOIN outgoing o 
    ON o.report_month = t.report_month
LEFT JOIN {{ ref('mart_insurance_operations_monthly') }} ops 
    ON ops.report_month = t.report_month AND ops.scenario = 'Actual'
ORDER BY t.report_month DESC
