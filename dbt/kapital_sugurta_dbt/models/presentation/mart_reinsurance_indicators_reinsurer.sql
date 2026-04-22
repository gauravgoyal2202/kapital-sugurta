{{ config(materialized='table') }}

WITH outgoing_base AS (
    SELECT
        COALESCE(NULLIF(TRIM(reinsurer), ''), 'Unknown') AS reinsurer,
        
        SUM(CASE WHEN EXTRACT(YEAR FROM contract_conclusion_date) = 2024 THEN total_accrued_premium_uzs ELSE 0 END) AS outgoing_volume_2024_uzs,
        SUM(CASE WHEN EXTRACT(YEAR FROM contract_conclusion_date) = 2025 THEN total_accrued_premium_uzs ELSE 0 END) AS outgoing_volume_2025_uzs
        
    FROM {{ ref('curated_reinsurance_outgoing_portfolio') }}
    WHERE contract_conclusion_date IS NOT NULL
    GROUP BY 1
),

incoming_base AS (
    SELECT
        COALESCE(NULLIF(TRIM(policyholder), ''), 'Unknown') AS reinsurer,
        
        SUM(CASE WHEN EXTRACT(YEAR FROM contract_conclusion_date) = 2024 THEN total_accrued_premium_uzs ELSE 0 END) AS incoming_volume_2024_uzs,
        SUM(CASE WHEN EXTRACT(YEAR FROM contract_conclusion_date) = 2025 THEN total_accrued_premium_uzs ELSE 0 END) AS incoming_volume_2025_uzs
        
    FROM {{ ref('curated_reinsurance_incoming_portfolio') }}
    WHERE contract_conclusion_date IS NOT NULL
    GROUP BY 1
),

combined_keys AS (
    SELECT reinsurer FROM outgoing_base
    UNION
    SELECT reinsurer FROM incoming_base
)

SELECT
    k.reinsurer,
    
    -- Outgoing Volume (Billion UZS)
    COALESCE(o.outgoing_volume_2024_uzs, 0) / 1000000000.0 AS outgoing_volume_2024_bn_uzs,
    COALESCE(o.outgoing_volume_2025_uzs, 0) / 1000000000.0 AS outgoing_volume_2025_bn_uzs,
    
    -- Outgoing YoY Change (%)
    CASE 
        WHEN COALESCE(o.outgoing_volume_2024_uzs, 0) > 0 
        THEN ((COALESCE(o.outgoing_volume_2025_uzs, 0) - COALESCE(o.outgoing_volume_2024_uzs, 0)) / o.outgoing_volume_2024_uzs) * 100
        ELSE 0 
    END AS outgoing_pct_change_yoy,

    -- Incoming Volume (Billion UZS)
    COALESCE(i.incoming_volume_2024_uzs, 0) / 1000000000.0 AS incoming_volume_2024_bn_uzs,
    COALESCE(i.incoming_volume_2025_uzs, 0) / 1000000000.0 AS incoming_volume_2025_bn_uzs,

    -- Incoming YoY Change (%)
    CASE 
        WHEN COALESCE(i.incoming_volume_2024_uzs, 0) > 0 
        THEN ((COALESCE(i.incoming_volume_2025_uzs, 0) - COALESCE(i.incoming_volume_2024_uzs, 0)) / i.incoming_volume_2024_uzs) * 100
        ELSE 0 
    END AS incoming_pct_change_yoy

FROM combined_keys k
LEFT JOIN outgoing_base o ON k.reinsurer = o.reinsurer
LEFT JOIN incoming_base i ON k.reinsurer = i.reinsurer
ORDER BY k.reinsurer
