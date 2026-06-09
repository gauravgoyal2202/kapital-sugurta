{{ config(materialized='table') }}

WITH outgoing_base AS (
    SELECT
        DATE_TRUNC('month', premium_accrual_date)::DATE AS report_month,
        COALESCE(NULLIF(TRIM(insurance_type), ''), 'Unknown') AS insurance_type,
        COALESCE(NULLIF(TRIM(voluntary_insurance_type), ''), NULLIF(TRIM(mandatory_insurance_type), ''), 'N/A') AS category,
        COALESCE(NULLIF(TRIM(insurance_type), ''), 'Unknown') AS product,
        COALESCE(NULLIF(TRIM(reinsurer), ''), 'Unknown') AS reinsurer,
        SUM(total_accrued_premium_uzs) AS outgoing_volume_uzs
        
    FROM {{ ref('curated_reinsurance_outgoing_portfolio') }}
    WHERE premium_accrual_date IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
),

incoming_base AS (
    SELECT
        DATE_TRUNC('month', contract_conclusion_date)::DATE AS report_month,
        COALESCE(NULLIF(TRIM(insurance_type), ''), 'Unknown') AS insurance_type,
        COALESCE(NULLIF(TRIM(voluntary_insurance_type), ''), NULLIF(TRIM(mandatory_insurance_type), ''), 'N/A') AS category,
        COALESCE(NULLIF(TRIM(insurance_type), ''), 'Unknown') AS product,
        COALESCE(NULLIF(TRIM(policyholder), ''), 'Unknown') AS reinsurer,
        SUM(total_accrued_premium_uzs) AS incoming_volume_uzs
        
    FROM {{ ref('curated_reinsurance_incoming_portfolio') }}
    WHERE contract_conclusion_date IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
),

combined_keys AS (
    SELECT report_month, insurance_type, category, product, reinsurer FROM outgoing_base
    UNION
    SELECT report_month, insurance_type, category, product, reinsurer FROM incoming_base
),

joined_data AS (
    SELECT
        k.report_month,
        k.insurance_type,
        k.category,
        k.product,
        k.reinsurer,
        COALESCE(o.outgoing_volume_uzs, 0) AS outgoing_volume_uzs,
        COALESCE(i.incoming_volume_uzs, 0) AS incoming_volume_uzs
    FROM combined_keys k
    LEFT JOIN outgoing_base o 
        ON k.report_month = o.report_month 
        AND k.insurance_type = o.insurance_type 
        AND k.category = o.category 
        AND k.product = o.product 
        AND k.reinsurer = o.reinsurer
    LEFT JOIN incoming_base i 
        ON k.report_month = i.report_month 
        AND k.insurance_type = i.insurance_type 
        AND k.category = i.category 
        AND k.product = i.product 
        AND k.reinsurer = i.reinsurer
)

SELECT
    report_month,
    insurance_type,
    category,
    product,
    reinsurer,
    
    -- Outgoing Volume (CY vs PY)
    outgoing_volume_uzs AS outgoing_volume_cy,
    COALESCE(LAG(outgoing_volume_uzs, 12) OVER (PARTITION BY insurance_type, category, product, reinsurer ORDER BY report_month), 0) AS outgoing_volume_py,
    
    -- Outgoing YoY Change (%) compared to same month last year
    CASE 
        WHEN LAG(outgoing_volume_uzs, 12) OVER (PARTITION BY insurance_type, category, product, reinsurer ORDER BY report_month) > 0 
        THEN ((outgoing_volume_uzs - LAG(outgoing_volume_uzs, 12) OVER (PARTITION BY insurance_type, category, product, reinsurer ORDER BY report_month)) / LAG(outgoing_volume_uzs, 12) OVER (PARTITION BY insurance_type, category, product, reinsurer ORDER BY report_month)) * 100
        ELSE 0 
    END AS outgoing_pct_change_yoy,

    -- Incoming Volume (CY vs PY)
    incoming_volume_uzs AS incoming_volume_cy,
    COALESCE(LAG(incoming_volume_uzs, 12) OVER (PARTITION BY insurance_type, category, product, reinsurer ORDER BY report_month), 0) AS incoming_volume_py,

    -- Incoming YoY Change (%) compared to same month last year
    CASE 
        WHEN LAG(incoming_volume_uzs, 12) OVER (PARTITION BY insurance_type, category, product, reinsurer ORDER BY report_month) > 0 
        THEN ((incoming_volume_uzs - LAG(incoming_volume_uzs, 12) OVER (PARTITION BY insurance_type, category, product, reinsurer ORDER BY report_month)) / LAG(incoming_volume_uzs, 12) OVER (PARTITION BY insurance_type, category, product, reinsurer ORDER BY report_month)) * 100
        ELSE 0 
    END AS incoming_pct_change_yoy

FROM joined_data
ORDER BY report_month DESC, insurance_type, category, product, reinsurer
