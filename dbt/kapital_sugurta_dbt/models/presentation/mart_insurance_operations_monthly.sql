{{ config(materialized='table') }}

WITH premium_agg AS (
    SELECT
        DATE_TRUNC('month', payment_date)::DATE AS report_month,
        'Actual' AS scenario,
        SUM(premium_amount) AS insurance_premium_volume_uzs,
        SUM(liability_amount) AS insurance_liabilities_volume_uzs
    FROM {{ ref('curated_insurance_premium') }}
    GROUP BY 1, 2
),

terminated_agg AS (
    SELECT
        DATE_TRUNC('month', termination_date)::DATE AS report_month,
        'Actual' AS scenario,
        SUM(terminated_amount) AS terminated_contracts_volume_uzs
    FROM {{ ref('curated_terminated_contracts') }}
    GROUP BY 1, 2
),

-- Constructing a master timeline unifying all sub-domains
time_spine AS (
    SELECT DISTINCT report_month, scenario FROM premium_agg
    UNION
    SELECT DISTINCT report_month, scenario FROM terminated_agg
    UNION
    SELECT DISTINCT report_month, scenario FROM {{ ref('mart_financial_ratios_monthly') }}
)

SELECT
    s.report_month,
    s.scenario,
    
    -- 1. Premium Volume
    COALESCE(p.insurance_premium_volume_uzs, 0) AS insurance_premium_volume_uzs,
    
    -- 2. Claims Volume (Seamlessly joined from the centralized Ratios mart)
    COALESCE(r.claims_payout, 0) AS insurance_claims_volume_uzs,
    
    -- 3. Loss Ratio % (Seamlessly joined from the centralized Ratios mart to avoid formula duplication)
    COALESCE(r.loss_ratio_pct, 0) AS loss_ratio_pct,
    
    -- 4. Liabilities Volume
    COALESCE(p.insurance_liabilities_volume_uzs, 0) AS insurance_liabilities_volume_uzs,
    
    -- 5. Terminated Contracts
    COALESCE(t.terminated_contracts_volume_uzs, 0) AS terminated_contracts_volume_uzs
    
FROM time_spine s
LEFT JOIN premium_agg p 
    ON s.report_month = p.report_month AND s.scenario = p.scenario
LEFT JOIN terminated_agg t 
    ON s.report_month = t.report_month AND s.scenario = t.scenario
LEFT JOIN {{ ref('mart_financial_ratios_monthly') }} r 
    ON s.report_month = r.report_month AND s.scenario = r.scenario
ORDER BY s.report_month DESC, s.scenario ASC
