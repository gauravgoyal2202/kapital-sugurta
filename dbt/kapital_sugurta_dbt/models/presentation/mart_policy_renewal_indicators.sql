{{ config(materialized='table') }}

/*
  Dashboard 15 — Policy Renewal Indicators
  ------------------------------------------------
  Aggregates renewal data by CLIENT (rekvezid) to match Oracle logic.
  Preserves original column names as aliases for backward compatibility.
*/

WITH monthly_renewals AS (
    SELECT
        report_month,
        EXTRACT(YEAR FROM report_month)::INT as report_year,
        EXTRACT(QUARTER FROM report_month)::INT as report_quarter,
        insurance_type,
        product_category,
        product_name,
        
        -- Grain: Distinct Clients (rekvezid) as per Oracle
        COUNT(DISTINCT client_id) as expired_clients,
        COUNT(DISTINCT CASE WHEN is_renewed = 1 THEN client_id END) as renewed_clients,
        
        -- Aliases for backward compatibility with original dashboard
        COUNT(DISTINCT client_id) as total_expiring_policies,
        COUNT(DISTINCT CASE WHEN is_renewed = 1 THEN client_id END) as total_renewed_policies
        
    FROM {{ ref('curated_policy_renewals') }}
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
    report_month,
    report_year,
    report_quarter,
    insurance_type,
    product_category,
    product_name,
    
    -- Oracle Metrics
    expired_clients,
    renewed_clients,
    
    -- Original Metrics (Aliases)
    total_expiring_policies,
    total_renewed_policies,
    
    CASE 
        WHEN expired_clients > 0 
        THEN ROUND((renewed_clients::NUMERIC / expired_clients::NUMERIC) * 100, 2)
        ELSE 0 
    END as renewal_rate_pct,
    
    'Actual' as scenario
FROM monthly_renewals
ORDER BY report_month DESC
