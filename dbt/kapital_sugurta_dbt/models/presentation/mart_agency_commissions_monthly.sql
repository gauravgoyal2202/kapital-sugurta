{{ config(materialized='table') }}

/*
  Dashboard 9 — Agency Commissions (Monthly Snapshots)
  ----------------------------------------------------
  Updated to support Snapshot aggregation:
  - Base columns show monthly totals.
  - _qtr and _year columns only populate on the last month of the period.
*/

WITH aggregated_commissions AS (
    SELECT
        DATE_TRUNC('month', commission_date)::DATE AS report_month,
        SUM(commission_amount_uzs) AS total_commission_volume_uzs,
        SUM(CASE WHEN entity_type_flag IN (1, '1') THEN commission_amount_uzs ELSE 0 END) AS legal_entity_commission_volume_uzs,
        SUM(CASE WHEN entity_type_flag IN (2, '2') THEN commission_amount_uzs ELSE 0 END) AS individual_commission_volume_uzs
    FROM {{ ref('curated_agency_commissions') }}
    WHERE commission_date IS NOT NULL
    GROUP BY 1
),

period_flags AS (
    SELECT
        *,
        'Actual' AS scenario,
        CASE WHEN report_month = MAX(report_month) OVER (PARTITION BY EXTRACT(YEAR FROM report_month), EXTRACT(QUARTER FROM report_month)) THEN 1 ELSE 0 END AS is_qtr_end,
        CASE WHEN report_month = MAX(report_month) OVER (PARTITION BY EXTRACT(YEAR FROM report_month)) THEN 1 ELSE 0 END AS is_year_end
    FROM aggregated_commissions
)

SELECT
    report_month,
    scenario,
    
    -- Monthly Values
    COALESCE(total_commission_volume_uzs, 0) AS total_commission_volume_uzs,
    COALESCE(legal_entity_commission_volume_uzs, 0) AS legal_entity_commission_volume_uzs,
    COALESCE(individual_commission_volume_uzs, 0) AS individual_commission_volume_uzs,
    
    -- Quarterly Aggregation Helpers (Show last month of quarter)
    CASE WHEN is_qtr_end = 1 THEN total_commission_volume_uzs ELSE 0 END AS total_commission_volume_uzs_qtr,
    CASE WHEN is_qtr_end = 1 THEN legal_entity_commission_volume_uzs ELSE 0 END AS legal_entity_commission_volume_uzs_qtr,
    CASE WHEN is_qtr_end = 1 THEN individual_commission_volume_uzs ELSE 0 END AS individual_commission_volume_uzs_qtr,

    -- Yearly Aggregation Helpers (Show last month of year)
    CASE WHEN is_year_end = 1 THEN total_commission_volume_uzs ELSE 0 END AS total_commission_volume_uzs_year,
    CASE WHEN is_year_end = 1 THEN legal_entity_commission_volume_uzs ELSE 0 END AS legal_entity_commission_volume_uzs_year,
    CASE WHEN is_year_end = 1 THEN individual_commission_volume_uzs ELSE 0 END AS individual_commission_volume_uzs_year,

    -- Percentage calculations
    CASE 
        WHEN total_commission_volume_uzs > 0 
        THEN (legal_entity_commission_volume_uzs / total_commission_volume_uzs) * 100 
        ELSE 0 
    END AS legal_entity_commissions_pct,
    
    CASE 
        WHEN total_commission_volume_uzs > 0 
        THEN (individual_commission_volume_uzs / total_commission_volume_uzs) * 100 
        ELSE 0 
    END AS individual_commission_volume_pct
    
FROM period_flags
ORDER BY report_month DESC
