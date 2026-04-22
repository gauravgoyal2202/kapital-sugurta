{{ config(materialized='table') }}

WITH aggregated_commissions AS (
    SELECT
        DATE_TRUNC('month', commission_date)::DATE AS report_month,
        
        -- Total Global Subrogation Sum
        SUM(commission_amount_uzs) AS total_commission_volume_uzs,
        
        -- Isolating strictly Juridical / Legal Entity Commissions (fizyur/tb_fizur = 1 is standard for Entities)
        SUM(CASE WHEN entity_type_flag IN (1, '1') THEN commission_amount_uzs ELSE 0 END) AS legal_entity_commission_volume_uzs,
        
        -- Isolating strictly Individual Persons (fizyur/tb_fizur = 2 is standard for Physical)
        SUM(CASE WHEN entity_type_flag IN (2, '2') THEN commission_amount_uzs ELSE 0 END) AS individual_commission_volume_uzs
        
    FROM {{ ref('curated_agency_commissions') }}
    WHERE commission_date IS NOT NULL
    GROUP BY 1
)

SELECT
    report_month,
    'Actual' AS scenario,
    COALESCE(total_commission_volume_uzs, 0) AS total_commission_volume_uzs,
    COALESCE(legal_entity_commission_volume_uzs, 0) AS legal_entity_commission_volume_uzs,
    COALESCE(individual_commission_volume_uzs, 0) AS individual_commission_volume_uzs
    
FROM aggregated_commissions
ORDER BY report_month DESC
