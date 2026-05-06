{{ config(materialized='view') }}

/*
  Mapping for Dashboard 11 — Priority Areas
  -----------------------------------------
  Groups internal products (pturi) and regulatory classes into:
  1. Auto
  2. Banking
  3. Property
  
  Logic based on standard Uzbekistan insurance class definitions.
*/

WITH pturi as (
    SELECT * FROM {{ ref('curated_product_dimension') }}
)

SELECT
    pturi_id,
    product_name,
    product_code,
    category as internal_vertical,
    CASE 
        -- MOTOR: Transport Risks (3) or Compulsory Liability (23 - includes OSAGO)
        WHEN vertical_id IN (3, 23) THEN 'Motor'
        -- BANKING: Credit Insurance (50) or Financial Risks (5)
        WHEN vertical_id IN (50, 5) THEN 'Banking'
        -- PROPERTY: Property Insurance (1) or Pledged Property/Leasing (8)
        WHEN vertical_id IN (1, 8) THEN 'Property'
        -- DEFAULT: Other
        ELSE 'Other'
    END as priority_area,
    insurance_type -- Compulsory / Voluntary
FROM pturi
