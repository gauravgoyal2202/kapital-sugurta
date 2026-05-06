{{ config(materialized='table') }}

/*
  Dashboard 7 Dimension: Insurance Portfolio Filters
  --------------------------------------------------
  This table provides the unique values for the Power BI filters:
  - Type of Insurance (Compulsory / Voluntary)
  - Category (Product Vertical)
  - Product Name
*/

WITH base AS (
    SELECT
        insurance_type,
        category,
        product_name,
        is_active,
        MIN(pturi_id) as pturi_id  -- Keep a representative ID if needed
    FROM {{ ref('curated_product_dimension') }}
    GROUP BY 1, 2, 3, 4
)

SELECT
    -- Generate a surrogate key for Power BI if needed
    md5(COALESCE(insurance_type, '') || COALESCE(category, '') || COALESCE(product_name, '')) as portfolio_id,
    insurance_type,
    category,
    product_name,
    is_active
FROM base
ORDER BY insurance_type, category, product_name
