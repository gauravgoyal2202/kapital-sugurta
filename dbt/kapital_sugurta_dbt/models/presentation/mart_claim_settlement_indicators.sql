{{ config(materialized='table') }}

/*
  Dashboard 15 — Claims Settlement Speed (Bottom Chart)
  -----------------------------------------------------
  Aggregates the duration (in days) required to settle a claim.
*/

WITH monthly_settlement AS (
    SELECT
        EXTRACT(YEAR FROM report_month)::INT AS report_year,
        EXTRACT(QUARTER FROM report_month)::INT AS report_quarter,
        report_month,
        insurance_type,
        insurance_type_ru,
        insurance_type_uz_cyrl,
        insurance_type_uz_latn,
        product_category,
        product_category_ru,
        product_category_uz,
        product_category_uz_latn,
        product_name,
        product_name_ru,
        product_name_uz,
        product_name_uz_latn,
        ROUND(AVG(settlement_days)::NUMERIC, 2) AS avg_settlement_days,
        ROUND(MIN(settlement_days)::NUMERIC, 2) AS min_settlement_days,
        ROUND(MAX(settlement_days)::NUMERIC, 2) AS max_settlement_days,
        COUNT(claim_event_id) AS total_claims_settled,
        'Actual' AS scenario
    FROM {{ ref('curated_claims_settlement') }}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
)

SELECT * FROM monthly_settlement
ORDER BY report_month DESC
