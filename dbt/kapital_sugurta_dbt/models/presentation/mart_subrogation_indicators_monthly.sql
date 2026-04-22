{{ config(materialized='table') }}

WITH monthly_subrogation AS (
    SELECT
        DATE_TRUNC('month', recovery_payment_date)::DATE AS report_month,
        SUM(total_recovery_amount) AS total_subrogation_volume_uzs,
        SUM(exact_recovery_amount) AS exact_subrogation_volume_uzs,
        SUM(undefined_recovery_amount) AS undefined_subrogation_volume_uzs
    FROM {{ ref('curated_subrogation_recoveries') }}
    GROUP BY 1
)

SELECT
    report_month,
    'Actual' AS scenario,
    COALESCE(total_subrogation_volume_uzs, 0) AS total_subrogation_volume_uzs,
    COALESCE(exact_subrogation_volume_uzs, 0) AS exact_subrogation_volume_uzs,
    COALESCE(undefined_subrogation_volume_uzs, 0) AS undefined_subrogation_volume_uzs
FROM monthly_subrogation
ORDER BY report_month DESC
