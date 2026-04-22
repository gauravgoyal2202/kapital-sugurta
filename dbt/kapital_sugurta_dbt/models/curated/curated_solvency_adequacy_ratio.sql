WITH source_data AS (
    SELECT * FROM {{ source('raw', 'solvency_adequacy') }}
)

SELECT
    TO_DATE(NULLIF(TRIM(report_date), ''), 'DD-MM-YYYY') AS report_date,
    actual_solvency_margin_adequacy_ratio,
    required_solvency_margin_adequacy_ratio,
    'Actual' AS scenario,
    loaded_at,
    CURRENT_TIMESTAMP AS updated_at
FROM source_data
