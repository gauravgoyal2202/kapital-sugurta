WITH source_data AS (
    SELECT * FROM {{ source('raw', 'claims_portfolio') }}
),
historical_excel_data AS (
    SELECT
        TRIM(record_id) AS record_id,
        TRIM(branch_name) AS branch_name,
        TRIM(insurance_type) AS insurance_type,
        TRIM(insurance_class) AS insurance_class,
        {{ clean_date('incident_date') }} AS incident_date,
        {{ clean_date('settlement_decision_date') }} AS settlement_decision_date,
        {{ clean_date('payout_date') }} AS payout_date,
        {{ clean_numeric('payout_total') }} AS payout_total,
        'Actual' AS scenario,
        CURRENT_TIMESTAMP AS updated_at
    FROM source_data
),

oracle_2026_data AS (
    SELECT
        record_id,
        branch_name,
        insurance_type,
        insurance_class,
        incident_date,
        settlement_decision_date,
        payout_date,
        payout_total,
        scenario,
        updated_at
    FROM {{ ref('curated_claims_2026_oracle') }}
)

SELECT * FROM historical_excel_data
UNION ALL
SELECT * FROM oracle_2026_data
