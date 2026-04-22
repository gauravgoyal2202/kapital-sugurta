{{ config(materialized='view') }}

WITH source_data AS (
    SELECT 
        d.dep_sum,
        d.depdate_from,
        d.depdate_to,
        "raw".kontragent_name(d.client_id) AS partner_name,
        CASE 
            WHEN d.val_type = 2 THEN 'Foreign Currency Deposits'
            ELSE 'Non-Foreign Currency Deposits'
        END AS deposit_type
    FROM {{ source('raw', 'ins_invdep_oracle') }} d
    LEFT JOIN {{ source('raw', 'p_sp_currency_oracle') }} c
           ON d.val_type = c.sp_id
)

SELECT 
    deposit_type,
    partner_name,
    dep_sum AS deposit_amount,
    depdate_from AS deposit_start_date,
    depdate_to AS deposit_end_date,
    'Actual' AS scenario,
    CURRENT_TIMESTAMP AS updated_at
FROM source_data
