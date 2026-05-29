{{ config(materialized='view') }}

WITH source_data AS (
    SELECT 
        d.dep_sum,
        d.depdate_from,
        d.depdate_to,
        d.val_type,
        knt.tb_orgname AS partner_name,
        CASE 
            WHEN d.val_type = 2 THEN 'Foreign Currency Deposits'
            ELSE 'Non-Foreign Currency Deposits'
        END AS deposit_type
    FROM {{ source('raw', 'ins_invdep_oracle') }} d
    LEFT JOIN {{ source('raw', 'p_sp_currency_oracle') }} c
           ON d.val_type = c.sp_id
    LEFT JOIN {{ source('raw', 'ins_kontragent_oracle') }} knt
           ON d.client_id = knt.tb_id
)

SELECT 
    deposit_type,
    val_type,
    partner_name,
    dep_sum AS deposit_amount,
    depdate_from AS deposit_start_date,
    depdate_to AS deposit_end_date,
    'Actual' AS scenario,
    CURRENT_TIMESTAMP AS updated_at
FROM source_data
