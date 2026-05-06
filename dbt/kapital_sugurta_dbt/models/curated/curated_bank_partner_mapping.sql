{{ config(materialized='view') }}

/*
  Bank Partner Mapping (Curated)
  --------------------------------
  Maps each insurance anketa_id to its Bank Partner.
  In Bancassurance, the "Bank" is typically the Beneficiary (Выгодоприобретатель)
  or the Mortgagor (Залогодержатель) of the policy.

  Source logic:
    ins_anketa_oracle.beneficiary -> ins_kontragent_oracle (name)
    ins_anketa_oracle.mortgagor   -> ins_kontragent_oracle (name)
*/

WITH partner_info AS (
    SELECT
        a.ins_id AS anketa_id,
        
        -- Identify the primary partner name (Beneficiary takes precedence, then Mortgagor)
        COALESCE(
            NULLIF(TRIM(kb.tb_orgname), ''), 
            NULLIF(TRIM(kb.tb_name), ''),
            NULLIF(TRIM(km.tb_orgname), ''), 
            NULLIF(TRIM(km.tb_name), ''),
            'Direct/No Partner'
        ) AS bank_partner_name,
        
        -- Map to Priority Area / Product Category
        COALESCE(m.priority_area, 'Other') AS product_category

    FROM {{ source('raw', 'ins_anketa_oracle') }} a
    LEFT JOIN {{ source('raw', 'ins_kontragent_oracle') }} kb ON a.beneficiary = kb.tb_id
    LEFT JOIN {{ source('raw', 'ins_kontragent_oracle') }} km ON a.mortgagor = km.tb_id
    LEFT JOIN {{ ref('curated_priority_area_mapping') }} m ON a.ins_type = m.pturi_id
)

SELECT * FROM partner_info
