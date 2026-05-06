{{ config(materialized='view') }}

/*
  Curated Claims Data (Oracle System)
  -----------------------------------
  This model standardizes claims payments from the Oracle 'ins_viplati' table
  and links them to policies and products.
*/

SELECT
    v.date_viplata::DATE                AS payment_date,
    v.ins_id                            AS viplati_id,
    v.sobitie_id,
    s.anketa_id,
    
    -- Claim Amount (handling currency conversion)
    (CASE 
        WHEN COALESCE(v.val_type, 1) = 1 THEN COALESCE(v.viplate, 0) 
        ELSE COALESCE(v.viplate, 0) * COALESCE(v.val_kurs, 1) 
    END)::NUMERIC                       AS claim_amount,
    
    -- Dimensions
    p.pturi_id,
    pt.vertical                         AS vertical_id

FROM {{ source('raw', 'ins_viplati_oracle') }} v
JOIN {{ source('raw', 'ins_sobitie_oracle') }} s ON s.ins_id = v.sobitie_id
JOIN {{ source('raw', 'ins_polis_oracle') }} p   ON p.tb_anketa = s.anketa_id
JOIN {{ source('raw', 'ins_pturi_oracle') }} pt  ON pt.ins_id = p.pturi_id
WHERE v.date_viplata IS NOT NULL
