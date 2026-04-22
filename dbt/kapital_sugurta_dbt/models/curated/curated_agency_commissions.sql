{{ config(materialized='view') }}

WITH general_commissions AS (
    -- Handles standard voluntary commissions combining logic from both client constraints
    SELECT 
        o.ins_id AS transaction_id,
        akt.akt_date::DATE AS commission_date,
        
        CASE
            WHEN o.opl_val = 1 THEN COALESCE(o.kommis_summa, 0)
            ELSE COALESCE(o.kommis_summa, 0) * COALESCE(o.val_kurs, 1)
        END::NUMERIC AS commission_amount_uzs,
        
        -- Pulling the native entity structural flag bridging straight to Juridical vs Physical breakdown
        a.fizyur AS entity_type_flag,
        
        -- Joining partner name dynamically using the policy owner and Kontragent dictionary
        COALESCE(NULLIF(TRIM(k.tb_orgname), ''), NULLIF(TRIM(k.tb_name), ''), 'Direct/No Partner') AS bank_partner_name

    FROM {{ source('raw', 'ins_oplata_oracle') }} o
    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a ON a.ins_id = o.anketa_id
    LEFT JOIN {{ source('raw', 'ins_kontragent_oracle') }} k ON k.tb_id = a.owner
    INNER JOIN {{ source('raw', 'ins_agent_akt_oracle') }} akt ON akt.ins_id = o.akt
    
    WHERE akt.active = 2
      -- Validates policy structural constraints dynamically
      AND EXISTS (
          SELECT 1 
          FROM {{ source('raw', 'ins_polis_oracle') }} p 
          WHERE p.tb_status IN (2, 9, 10) AND p.tb_anketa = o.anketa_id
      )
),

osago_commissions AS (
    -- OSAGO (Mandatory Auto) exact mathematical mapping 
    SELECT
        o.tb_id AS transaction_id,
        akt.akt_date::DATE AS commission_date,
        
        -- tb_summa * tb_komissia % dynamically 
        (COALESCE(o.tb_summa, 0) * COALESCE(p.tb_komissia, 0) / 100.0)::NUMERIC AS commission_amount_uzs,
        
        t.tb_fizur AS entity_type_flag,
        
        -- Joining exactly like General Commissions using the explicit policy master tb_user mapping
        COALESCE(NULLIF(TRIM(k.tb_orgname), ''), NULLIF(TRIM(k.tb_name), ''), 'Direct/No Partner') AS bank_partner_name

    FROM {{ source('raw', 'tb_anketa_oracle') }} t
    INNER JOIN {{ source('raw', 'tb_polis_oracle') }} p ON t.tb_id = p.tb_anketa
    INNER JOIN {{ source('raw', 'tb_oplata_oracle') }} o ON t.tb_id = o.tb_anketa
    LEFT JOIN {{ source('raw', 'ins_kontragent_oracle') }} k ON t.tb_user = k.tb_id 
    INNER JOIN {{ source('raw', 'ins_agent_akt_oracle') }} akt ON akt.ins_id = p.akt 
    
    WHERE akt.active = 2 
      AND o.tb_typepl = 1
      AND p.tb_status IN (2, 8)
)

-- Stripped naked of all restrictive date logic so PowerBI time-spines can build historical trends!
SELECT * FROM general_commissions
UNION ALL
SELECT * FROM osago_commissions
