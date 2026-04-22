{{ config(materialized='view') }}

WITH query1 AS (
    SELECT
        bc.pym_date::DATE AS payment_date,
        1 AS source_query_id,
        o.anketa_id,
        
        -- Premium Amount
        (CASE
            WHEN o.opl_val = 1 THEN COALESCE(o.oplata, 0)
            ELSE COALESCE(o.opl_summa, 0) * "raw".f_ins_getkurs(o.opl_val, o.opl_data)
        END)::NUMERIC AS premium_amount,
        
        -- Liability Amount
        (CASE
            WHEN o.opl_val = 1 THEN COALESCE(a.ins_otv, 0)
            ELSE COALESCE(a.ins_otv, 0) * "raw".f_ins_getkurs(o.opl_val, o.opl_data)
        END)::NUMERIC AS liability_amount
        
    FROM {{ source('raw', 'ins_oplata_oracle') }} o
    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a ON a.ins_id = o.anketa_id
    -- Extracted inner joins from user's effectively-inner WHERE filtering
    INNER JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc ON o.bc_id = bc.ins_id
    WHERE o.ins_type <> 3
      AND EXISTS (
        SELECT 1
        FROM {{ source('raw', 'ins_polis_oracle') }} p
        WHERE p.tb_status IN (2, 9, 10)
          AND p.tb_anketa = o.anketa_id
      )
),

query2_part1 AS (
    SELECT
        bc.pym_date::DATE AS payment_date,
        21 AS source_query_id,
        t.tb_id AS anketa_id,
        
        0::NUMERIC AS premium_amount,
        COALESCE(p.tb_summa, 0)::NUMERIC AS liability_amount
        
    FROM {{ source('raw', 'tb_anketa_oracle') }} t
    INNER JOIN {{ source('raw', 'tb_polis_oracle') }} p ON t.tb_id = p.tb_anketa
    INNER JOIN {{ source('raw', 'tb_oplata_oracle') }} o ON t.tb_id = o.tb_anketa
    INNER JOIN {{ source('raw', 'tb_avto_oracle') }} v ON t.tb_id = v.tb_anketa
    INNER JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc ON o.bc_id = bc.ins_id AND bc.status = 2
    WHERE o.tb_typepl = 1
      AND p.tb_status IN (2, 8)
),

query2_part2 AS (
    SELECT
        bc.pym_date::DATE AS payment_date,
        22 AS source_query_id,
        o.anketa_id,
        
        (CASE
            WHEN o.opl_val = 1 THEN COALESCE(o.oplata, 0)
            ELSE COALESCE(o.oplata, 0) * COALESCE(o.val_kurs, 1)
        END)::NUMERIC AS premium_amount,
        
        COALESCE(a.ins_otv, 0)::NUMERIC AS liability_amount
        
    FROM {{ source('raw', 'ins_oplata_oracle') }} o
    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a ON a.ins_id = o.anketa_id
    INNER JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc ON o.bc_id = bc.ins_id AND bc.status = 2
    WHERE o.ins_type = 3
      AND EXISTS (
            SELECT 1
            FROM {{ source('raw', 'ins_polis_oracle') }} p
            WHERE p.tb_status IN (2, 9, 10)
              AND p.tb_anketa = o.anketa_id
        )
)

SELECT * FROM query1
UNION ALL
SELECT * FROM query2_part1
UNION ALL
SELECT * FROM query2_part2
