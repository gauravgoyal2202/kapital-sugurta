{{ config(
    materialized = 'table',
    post_hook    = [
        "CREATE INDEX IF NOT EXISTS idx_cur_ins_prem_paydate  ON {{ this }} (payment_date)",
        "CREATE INDEX IF NOT EXISTS idx_cur_ins_prem_anketa   ON {{ this }} (anketa_id)",
        "CREATE INDEX IF NOT EXISTS idx_cur_ins_prem_pturi    ON {{ this }} (pturi_id)"
    ]
) }}

WITH representative_policies AS (
    -- Identify the "best" policy for each anketa to link to dimensions
    -- Priority: Active status (2, 9, 10), then most recent
    SELECT DISTINCT ON (tb_anketa)
        tb_anketa,
        pturi_id,
        tb_id as polis_id
    FROM {{ source('raw', 'ins_polis_oracle') }}
    ORDER BY tb_anketa, 
             CASE WHEN tb_status IN (2, 9, 10) THEN 0 ELSE 1 END,
             tb_date_begin DESC
),

query1 AS (
    SELECT
        bc.pym_date::DATE AS payment_date,
        1 AS source_query_id,
        o.anketa_id,
        
        -- Premium Amount (Using User Query 1 Logic)
        (CASE
            WHEN o.opl_val = 1 THEN COALESCE(o.oplata, 0)
            ELSE COALESCE(o.opl_summa, 0) * "raw".f_ins_getkurs(o.opl_val, o.opl_data)
        END)::NUMERIC AS premium_amount,
        
        -- Liability Amount
        (CASE
            WHEN o.opl_val = 1 THEN COALESCE(a.ins_otv, 0)
            ELSE COALESCE(a.ins_otv, 0) * "raw".f_ins_getkurs(o.opl_val, o.opl_data)
        END)::NUMERIC AS liability_amount,
        
        -- Dimensions for Marts (Joined via polis_id where possible, else fallback)
        COALESCE(p_direct.pturi_id, p_rep.pturi_id) AS pturi_id,
        pt.vertical AS vertical_id,
        a.fizyur
        
    FROM {{ source('raw', 'ins_oplata_oracle') }} o
    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a ON a.ins_id = o.anketa_id
    LEFT JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc ON o.bc_id = bc.ins_id
    -- Link dimensions to the specific policy or fallback
    LEFT JOIN {{ source('raw', 'ins_polis_oracle') }} p_direct ON p_direct.tb_id = o.polis_id
    LEFT JOIN representative_policies p_rep ON p_rep.tb_anketa = o.anketa_id
    LEFT JOIN {{ source('raw', 'ins_pturi_oracle') }} pt ON pt.ins_id = COALESCE(p_direct.pturi_id, p_rep.pturi_id)
    
    WHERE o.ins_type <> 3
      -- Exact status filter from User Ground Truth
      AND EXISTS (
          SELECT 1 FROM {{ source('raw', 'ins_polis_oracle') }} ps
          WHERE ps.tb_status IN (2, 9, 10) AND ps.tb_anketa = o.anketa_id
      )
),

query2_part1 AS (
    SELECT
        bc.pym_date::DATE AS payment_date,
        21 AS source_query_id,
        t.tb_id AS anketa_id,
        
        0::NUMERIC AS premium_amount,
        COALESCE(p.tb_summa, 0)::NUMERIC AS liability_amount,
        
        NULL::BIGINT AS pturi_id,
        NULL::BIGINT AS vertical_id,
        t.tb_fizur AS fizyur
        
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
        
        -- Premium Amount (Using User Query 2 Part 2 Logic)
        (CASE
            WHEN o.opl_val = 1 THEN COALESCE(o.oplata, 0)
            ELSE COALESCE(o.oplata, 0) * COALESCE(o.val_kurs, 1)
        END)::NUMERIC AS premium_amount,
        
        COALESCE(a.ins_otv, 0)::NUMERIC AS liability_amount,
        
        COALESCE(p_direct.pturi_id, p_rep.pturi_id) AS pturi_id,
        pt.vertical AS vertical_id,
        a.fizyur
        
    FROM {{ source('raw', 'ins_oplata_oracle') }} o
    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a ON a.ins_id = o.anketa_id
    INNER JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc ON o.bc_id = bc.ins_id AND bc.status = 2
    LEFT JOIN {{ source('raw', 'ins_polis_oracle') }} p_direct ON p_direct.tb_id = o.polis_id
    LEFT JOIN representative_policies p_rep ON p_rep.tb_anketa = o.anketa_id
    LEFT JOIN {{ source('raw', 'ins_pturi_oracle') }} pt ON pt.ins_id = COALESCE(p_direct.pturi_id, p_rep.pturi_id)
    
    WHERE o.ins_type = 3
      AND EXISTS (
            SELECT 1 FROM {{ source('raw', 'ins_polis_oracle') }} ps
            WHERE ps.tb_status IN (2, 9, 10) AND ps.tb_anketa = o.anketa_id
        )
)

SELECT * FROM query1
UNION ALL
SELECT * FROM query2_part1
UNION ALL
SELECT * FROM query2_part2
