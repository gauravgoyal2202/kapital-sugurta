{{ config(
    materialized = 'table',
    post_hook    = [
      "CREATE INDEX IF NOT EXISTS idx_stg_fifty_ins_id ON {{ this }} (ins_id)"
    ]
) }}

WITH payments AS (
    SELECT
        o.ins_id,
        o.anketa_id,
        o.ins_type,
        o.opl_data,
        CASE
            WHEN o.opl_val = 1 THEN o.oplata
            ELSE o.opl_summa * o.val_kurs
        END AS v_summa
    FROM {{ source('raw', 'ins_oplata_oracle') }} o
    JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc
        ON bc.ins_id = o.bc_id
    WHERE bc.pym_date >= DATE '2021-01-01'
),

payment_anketa AS (
    SELECT
        p.*,
        a.ins_div,
        a.owner AS a_owner,
        a.beneficiary AS a_beneficiary,
        k_ben.is_beneficiary,
        k_ben.head_id AS ben_head_id,
        k_ben.tb_isbank
    FROM payments p
    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a
        ON a.ins_id = p.anketa_id
    LEFT JOIN {{ source('raw', 'ins_kontragent_oracle') }} k_ben
        ON k_ben.tb_id = a.beneficiary
),

dopstimul_step4 AS (
    SELECT
        pa.ins_id,
        COALESCE(pt.dop_stimul, 0) AS default_f_dop_stimul,
        COALESCE(dop_month.fifty_id, dop_fallback.fifty_id) AS f_temp_dop_sti
    FROM payment_anketa pa
    LEFT JOIN {{ source('raw', 'ins_pturi_oracle') }} pt
        ON pt.ins_id = pa.ins_type
    LEFT JOIN LATERAL (
        SELECT bp.fifty_id
        FROM {{ source('raw', 'ins_bank_pturi_oracle') }} bp
        WHERE bp.bank_id = pa.ben_head_id
          AND (bp.pturi_id = pa.ins_type OR bp.pturi_id = 0)
          AND bp.type = 0
          AND bp.start_date BETWEEN pa.opl_data::date
                               AND (DATE_TRUNC('month', pa.opl_data::date) + INTERVAL '1 month' - INTERVAL '1 day')::date
          AND bp.fifty_id IS NOT NULL
          AND bp.division_id = pa.ins_div
        ORDER BY bp.start_date DESC, bp.fifty_id DESC
        LIMIT 1
    ) dop_month ON pa.tb_isbank = 1
    LEFT JOIN LATERAL (
        SELECT bp.fifty_id
        FROM {{ source('raw', 'ins_bank_pturi_oracle') }} bp
        WHERE bp.bank_id = pa.ben_head_id
          AND (bp.pturi_id = pa.ins_type OR bp.pturi_id = 0)
          AND bp.type = 0
          AND pa.opl_data::date >= bp.start_date
          AND bp.fifty_id IS NOT NULL
        ORDER BY bp.start_date DESC, bp.fifty_id DESC
        LIMIT 1
    ) dop_fallback ON pa.tb_isbank = 1 AND dop_month.fifty_id IS NULL
),

fid_step3_5 AS (
    SELECT
        pa.ins_id,
        pt.fifty_id AS v_fid_default,
        COALESCE(fid_month.fifty_id, fid_fallback.fifty_id) AS v_fid_temp
    FROM payment_anketa pa
    LEFT JOIN {{ source('raw', 'ins_pturi_oracle') }} pt
        ON pt.ins_id = pa.ins_type
    LEFT JOIN LATERAL (
        SELECT bp.fifty_id
        FROM {{ source('raw', 'ins_bank_pturi_oracle') }} bp
        WHERE bp.bank_id = pa.ben_head_id
          AND (bp.pturi_id = pa.ins_type OR bp.pturi_id = 0)
          AND bp.type = 1
          AND bp.start_date BETWEEN pa.opl_data::date
                               AND (DATE_TRUNC('month', pa.opl_data::date) + INTERVAL '1 month' - INTERVAL '1 day')::date
          AND bp.fifty_id IS NOT NULL
          AND bp.division_id = pa.ins_div
        ORDER BY bp.start_date DESC, bp.fifty_id DESC
        LIMIT 1
    ) fid_month ON pa.is_beneficiary = 1
    LEFT JOIN LATERAL (
        SELECT bp.fifty_id
        FROM {{ source('raw', 'ins_bank_pturi_oracle') }} bp
        WHERE bp.bank_id = pa.ben_head_id
          AND (bp.pturi_id = pa.ins_type OR bp.pturi_id = 0)
          AND bp.type = 1
          AND pa.opl_data::date >= bp.start_date
          AND bp.fifty_id IS NOT NULL
        ORDER BY bp.start_date DESC, bp.fifty_id DESC
        LIMIT 1
    ) fid_fallback ON pa.is_beneficiary = 1 AND fid_month.fifty_id IS NULL
),

fifty_resolved_ids AS (
    SELECT
        pa.ins_id,
        pa.v_summa,
        pa.ins_div,
        pa.anketa_id,
        pa.a_owner,
        
        -- Dop stimul logic
        CASE
            WHEN d4.f_temp_dop_sti = 0 THEN 0
            WHEN d4.f_temp_dop_sti IS NOT NULL THEN d4.f_temp_dop_sti
            ELSE d4.default_f_dop_stimul
        END AS f_dop_stimul_id,

        -- Standard fifty_id logic
        CASE
            WHEN f35.v_fid_temp IS NOT NULL THEN f35.v_fid_temp
            ELSE f35.v_fid_default
        END AS v_fid
    FROM payment_anketa pa
    JOIN dopstimul_step4 d4 ON d4.ins_id = pa.ins_id
    JOIN fid_step3_5 f35 ON f35.ins_id = pa.ins_id
),

fifty_dop_value AS (
    SELECT
        fri.ins_id,
        CASE
            WHEN fri.f_dop_stimul_id = 0 THEN 0
            ELSE
                CASE
                    WHEN f_dop.percent_or_sum = 0 THEN ROUND(f_dop.percent * fri.v_summa / 100, 2)
                    ELSE f_dop.sum
                END
        END AS fifty_dop
    FROM fifty_resolved_ids fri
    LEFT JOIN {{ source('raw', 'ins_fifty_oracle') }} f_dop
        ON f_dop.id = fri.f_dop_stimul_id
),

osgo_properties AS (
    SELECT
        fri.ins_id,
        fri.ins_div,
        f.kod,
        f.percent_or_sum,
        f.percent,
        f.sum,
        f.for_director,
        
        -- OSGO OLD/NEW specific properties
        osgo.use_territory,
        osgo.driver_limit,
        COALESCE(owner_k.tb_rezident, 1) AS tb_rezident
    FROM fifty_resolved_ids fri
    LEFT JOIN {{ source('raw', 'ins_fifty_oracle') }} f
        ON f.id = fri.v_fid
    LEFT JOIN {{ source('raw', 'ins_osgo_oracle') }} osgo
        ON osgo.anketa_id = fri.anketa_id
    LEFT JOIN {{ source('raw', 'ins_kontragent_oracle') }} owner_k
        ON owner_k.tb_id = fri.a_owner
),

osgo_resolved AS (
    SELECT
        op.ins_id,
        CASE
            WHEN op.kod = 30 THEN
                CASE
                    WHEN op.tb_rezident = 2 THEN 33
                    WHEN FLOOR(op.driver_limit) IS NULL AND FLOOR(op.use_territory) IS NULL THEN 0 -- Safe fallback if no OSGO record
                    WHEN FLOOR(op.driver_limit) IS NOT NULL AND (FLOOR(op.ins_div) IS NOT NULL) AND (
                        FLOOR(op.ins_div / 1000) IN (39, 90, 10, 34) OR op.ins_div = 11000
                    ) THEN
                        CASE
                            WHEN op.driver_limit = 0 THEN 31.2
                            WHEN op.driver_limit = 1 THEN 31.1
                            ELSE 0
                        END
                    ELSE
                        CASE
                            WHEN op.use_territory NOT IN (1) THEN 32
                            ELSE
                                CASE
                                    WHEN op.driver_limit = 0 THEN 31.2
                                    WHEN op.driver_limit = 1 THEN 31.1
                                    ELSE 0
                                END
                        END
                END
            ELSE op.kod
        END AS resolved_kod,
        op.percent_or_sum,
        op.percent,
        op.sum,
        op.for_director,
        op.kod AS original_kod
    FROM osgo_properties op
),

osgo_fifty_rules AS (
    SELECT
        orv.ins_id,
        CASE
            WHEN orv.original_kod = 30 THEN fos.percent_or_sum
            ELSE orv.percent_or_sum
        END AS final_percent_or_sum,
        CASE
            WHEN orv.original_kod = 30 THEN fos.percent
            ELSE orv.percent
        END AS final_percent,
        CASE
            WHEN orv.original_kod = 30 THEN fos.sum
            ELSE orv.sum
        END AS final_sum,
        CASE
            WHEN orv.original_kod = 30 THEN fos.for_director
            ELSE orv.for_director
        END AS final_for_director
    FROM osgo_resolved orv
    LEFT JOIN {{ source('raw', 'ins_fifty_oracle') }} fos
        ON fos.kod = orv.resolved_kod AND orv.original_kod = 30
),

final_motivation AS (
    SELECT
        fri.ins_id,
        
        -- ZP / Step 3
        CASE
            WHEN ofr.final_percent_or_sum = 0 THEN ROUND(ofr.final_percent * fri.v_summa / 100, 2)
            ELSE ofr.final_sum
        END AS fifty_zp,

        -- DOP
        fdv.fifty_dop,

        -- DIRECTOR / Step 5
        ROUND(ofr.final_for_director * fri.v_summa / 100, 2) AS fifty_director

    FROM fifty_resolved_ids fri
    JOIN osgo_fifty_rules ofr ON ofr.ins_id = fri.ins_id
    JOIN fifty_dop_value fdv ON fdv.ins_id = fri.ins_id
)

SELECT
    ins_id,
    COALESCE(fifty_zp, 0) AS fifty_zp,
    COALESCE(fifty_dop, 0) AS fifty_dop,
    COALESCE(fifty_director, 0) AS fifty_director,
    (COALESCE(fifty_zp, 0) + COALESCE(fifty_dop, 0) + COALESCE(fifty_director, 0)) AS fifty_total
FROM final_motivation
