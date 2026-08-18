{{ config(
    materialized = 'table',
    tags         = ['profitability', 'dashboard', 'earned'],
    post_hook    = [
        "CREATE INDEX IF NOT EXISTS idx_cpepb_anketa ON {{ this }} (anketa_id)",
        "CREATE INDEX IF NOT EXISTS idx_cpepb_dates  ON {{ this }} (ins_datef, ins_datet)"
    ]
) }}

/*
  Pre-computed payment amounts for earned premium logic (one row per ins_oplata).

  Performance: no row-by-row PL/pgSQL - replaces:
    - F_INS_GETKURS        -> join raw.ins_kurs_oracle
    - INS_FIFTY_GET / pack -> inline SQL mirroring raw.ins_fifty_get()

  Same logic as docs/queries/postgres_earned_premium_claims_reinsurance.sql query 1.
*/

WITH valid_polis_anketa AS (
    SELECT DISTINCT tb_anketa
    FROM {{ source('raw', 'ins_polis_oracle') }}
    WHERE tb_status IN (2, 9, 10)
),

payment_lines AS (

    SELECT
        o.ins_id                                                            AS oplata_id,
        o.anketa_id,
        a.ins_type,
        po.tb_id                                                            AS polis_id,
        a.ins_datef,
        a.ins_datet,
        a.ins_div,
        a.owner,
        a.beneficiary,
        o.opl_val,
        o.opl_data,
        CASE
            WHEN o.opl_val = 1 THEN COALESCE(o.oplata, 0)
            ELSE COALESCE(o.opl_summa, 0) * COALESCE(kurs.kurs_value, 0)
        END                                                                 AS oplsum,
        COALESCE(o.kommis_summa, 0) * COALESCE(kurs.kurs_value, 0)        AS kom_sum,
        CASE
            WHEN o.opl_val = 1 THEN COALESCE(o.oplata, 0)
            ELSE COALESCE(o.opl_summa, 0) * COALESCE(o.val_kurs, 0)
        END                                                                 AS fifty_base_summa
    FROM {{ source('raw', 'ins_oplata_oracle') }} o
    INNER JOIN valid_polis_anketa vp ON vp.tb_anketa = o.anketa_id
    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a ON a.ins_id = o.anketa_id
    LEFT JOIN {{ source('raw', 'ins_polis_oracle') }} po ON po.tb_id = o.polis_id
    LEFT JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc ON o.bc_id = bc.ins_id
    LEFT JOIN {{ source('raw', 'ins_kurs_oracle') }} k
        ON k.kurs_date::DATE = bc.pym_date::DATE
    CROSS JOIN LATERAL (
        SELECT {{ profitability_kurs_value('o.opl_val', 'k') }} AS kurs_value
    ) kurs
    WHERE o.ins_type <> 3
      AND a.ins_datef IS NOT NULL
      AND a.ins_datet IS NOT NULL
      AND a.ins_datet >= DATE '2021-01-01'
      AND a.ins_datef < (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month')::DATE

    UNION ALL

    SELECT
        o.ins_id                                                            AS oplata_id,
        o.anketa_id,
        a.ins_type,
        po.tb_id                                                            AS polis_id,
        a.ins_datef,
        a.ins_datet,
        a.ins_div,
        a.owner,
        a.beneficiary,
        o.opl_val,
        o.opl_data,
        CASE
            WHEN o.opl_val = 1 THEN COALESCE(o.oplata, 0)
            ELSE COALESCE(o.opl_summa, 0) * COALESCE(kurs.kurs_value, 0)
        END                                                                 AS oplsum,
        COALESCE(o.kommis_summa, 0) * COALESCE(kurs.kurs_value, 0)        AS kom_sum,
        CASE
            WHEN o.opl_val = 1 THEN COALESCE(o.oplata, 0)
            ELSE COALESCE(o.opl_summa, 0) * COALESCE(o.val_kurs, 0)
        END                                                                 AS fifty_base_summa
    FROM {{ source('raw', 'ins_oplata_oracle') }} o
    INNER JOIN valid_polis_anketa vp ON vp.tb_anketa = o.anketa_id
    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a ON a.ins_id = o.anketa_id
    LEFT JOIN {{ source('raw', 'ins_polis_oracle') }} po ON po.tb_id = o.polis_id
    LEFT JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc
        ON o.bc_id = bc.ins_id AND bc.status = 2
    LEFT JOIN {{ source('raw', 'ins_kurs_oracle') }} k
        ON k.kurs_date::DATE = bc.pym_date::DATE
    CROSS JOIN LATERAL (
        SELECT {{ profitability_kurs_value('o.opl_val', 'k') }} AS kurs_value
    ) kurs
    WHERE o.ins_type = 3
      AND a.ins_datef IS NOT NULL
      AND a.ins_datet IS NOT NULL
      AND a.ins_datet >= DATE '2021-01-01'
      AND a.ins_datef < (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month')::DATE
),

payment_context AS (
    SELECT
        p.*,
        bk.head_id                                                          AS bank_head_id,
        bk.is_beneficiary,
        COALESCE(ko.tb_rezident, 1)                                         AS owner_rezident,
        g.use_territory,
        g.driver_limit,
        pt.fifty_id                                                         AS pturi_fifty_id,
        pt.dop_stimul                                                       AS pturi_dop_stimul
    FROM payment_lines p
    LEFT JOIN {{ source('raw', 'ins_pturi_oracle') }} pt ON pt.ins_id = p.ins_type
    LEFT JOIN {{ source('raw', 'ins_kontragent_oracle') }} bk
        ON p.beneficiary = bk.tb_id
    LEFT JOIN {{ source('raw', 'ins_kontragent_oracle') }} ko
        ON p.owner = ko.tb_id
    LEFT JOIN {{ source('raw', 'ins_osgo_oracle') }} g
        ON g.anketa_id = p.anketa_id
),

fifty_zp_resolved AS (
    SELECT
        pc.*,
        CASE
            WHEN pc.is_beneficiary = 1 AND pc.bank_head_id IS NOT NULL
            THEN {{ get_dopstimul_sql('pc.bank_head_id', 'pc.ins_type', 'pc.opl_data', 'pc.ins_div', '1') }}
        END                                                                 AS bank_zp_fifty_id
    FROM payment_context pc
),

fifty_zp_rule AS (
    SELECT
        fz.*,
        COALESCE(fz.bank_zp_fifty_id, fz.pturi_fifty_id)                    AS resolved_zp_fifty_id,
        CASE
            WHEN f_base.kod = 30 THEN
                CASE
                    WHEN fz.owner_rezident = 2 THEN 33
                    WHEN FLOOR(fz.ins_div / 1000) IN (39, 90, 10, 34)
                      OR fz.ins_div = 11000 THEN
                        CASE fz.driver_limit
                            WHEN 0 THEN 31.2
                            WHEN 1 THEN 31.1
                            ELSE 0
                        END
                    WHEN COALESCE(fz.use_territory, 0) NOT IN (1) THEN 32
                    ELSE
                        CASE fz.driver_limit
                            WHEN 0 THEN 31.2
                            WHEN 1 THEN 31.1
                            ELSE 0
                        END
                END
        END                                                                 AS osago_kod
    FROM fifty_zp_resolved fz
    LEFT JOIN {{ source('raw', 'ins_fifty_oracle') }} f_base
        ON f_base.id = COALESCE(fz.bank_zp_fifty_id, fz.pturi_fifty_id)
),

fifty_zp_amounts AS (
    SELECT
        fz.*,
        COALESCE(f_osago.id, f_rule.id)                                    AS zp_fifty_id,
        COALESCE(f_osago.percent_or_sum, f_rule.percent_or_sum)             AS zp_percent_or_sum,
        COALESCE(f_osago.percent, f_rule.percent)                           AS zp_percent,
        COALESCE(f_osago.sum, f_rule.sum)                                   AS zp_sum,
        COALESCE(f_osago.for_director, f_rule.for_director)                 AS zp_for_director
    FROM fifty_zp_rule fz
    LEFT JOIN {{ source('raw', 'ins_fifty_oracle') }} f_rule
        ON f_rule.id = fz.resolved_zp_fifty_id
    LEFT JOIN {{ source('raw', 'ins_fifty_oracle') }} f_osago
        ON f_osago.kod = fz.osago_kod
       AND fz.osago_kod IS NOT NULL
       AND fz.osago_kod <> 0
),

fifty_dop_amounts AS (
    SELECT
        za.*,
        CASE
            WHEN pt_chk.ins_id IS NULL
              OR f_pt.id IS NULL
              OR bk_bank.tb_id IS NULL
              OR bk_bank.tb_isbank <> 1
            THEN 0::NUMERIC
            ELSE
                CASE
                    WHEN dop_bank.fifty_id = 0 THEN 0::NUMERIC
                    WHEN dop_bank.fifty_id IS NOT NULL THEN
                        CASE
                            WHEN fd.percent_or_sum = 0
                            THEN ROUND(fd.percent * za.fifty_base_summa / 100, 2)
                            ELSE fd.sum::NUMERIC
                        END
                    WHEN COALESCE(za.pturi_dop_stimul, 0) <> 0 THEN
                        CASE
                            WHEN fd_pt.percent_or_sum = 0
                            THEN ROUND(fd_pt.percent * za.fifty_base_summa / 100, 2)
                            ELSE fd_pt.sum::NUMERIC
                        END
                    ELSE 0::NUMERIC
                END
        END                                                                 AS fifty_dop
    FROM fifty_zp_amounts za
    LEFT JOIN {{ source('raw', 'ins_pturi_oracle') }} pt_chk
        ON pt_chk.ins_id = za.ins_type
    LEFT JOIN {{ source('raw', 'ins_fifty_oracle') }} f_pt
        ON f_pt.id = pt_chk.fifty_id
    LEFT JOIN {{ source('raw', 'ins_kontragent_oracle') }} bk_bank
        ON za.beneficiary = bk_bank.tb_id
       AND bk_bank.tb_isbank = 1
    LEFT JOIN LATERAL (
        SELECT {{ get_dopstimul_sql('bk_bank.head_id', 'za.ins_type', 'za.opl_data', 'za.ins_div', '0') }} AS fifty_id
    ) dop_bank ON bk_bank.head_id IS NOT NULL
    LEFT JOIN {{ source('raw', 'ins_fifty_oracle') }} fd
        ON fd.id = dop_bank.fifty_id
       AND dop_bank.fifty_id IS NOT NULL
       AND dop_bank.fifty_id <> 0
    LEFT JOIN {{ source('raw', 'ins_fifty_oracle') }} fd_pt
        ON fd_pt.id = za.pturi_dop_stimul
       AND dop_bank.fifty_id IS NULL
       AND COALESCE(za.pturi_dop_stimul, 0) <> 0
)

SELECT
    oplata_id,
    anketa_id,
    polis_id,
    ins_datef,
    ins_datet,
    oplsum,
    kom_sum,
    CASE
        WHEN zp_fifty_id IS NULL THEN 0::NUMERIC
        WHEN zp_percent_or_sum = 0
        THEN ROUND(zp_percent * fifty_base_summa / 100, 2)
        ELSE zp_sum::NUMERIC
    END                                                                 AS fifty_zp,
    fifty_dop,
    CASE
        WHEN zp_fifty_id IS NULL THEN 0::NUMERIC
        ELSE ROUND(COALESCE(zp_for_director, 0) * fifty_base_summa / 100, 2)
    END                                                                 AS fifty_director
FROM fifty_dop_amounts
