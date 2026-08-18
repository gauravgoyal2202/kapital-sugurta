{{ config(
    materialized = 'table',
    tags         = ['partner', 'payment_base'],
    post_hook    = [
        "CREATE INDEX IF NOT EXISTS idx_cppb_oplata   ON {{ this }} (oplata_id)",
        "CREATE INDEX IF NOT EXISTS idx_cppb_anketa   ON {{ this }} (anketa_id)",
        "CREATE INDEX IF NOT EXISTS idx_cppb_pym_date ON {{ this }} (pym_date)",
        "CREATE INDEX IF NOT EXISTS idx_cppb_pturi    ON {{ this }} (pturi_id)"
    ]
) }}

/*
  Payment-level base for partner / channel dashboards.

  Performance: replaces per-row PL/pgSQL calls used in partner and commercial models:
    - F_INS_GETKURS / ins_fifty_pack / INS_FIFTY_GET -> join ins_kurs_oracle + inline fifty SQL
*/

WITH valid_polis_anketa AS (
    SELECT DISTINCT tb_anketa
    FROM {{ source('raw', 'ins_polis_oracle') }}
    WHERE tb_status IN (2, 9, 10)
),

akt_ch AS (
    SELECT
        ins_id,
        MAX(akt_type) AS akt_type
    FROM {{ source('raw', 'ins_agent_akt_oracle') }}
    GROUP BY ins_id
),

payment_lines AS (
    SELECT
        o.ins_id                                                            AS oplata_id,
        o.anketa_id,
        po.tb_id                                                            AS polis_id,
        o.user_id,
        bc.pym_date,
        bc.ins_id                                                           AS bc_id,
        o.ins_type                                                          AS pturi_id,
        o.opl_type,
        o.opl_val,
        o.opl_data,
        o.oplata,
        o.opl_summa,
        o.val_kurs,
        o.kommis_summa,
        a.ins_type,
        a.ins_div,
        a.owner                                                             AS owner_id,
        a.beneficiary,
        a.ins_otv,
        a.ins_prem,
        COALESCE(akt.akt_type, akt_ch.akt_type)                             AS akt_type,
        bank_k.tb_id                                                        AS bank_k_id,
        bank_k.tb_orgname                                                   AS bank_orgname,
        TRIM(u.tb_surname)                                                  AS user_surname,
        TRIM(u.tb_name)                                                     AS user_name,
        pt.mandatory,
        COALESCE(pt.polis_name, 'Other')                                    AS product_name,
        COALESCE(v_cat.name3, 'Other')                                      AS product_category,
        CASE
            WHEN o.opl_val = 1 THEN COALESCE(o.oplata, 0)
            ELSE COALESCE(o.opl_summa, 0) * COALESCE(kurs.kurs_value, 0)
        END                                                                 AS oplsum,
        COALESCE(o.kommis_summa, 0) * COALESCE(kurs.kurs_value, 0)        AS kom_sum,
        CASE
            WHEN o.opl_val = 1 THEN COALESCE(o.oplata, 0)
            ELSE COALESCE(o.opl_summa, 0) * COALESCE(o.val_kurs, 0)
        END                                                                 AS fifty_base_summa,
        FALSE                                                               AS is_osago_old
    FROM {{ source('raw', 'ins_oplata_oracle') }} o
    INNER JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc
        ON bc.ins_id = o.bc_id
       AND bc.pym_date >= DATE '2021-01-01'
    INNER JOIN valid_polis_anketa vp
        ON vp.tb_anketa = o.anketa_id
    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a
        ON a.ins_id = o.anketa_id
    LEFT JOIN {{ source('raw', 'ins_polis_oracle') }} po
        ON po.tb_id = o.polis_id
    LEFT JOIN {{ source('raw', 'tb_users_oracle') }} u
        ON u.tb_id = o.user_id
    LEFT JOIN {{ source('raw', 'ins_agent_akt_oracle') }} akt
        ON akt.ins_id = o.akt
       AND akt.active = 2
    LEFT JOIN akt_ch
        ON akt_ch.ins_id = o.akt
    LEFT JOIN {{ source('raw', 'ins_kontragent_oracle') }} bank_k
        ON bank_k.tb_isbank = 1
       AND bank_k.tb_id IN (a.owner, a.beneficiary, a.mortgagor)
    LEFT JOIN {{ source('raw', 'ins_pturi_oracle') }} pt
        ON pt.ins_id = a.ins_type
    LEFT JOIN {{ source('raw', 'ins_vertical_oracle') }} v_cat
        ON v_cat.ins_id = pt.vertical
    LEFT JOIN {{ source('raw', 'ins_kurs_oracle') }} k
        ON k.kurs_date::DATE = bc.pym_date::DATE
    CROSS JOIN LATERAL (
        SELECT {{ profitability_kurs_value('o.opl_val', 'k') }} AS kurs_value
    ) kurs
    WHERE (
            o.ins_type <> 3
            OR (o.ins_type = 3 AND bc.status = 2)
          )
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
    LEFT JOIN {{ source('raw', 'ins_pturi_oracle') }} pt
        ON pt.ins_id = p.ins_type
    LEFT JOIN {{ source('raw', 'ins_kontragent_oracle') }} bk
        ON p.beneficiary = bk.tb_id
    LEFT JOIN {{ source('raw', 'ins_kontragent_oracle') }} ko
        ON p.owner_id = ko.tb_id
    LEFT JOIN {{ source('raw', 'ins_osgo_oracle') }} g
        ON g.anketa_id = p.anketa_id
),

{{ payment_fifty_amounts_ctes('payment_context') }},

standard_payments AS (
    SELECT
        fa.oplata_id,
        fa.anketa_id,
        fa.polis_id,
        fa.user_id,
        fa.pym_date,
        fa.bc_id,
        fa.pturi_id,
        fa.opl_type,
        fa.opl_val,
        fa.opl_data,
        fa.oplata,
        fa.opl_summa,
        fa.val_kurs,
        fa.kommis_summa,
        fa.ins_div,
        fa.owner_id,
        fa.ins_otv,
        fa.ins_prem,
        fa.akt_type,
        fa.bank_k_id,
        fa.bank_orgname,
        fa.user_surname,
        fa.user_name,
        fa.mandatory,
        fa.product_category,
        fa.product_name,
        fa.oplsum,
        fa.kom_sum,
        {{ payment_fifty_select_columns() }},
        fa.is_osago_old
    FROM fifty_dop_amounts fa
),

osago_old_payments AS (
    SELECT
        o.tb_id                                                             AS oplata_id,
        t.tb_id                                                             AS anketa_id,
        NULL::NUMERIC                                                       AS polis_id,
        NULL::NUMERIC                                                       AS user_id,
        bc.pym_date,
        bc.ins_id                                                           AS bc_id,
        3::NUMERIC                                                          AS pturi_id,
        o.tb_typepl                                                         AS opl_type,
        NULL::NUMERIC                                                       AS opl_val,
        NULL::DATE                                                          AS opl_data,
        COALESCE(o.tb_summa, 0)                                             AS oplata,
        NULL::NUMERIC                                                       AS opl_summa,
        NULL::NUMERIC                                                       AS val_kurs,
        NULL::NUMERIC                                                       AS kommis_summa,
        NULL::NUMERIC                                                       AS ins_div,
        NULL::NUMERIC                                                       AS owner_id,
        NULL::NUMERIC                                                       AS ins_otv,
        NULL::NUMERIC                                                       AS ins_prem,
        NULL::NUMERIC                                                       AS akt_type,
        NULL::NUMERIC                                                       AS bank_k_id,
        NULL::TEXT                                                          AS bank_orgname,
        NULL::TEXT                                                          AS user_surname,
        NULL::TEXT                                                          AS user_name,
        1::NUMERIC                                                          AS mandatory,
        'OSAGO OLD'::TEXT                                                   AS product_category,
        'OSAGO OLD'::TEXT                                                   AS product_name,
        COALESCE(o.tb_summa, 0)                                             AS oplsum,
        0::NUMERIC                                                          AS kom_sum,
        0::NUMERIC                                                          AS fifty_zp,
        0::NUMERIC                                                          AS fifty_dop,
        0::NUMERIC                                                          AS fifty_director,
        TRUE                                                                AS is_osago_old
    FROM {{ source('raw', 'tb_anketa_oracle') }} t
    INNER JOIN {{ source('raw', 'tb_polis_oracle') }} p
        ON p.tb_anketa = t.tb_id
    INNER JOIN {{ source('raw', 'tb_oplata_oracle') }} o
        ON o.tb_anketa = t.tb_id
    INNER JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc
        ON bc.ins_id = o.bc_id
       AND bc.status = 2
       AND bc.pym_date >= DATE '2021-01-01'
    WHERE o.tb_typepl = 1
      AND p.tb_status IN (2, 8)
),

all_payments AS (
    SELECT * FROM standard_payments
    UNION ALL
    SELECT * FROM osago_old_payments
)

SELECT
    p.*,
    CASE
        WHEN p.is_osago_old THEN 'In-House - Internal - Not API'
        ELSE (
            CASE
                WHEN p.user_surname = 'KSC.UZ' AND p.user_name = 'WEB' THEN 'Website'
                WHEN p.bank_k_id IS NOT NULL THEN 'Banks'
                WHEN p.user_id IN (19588, 40791, 44788, 20879, 50788) THEN 'Banks'
                WHEN p.user_id IN (
                    19887, 20174, 20522, 20471, 20822, 20827, 20821,
                    20819, 20538, 20732, 20463, 20729, 82800, 64788, 20282
                ) THEN 'Marketplace'
                ELSE 'In-House'
            END
            || ' - '
            || CASE
                WHEN p.akt_type IN (0, 1, 2) THEN 'Agent'
                ELSE 'Internal'
               END
            || ' - '
            || CASE
                WHEN p.user_id IN (
                    82793, 40791, 64788, 21741, 20845, 20829, 20827,
                    20732, 20731, 20729, 20704, 20574, 20546, 20471,
                    20282, 20326, 20240, 20325, 19626, 20323, 19998,
                    19887, 19768, 20322, 19459, 20318, 19417, 20317,
                    19366, 20504, 20538, 20553, 20554, 20562, 20645,
                    20728, 20771, 20822, 20828, 20904, 21410, 21409,
                    21740, 21739, 21774, 40795, 72788, 82792, 82794,
                    82795, 19482, 19588, 19738, 20174, 20463, 20522,
                    20877, 19477, 21367, 21405, 21751, 35788, 44788,
                    67790, 8280, 21362
                ) THEN 'API'
                ELSE 'Not API'
               END
        )
    END                                                                 AS channels,
    CASE
        WHEN p.is_osago_old THEN 'Mandatory'
        WHEN p.mandatory = 1 THEN 'Mandatory'
        ELSE 'Voluntary'
    END                                                                 AS insurance_type,
    CASE
        WHEN p.bank_k_id IS NOT NULL THEN 'Banks - ' || p.bank_orgname
        WHEN p.user_id IN (19588, 40791, 44788, 20879, 50788)
            THEN 'Banks - ' || COALESCE(p.user_surname, '') || ' ' || COALESCE(p.user_name, '')
        ELSE NULL
    END                                                                 AS bank_name,
    COALESCE(p.fifty_zp, 0)
      + COALESCE(p.fifty_dop, 0)
      + COALESCE(p.fifty_director, 0)                                   AS fifty_total
FROM all_payments p
