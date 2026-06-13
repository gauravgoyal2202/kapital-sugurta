{{ config(materialized='view') }}

/*
  curated_partner_indicators_detail
  -----------------------------------
  Extended version of curated_partner_indicators that adds `user_id`
  to the aggregation grain.

  This allows the presentation mart to LEFT JOIN tb_users_oracle and
  expose real partner names (partner_surname, partner_name) per row.

  All business logic is identical to curated_partner_indicators.
  Only the grain is different:
    curated_partner_indicators       → month × channels × insurance_type × product_category × product_name
    curated_partner_indicators_detail → month × user_id × channels × insurance_type × product_category × product_name
*/

-- ──────────────────────────────────────────────
-- 1. Only ankets whose policy is in a valid state
-- ──────────────────────────────────────────────
WITH valid_polis_anketa AS (

    SELECT DISTINCT tb_anketa
    FROM {{ source('raw', 'ins_polis_oracle') }}
    WHERE tb_status IN (2, 9, 10)

),

-- ──────────────────────────────────────────────
-- 2. Claims  (from ins_loss_oracle)
-- ──────────────────────────────────────────────
claims_by_anketa AS (

    SELECT
        anketa_id,
        SUM(COALESCE(decision_summa, 0)) AS claim_value
    FROM {{ source('raw', 'ins_loss_oracle') }}
    WHERE anketa_id IS NOT NULL
    GROUP BY anketa_id

),

-- ──────────────────────────────────────────────
-- 3. Rastorg (reinsurance / cession payments)
-- ──────────────────────────────────────────────
rastorg AS (

    SELECT
        tb_anketa AS anketa_id,
        tb_polis  AS polis_id,
        SUM(COALESCE(tb_summa, 0)) AS tb_summa
    FROM {{ source('raw', 'ins_rastorg_oracle') }}
    WHERE tb_anketa   IS NOT NULL
      AND tb_dateras  IS NOT NULL
      AND COALESCE(tb_summa, 0) <> 0
    GROUP BY tb_anketa, tb_polis

),

-- ──────────────────────────────────────────────
-- 4. Agent-akt type fallback (latest per ins_id)
-- ──────────────────────────────────────────────
akt_ch AS (

    SELECT
        ins_id,
        MAX(akt_type) AS akt_type
    FROM {{ source('raw', 'ins_agent_akt_oracle') }}
    GROUP BY ins_id

),

-- ──────────────────────────────────────────────
-- 5. Main data — new insurance premiums
--    (same as curated_partner_indicators but exposes o.user_id)
-- ──────────────────────────────────────────────
main_data AS (

    SELECT

        DATE_TRUNC('month', bc.pym_date)::date AS month,

        -- ── user_id (NEW — for partner name join) ────────────────────────
        o.user_id,

        -- ── Channel composite label ───────────────────────────────────────
        (
            CASE
                WHEN u.tb_surname = 'KSC.UZ' AND u.tb_name = 'WEB'
                    THEN 'Website'
                WHEN bank_k.tb_id IS NOT NULL
                    THEN 'Banks'
                WHEN o.user_id IN (19588, 40791, 44788, 20879, 50788)
                    THEN 'Banks'
                WHEN o.user_id IN (
                    19887, 20174, 20522, 20471, 20822, 20827, 20821,
                    20819, 20538, 20732, 20463, 20729, 82800, 64788, 20282
                )
                    THEN 'Marketplace'
                ELSE 'In-House'
            END

            || ' - ' ||

            CASE
                WHEN COALESCE(akt.akt_type, akt_ch.akt_type) IN (0, 1, 2)
                    THEN 'Agent'
                ELSE 'Internal'
            END

            || ' - ' ||

            CASE
                WHEN o.user_id IN (
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
                )
                    THEN 'API'
                ELSE 'Not API'
            END
        ) AS channels,

        CASE
            WHEN pt.mandatory = 1 THEN 'Mandatory'
            ELSE 'Voluntary'
        END AS insurance_type,

        COALESCE(v_cat.name3, 'Other') AS product_category,
        COALESCE(pt.polis_name, 'Other') AS product_name,

        -- Premium in UZS
        CASE
            WHEN o.opl_val = 1
                THEN COALESCE(o.oplata, 0)
            ELSE
                COALESCE(o.opl_summa, 0) * COALESCE(kurs.kurs_value, 0)
        END AS oplsum,

        -- Commission in UZS
        COALESCE(o.kommis_summa, 0) * COALESCE(kurs.kurs_value, 0) AS kom_sum,

        COALESCE(cla.claim_value, 0) AS claim_value,

        COALESCE(fpack.fifty_total, 0) AS fifty,

        o.anketa_id,
        po.tb_id AS polis_id,

        -- Bank Name mapping from client query
        CASE
            WHEN bank_k.tb_id IS NOT NULL THEN 'Banks - ' || bank_k.tb_orgname
            WHEN o.user_id IN (19588, 40791, 44788, 20879, 50788) THEN 'Banks - ' || TRIM(u.tb_surname) || ' ' || TRIM(u.tb_name)
            ELSE NULL
        END AS bank_name

    FROM {{ source('raw', 'ins_oplata_oracle') }} o

    JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc
        ON  bc.ins_id   = o.bc_id
        AND bc.pym_date >= DATE '2021-01-01'

    JOIN valid_polis_anketa vp
        ON vp.tb_anketa = o.anketa_id

    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a
        ON a.ins_id = o.anketa_id

    LEFT JOIN {{ source('raw', 'ins_polis_oracle') }} po
        ON po.tb_id = o.polis_id

    LEFT JOIN {{ source('raw', 'tb_users_oracle') }} u
        ON u.tb_id = o.user_id

    LEFT JOIN {{ source('raw', 'ins_agent_akt_oracle') }} akt
        ON  akt.ins_id = o.akt
        AND akt.active = 2

    LEFT JOIN akt_ch
        ON akt_ch.ins_id = o.akt

    LEFT JOIN {{ source('raw', 'ins_kontragent_oracle') }} bank_k
        ON  bank_k.tb_isbank = 1
        AND bank_k.tb_id IN (a.owner, a.beneficiary, a.mortgagor)

    LEFT JOIN claims_by_anketa cla
        ON cla.anketa_id = o.anketa_id

    LEFT JOIN {{ source('raw', 'ins_pturi_oracle') }} pt
        ON pt.ins_id = a.ins_type

    LEFT JOIN {{ source('raw', 'ins_vertical_oracle') }} v_cat
        ON v_cat.ins_id = pt.vertical

    LEFT JOIN {{ source('raw', 'ins_kurs_oracle') }} k
        ON k.kurs_date::date = bc.pym_date::date

    CROSS JOIN LATERAL (
        SELECT CASE o.opl_val
            WHEN  1 THEN 1
            WHEN  2 THEN k.kurs_usd
            WHEN  3 THEN k.kurs_eur
            WHEN  4 THEN k.kurs_rub
            WHEN  5 THEN k.kurs_aed
            WHEN  6 THEN k.kurs_aud
            WHEN  7 THEN k.kurs_cad
            WHEN  8 THEN k.kurs_chf
            WHEN  9 THEN k.kurs_cny
            WHEN 10 THEN k.kurs_dkk
            WHEN 11 THEN k.kurs_egp
            WHEN 12 THEN k.kurs_gbp
            WHEN 13 THEN k.kurs_isk
            WHEN 14 THEN k.kurs_jpy
            WHEN 15 THEN k.kurs_krw
            WHEN 16 THEN k.kurs_kwd
            WHEN 17 THEN k.kurs_lbp
            WHEN 18 THEN k.kurs_myr
            WHEN 19 THEN k.kurs_nok
            WHEN 20 THEN k.kurs_pln
            WHEN 21 THEN k.kurs_sek
            WHEN 22 THEN k.kurs_sgd
            WHEN 23 THEN k.kurs_try
            WHEN 24 THEN k.kurs_uah
            WHEN 26 THEN k.kurs_kzt
            ELSE NULL
        END AS kurs_value
    ) kurs

    CROSS JOIN LATERAL raw.ins_fifty_pack(
        a.ins_type,
        o.anketa_id,
        CASE
            WHEN o.opl_val = 1
                THEN o.oplata
            ELSE
                o.opl_summa * o.val_kurs
        END,
        o.opl_data
    ) AS fpack(
        fifty_zp,
        fifty_dop,
        fifty_director,
        fifty_total
    )

    WHERE (
        o.ins_type <> 3
        OR (
            o.ins_type = 3
            AND bc.status = 2
        )
    )

    -- ── OSAGO OLD  (legacy policies) ─────────────────────────────────────
    UNION ALL

    SELECT

        DATE_TRUNC('month', bc.pym_date)::date AS month,

        NULL::numeric                           AS user_id,   -- no user on legacy OSAGO

        'In-House - Internal - Not API'         AS channels,

        'Mandatory'                             AS insurance_type,
        'OSAGO OLD'                             AS product_category,
        'OSAGO OLD'                             AS product_name,

        COALESCE(o.tb_summa, 0)                 AS oplsum,
        0                                       AS kom_sum,
        0                                       AS claim_value,
        0                                       AS fifty,

        t.tb_id                                 AS anketa_id,
        NULL::numeric                           AS polis_id,
        NULL::varchar                           AS bank_name

    FROM {{ source('raw', 'tb_anketa_oracle') }} t

    JOIN {{ source('raw', 'tb_polis_oracle') }} p
        ON p.tb_anketa = t.tb_id

    JOIN {{ source('raw', 'tb_oplata_oracle') }} o
        ON o.tb_anketa = t.tb_id

    JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc
        ON  bc.ins_id   = o.bc_id
        AND bc.status   = 2
        AND bc.pym_date >= DATE '2021-01-01'

    WHERE o.tb_typepl = 1
      AND p.tb_status IN (2, 8)

),

-- ──────────────────────────────────────────────
-- 6. Aggregate metrics — grain includes user_id
-- ──────────────────────────────────────────────
main_grouped AS (

    SELECT
        month,
        user_id,
        channels,
        insurance_type,
        product_category,
        product_name,
        bank_name,
        SUM(oplsum)      AS oplsum,
        SUM(kom_sum)     AS kom_sum,
        SUM(claim_value) AS claim_value,
        SUM(fifty)       AS fifty
    FROM main_data
    GROUP BY
        month,
        user_id,
        channels,
        insurance_type,
        product_category,
        product_name,
        bank_name

),

-- ──────────────────────────────────────────────
-- 7. Rastorg aggregated along the same dimensions (+ user_id)
-- ──────────────────────────────────────────────
ras_grouped AS (

    SELECT
        d.month,
        d.user_id,
        d.channels,
        d.insurance_type,
        d.product_category,
        d.product_name,
        d.bank_name,
        SUM(COALESCE(r.tb_summa, 0)) AS ras_value
    FROM main_data d
    LEFT JOIN rastorg r
        ON  r.anketa_id = d.anketa_id
        AND (r.polis_id = d.polis_id OR r.polis_id IS NULL)
    GROUP BY
        d.month,
        d.user_id,
        d.channels,
        d.insurance_type,
        d.product_category,
        d.product_name,
        d.bank_name

)

-- ──────────────────────────────────────────────
-- 8. Final output — includes user_id for partner name join
-- ──────────────────────────────────────────────
SELECT
    m.month,
    m.user_id,
    m.channels,
    m.insurance_type,
    m.product_category,
    m.product_name,
    m.oplsum,
    m.kom_sum,
    m.claim_value,
    m.fifty,
    m.bank_name,
    COALESCE(r.ras_value, 0) AS ras_value

FROM main_grouped m

LEFT JOIN ras_grouped r
    ON  r.month            = m.month
    AND (r.user_id = m.user_id OR (r.user_id IS NULL AND m.user_id IS NULL))
    AND r.channels         = m.channels
    AND r.insurance_type   = m.insurance_type
    AND r.product_category = m.product_category
    AND r.product_name     = m.product_name
    AND (r.bank_name = m.bank_name OR (r.bank_name IS NULL AND m.bank_name IS NULL))

ORDER BY
    m.month,
    m.channels,
    m.insurance_type,
    m.product_category,
    m.product_name
