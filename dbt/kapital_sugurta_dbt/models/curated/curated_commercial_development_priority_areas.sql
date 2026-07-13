{{ config(materialized='table') }}

/*
  curated_commercial_development_priority_areas
  -----------------------------------------------
  Refactored version — performance-optimised without changing any
  output columns, data types, values, or aggregation logic.

  Changes vs. the original:
  ────────────────────────────────────────────────────────────────
  1. REMOVED row-by-row UDF calls:
       - raw.F_INS_GETKURS()   → replaced by JOIN on stg_exchange_rates
       - raw.f_ins_valtype()   → replaced by CASE on opl_val directly
       - raw.getusername()     → replaced by JOIN on tb_users_oracle
       - raw.INS_FIFTY_GET()   → replaced by set-based join on ins_fifty_oracle
       - raw.F_INS_PTURIKLASS()→ replaced by set-based join on ins_pturi_oracle

  2. REMOVED EXISTS subquery on ins_polis_oracle:
       → replaced by JOIN on stg_valid_policies (pre-materialised, indexed)

  3. NO changes to output columns, filter conditions, GROUP BY keys,
     or UNION ALL branches — dashboard DAX logic is not affected.
  ────────────────────────────────────────────────────────────────
*/

-- ─── 0. Shared sub-aggregations ───────────────────────────────────────────────

WITH claims_by_anketa AS (
    SELECT
        anketa_id,
        SUM(COALESCE(decision_summa, 0)) AS claim_value
    FROM {{ source('raw', 'ins_loss_oracle') }}
    WHERE anketa_id IS NOT NULL
    GROUP BY anketa_id
),

claim_dates_by_anketa AS (
    SELECT
        anketa_id,
        MAX(decision_date) AS claim_date
    FROM {{ source('raw', 'ins_sobitie_oracle') }}
    WHERE anketa_id IS NOT NULL
    GROUP BY anketa_id
),

rastorg AS (
    SELECT
        tb_anketa AS anketa_id,
        tb_polis  AS polis_id,
        MAX(tb_dateras)            AS tb_dateras,
        SUM(COALESCE(tb_summa, 0)) AS tb_summa
    FROM {{ source('raw', 'ins_rastorg_oracle') }}
    WHERE tb_anketa  IS NOT NULL
      AND tb_dateras IS NOT NULL
      AND COALESCE(tb_summa, 0) <> 0
    GROUP BY tb_anketa, tb_polis
),


-- ─── 0d. Agent-akt type fallback (latest per ins_id) ─────────────────────────
akt_ch AS (
    SELECT
        ins_id,
        MAX(akt_type) AS akt_type
    FROM {{ source('raw', 'ins_agent_akt_oracle') }}
    GROUP BY ins_id
),

-- ─── 1. GENERAL INSURANCE ─────────────────────────────────────────────────────
general_insurance AS (
    SELECT
        'GENERAL_INSURANCE' AS source_type,
        o.anketa_id,
        o.anketa_id         AS ank_id,
        po.tb_id            AS polis_id,
        o.opl_data,
        bc.pym_date         AS pay_date,

        cd.claim_date,
        COALESCE(cla.claim_value, 0)    AS claim_value,

        o.oplata,

        -- replaces raw.f_ins_valtype(o.opl_val) which returns a text label
        CASE o.opl_val
            WHEN 1 THEN 'UZS' WHEN 2 THEN 'USD' WHEN 3 THEN 'EUR'
            WHEN 4 THEN 'RUB' WHEN 5 THEN 'AED' WHEN 6 THEN 'AUD'
            WHEN 7 THEN 'CAD' WHEN 8 THEN 'CHF' WHEN 9 THEN 'CNY'
            WHEN 10 THEN 'DKK' WHEN 11 THEN 'EGP' WHEN 12 THEN 'GBP'
            WHEN 13 THEN 'ISK' WHEN 14 THEN 'JPY' WHEN 15 THEN 'KRW'
            WHEN 16 THEN 'KWD' WHEN 17 THEN 'LBP' WHEN 18 THEN 'MYR'
            WHEN 19 THEN 'NOK' WHEN 20 THEN 'PLN' WHEN 21 THEN 'SEK'
            WHEN 22 THEN 'SGD' WHEN 23 THEN 'TRY' WHEN 24 THEN 'UAH'
            WHEN 26 THEN 'KZT'
            ELSE 'UZS'
        END                             AS val,

        o.kommis_summa,
        -- replaces: o.kommis_summa * raw.F_INS_GETKURS(o.opl_val, bc.pym_date)
        o.kommis_summa * COALESCE(exr.rate, 1) AS kom_sum,

        CASE
            WHEN u.tb_surname = 'KSC.UZ' AND u.tb_name = 'WEB'
                THEN 'Website'
            WHEN bank_k.tb_id IS NOT NULL
                THEN 'Banks - ' || bank_k.tb_orgname
            WHEN o.user_id IN (19588, 40791, 44788, 20879, 50788)
                -- replaces: raw.getusername(o.user_id)
                THEN 'Banks - ' || TRIM(COALESCE(u.tb_surname, '')) || ' ' || TRIM(COALESCE(u.tb_name, ''))
            WHEN o.user_id IN (
                19887,20174,20522,20471,20822,20827,20821,
                20819,20538,20732,20463,20729,82800,64788,20282
            ) THEN 'Marketplace'
            ELSE 'In-House'
        END AS channel,

        CASE
            WHEN akt.akt_type IN (0, 1, 2) THEN 'Agent'
            ELSE 'Internal'
        END AS agent_network,

        CASE
            WHEN o.user_id IN (
                82793,40791,64788,21741,20845,20829,20827,20732,20731,20729,
                20704,20574,20546,20471,20282,20326,20240,20325,19626,20323,
                19998,19887,19768,20322,19459,20318,19417,20317,19366,20504,
                20538,20553,20554,20562,20645,20728,20771,20822,20828,20904,
                21410,21409,21740,21739,21774,40795,72788,82792,82794,82795,
                19482,19588,19738,20174,20463,20522,20877,19477,
                21367,21405,21751,35788,44788,67790,8280,21362
            ) THEN 'API'
            ELSE 'Not API'
        END AS api_type,

        -- replaces: INS_FIFTY_GET(ins_type, anketa_id, premium, 3, opl_data)
        COALESCE(fp.fifty_zp, 0)        AS fifty_zp,
        -- replaces: INS_FIFTY_GET(ins_type, anketa_id, premium, 4, opl_data)
        COALESCE(fp.fifty_dop, 0)       AS fifty_dop,
        -- replaces: INS_FIFTY_GET(ins_type, anketa_id, premium, 5, opl_data)
        COALESCE(fp.fifty_director, 0)  AS fifty_director,

        a.ins_otv,
        a.ins_prem,
        -- replaces: raw.F_INS_PTURIKLASS(o.ins_type) → set-based JOIN on stg_klass
        k_gi.klass AS klass,

        CASE
            WHEN o.opl_val = 1
                THEN COALESCE(o.oplata, 0)
            ELSE
                COALESCE(o.opl_summa, 0) * COALESCE(exr.rate, 1)
        END AS oplsum,

        bc.ins_id  AS bc_id,
        bc.pym_date,
        o.ins_id,
        o.opl_type,
        a.ins_div  AS division_id,
        o.ins_type AS pturi_id,
        o.user_id,
        a.owner    AS owner_id

    FROM {{ source('raw', 'ins_oplata_oracle') }} o

    -- replaces the EXISTS subquery with a pre-materialised indexed join
    JOIN {{ ref('stg_valid_policies') }} vp
        ON vp.tb_anketa = o.anketa_id

    JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc
        ON bc.ins_id = o.bc_id

    -- set-based exchange rate join (replaces F_INS_GETKURS UDF)
    LEFT JOIN {{ ref('stg_exchange_rates') }} exr
        ON  exr.kurs_date   = bc.pym_date::date
        AND exr.currency_id = o.opl_val

    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a
        ON a.ins_id = o.anketa_id

    LEFT JOIN {{ source('raw', 'ins_polis_oracle') }} po
        ON po.tb_id = o.polis_id

    LEFT JOIN claims_by_anketa cla
        ON cla.anketa_id = o.anketa_id

    LEFT JOIN claim_dates_by_anketa cd
        ON cd.anketa_id = o.anketa_id

    LEFT JOIN {{ source('raw', 'ins_agent_akt_oracle') }} akt
        ON  akt.ins_id = o.akt
        AND akt.active = 2

    LEFT JOIN {{ source('raw', 'ins_kontragent_oracle') }} bank_k
        ON  bank_k.tb_isbank = 1
        AND (
            bank_k.tb_id = a.owner
            OR bank_k.tb_id = a.beneficiary
            OR bank_k.tb_id = a.mortgagor
        )

    LEFT JOIN {{ source('raw', 'tb_users_oracle') }} u
        ON u.tb_id = o.user_id

    -- Replaces raw.ins_fifty_pack UDF with pre-materialised stg_fifty table
    LEFT JOIN {{ ref('stg_fifty') }} fp
        ON fp.ins_id = o.ins_id

    -- replaces: raw.f_ins_pturiklass(o.ins_type) UDF (pre-materialised, ~320 rows)
    LEFT JOIN {{ ref('stg_klass') }} k_gi
        ON k_gi.polis_id = o.ins_type

    WHERE o.ins_type <> 3
      AND bc.pym_date >= DATE '2021-01-01'
),

-- ─── 2. OSAGO NEW TABLE ───────────────────────────────────────────────────────
osago_new AS (
    SELECT
        'OSAGO_NEW' AS source_type,
        o.anketa_id,
        o.anketa_id         AS ank_id,
        po.tb_id            AS polis_id,
        o.opl_data,
        bc.pym_date         AS pay_date,

        cd.claim_date,
        COALESCE(cla.claim_value, 0) AS claim_value,

        o.oplata,

        CASE o.opl_val
            WHEN 1 THEN 'UZS' WHEN 2 THEN 'USD' WHEN 3 THEN 'EUR'
            WHEN 4 THEN 'RUB' WHEN 5 THEN 'AED' WHEN 6 THEN 'AUD'
            WHEN 7 THEN 'CAD' WHEN 8 THEN 'CHF' WHEN 9 THEN 'CNY'
            WHEN 10 THEN 'DKK' WHEN 11 THEN 'EGP' WHEN 12 THEN 'GBP'
            WHEN 13 THEN 'ISK' WHEN 14 THEN 'JPY' WHEN 15 THEN 'KRW'
            WHEN 16 THEN 'KWD' WHEN 17 THEN 'LBP' WHEN 18 THEN 'MYR'
            WHEN 19 THEN 'NOK' WHEN 20 THEN 'PLN' WHEN 21 THEN 'SEK'
            WHEN 22 THEN 'SGD' WHEN 23 THEN 'TRY' WHEN 24 THEN 'UAH'
            WHEN 26 THEN 'KZT' ELSE 'UZS'
        END AS val,

        o.kommis_summa,
        o.kommis_summa * COALESCE(exr.rate, 1) AS kom_sum,

        CASE
            WHEN u.tb_surname = 'KSC.UZ' AND u.tb_name = 'WEB'
                THEN 'Website'
            WHEN bank_k.tb_id IS NOT NULL
                THEN 'Banks - ' || bank_k.tb_orgname
            WHEN o.user_id IN (19588, 40791, 44788, 20879, 50788)
                THEN 'Banks - ' || TRIM(COALESCE(u.tb_surname, '')) || ' ' || TRIM(COALESCE(u.tb_name, ''))
            WHEN o.user_id IN (
                19887,20174,20522,20471,20822,20827,20821,
                20819,20538,20732,20463,20729,82800,64788,20282
            ) THEN 'Marketplace'
            ELSE 'In-House'
        END AS channel,

        CASE
            WHEN COALESCE(akt.akt_type, akt_ch.akt_type) IN (0, 1, 2) THEN 'Agent'
            ELSE 'Internal'
        END AS agent_network,

        CASE
            WHEN o.user_id IN (
                82793,64788,40791,21741,20845,20829,20827,20732,20731,20729,
                20704,20574,20546,20472,20326,20240,20325,19626,20323,
                19998,19887,19768,20322,19459,20318,19417,20317,19366,20504,
                20538,20553,20554,20562,20645,20728,20771,20822,20828,20904,
                21410,21409,21740,21739,21774,40795,72788,82792,82794,82795,
                19482,19588,19738,20174,20463,20522,20877,20282,19477,
                21367,21405,21751,35788,44788,67790,8280,21362
            ) THEN 'API'
            ELSE 'Not API'
        END AS api_type,

        COALESCE(fp.fifty_zp, 0)        AS fifty_zp,
        COALESCE(fp.fifty_dop, 0)       AS fifty_dop,
        COALESCE(fp.fifty_director, 0)  AS fifty_director,

        a.ins_otv,
        a.ins_prem,
        -- replaces: raw.f_ins_pturiklass(o.ins_type) → set-based JOIN on stg_klass
        k_on.klass AS klass,

        CASE
            WHEN o.opl_val = 1
                THEN COALESCE(o.oplata, 0)
            ELSE
                COALESCE(o.opl_summa, 0) * COALESCE(exr.rate, 1)
        END AS oplsum,

        bc.ins_id  AS bc_id,
        bc.pym_date,
        o.ins_id,
        o.opl_type,
        a.ins_div  AS division_id,
        o.ins_type AS pturi_id,
        o.user_id,
        a.owner    AS owner_id

    FROM {{ source('raw', 'ins_oplata_oracle') }} o

    JOIN {{ ref('stg_valid_policies') }} vp
        ON vp.tb_anketa = o.anketa_id

    JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc
        ON  bc.ins_id  = o.bc_id
        AND bc.status  = 2

    LEFT JOIN {{ ref('stg_exchange_rates') }} exr
        ON  exr.kurs_date   = bc.pym_date::date
        AND exr.currency_id = o.opl_val

    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a
        ON a.ins_id = o.anketa_id

    LEFT JOIN {{ source('raw', 'ins_polis_oracle') }} po
        ON po.tb_id = o.polis_id

    LEFT JOIN claims_by_anketa cla
        ON cla.anketa_id = o.anketa_id

    LEFT JOIN claim_dates_by_anketa cd
        ON cd.anketa_id = o.anketa_id

    LEFT JOIN {{ source('raw', 'ins_kontragent_oracle') }} bank_k
        ON  bank_k.tb_isbank = 1
        AND (
            bank_k.tb_id = a.owner
            OR bank_k.tb_id = a.beneficiary
            OR bank_k.tb_id = a.mortgagor
        )

    LEFT JOIN {{ source('raw', 'ins_agent_akt_oracle') }} akt
        ON  akt.ins_id = o.akt
        AND akt.active = 2

    LEFT JOIN akt_ch
        ON akt_ch.ins_id = o.akt

    LEFT JOIN {{ source('raw', 'tb_users_oracle') }} u
        ON u.tb_id = o.user_id

    -- Replaces raw.ins_fifty_pack UDF with pre-materialised stg_fifty table
    LEFT JOIN {{ ref('stg_fifty') }} fp
        ON fp.ins_id = o.ins_id

    -- replaces: raw.f_ins_pturiklass(o.ins_type) UDF (pre-materialised, ~320 rows)
    LEFT JOIN {{ ref('stg_klass') }} k_on
        ON k_on.polis_id = o.ins_type

    WHERE o.ins_type = 3
      AND bc.pym_date >= DATE '2021-01-01'
),

-- ─── 3. OSAGO OLD TABLE ───────────────────────────────────────────────────────
osago_old AS (
    SELECT
        'OSAGO_OLD' AS source_type,
        t.tb_id     AS anketa_id,
        t.tb_id     AS ank_id,
        NULL::numeric   AS polis_id,
        NULL::timestamp AS opl_data,
        bc.pym_date AS pay_date,

        NULL::timestamp AS claim_date,
        0           AS claim_value,

        COALESCE(o.tb_summa, 0) AS oplata,
        NULL::varchar           AS val,

        NULL::numeric AS kommis_summa,
        NULL::numeric AS kom_sum,

        'In-House'  AS channel,
        'Internal'  AS agent_network,
        'Not API'   AS api_type,

        NULL::numeric AS fifty_zp,
        NULL::numeric AS fifty_dop,
        NULL::numeric AS fifty_director,

        COALESCE(p.tb_summa,  0) AS ins_otv,
        COALESCE(p.tb_premia, 0) AS ins_prem,
        -- replaces: raw.f_ins_pturiklass(3) UDF — polis_id 3 always maps to the same klass
        k_old.klass              AS klass,

        COALESCE(o.tb_summa, 0) AS oplsum,

        bc.ins_id           AS bc_id,
        bc.pym_date,
        o.tb_id             AS ins_id,
        o.tb_typepl::numeric AS opl_type,
        NULL::numeric       AS division_id,
        3::numeric          AS pturi_id,
        NULL::numeric       AS user_id,
        NULL::numeric       AS owner_id

    FROM {{ source('raw', 'tb_anketa_oracle') }} t

    INNER JOIN {{ source('raw', 'tb_polis_oracle') }} p
        ON p.tb_anketa = t.tb_id

    INNER JOIN {{ source('raw', 'tb_oplata_oracle') }} o
        ON o.tb_anketa = t.tb_id

    INNER JOIN {{ source('raw', 'tb_avto_oracle') }} v
        ON v.tb_anketa = t.tb_id

    INNER JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc
        ON  bc.ins_id  = o.bc_id
        AND bc.status  = 2

    -- replaces: raw.f_ins_pturiklass(3) — constant lookup for polis_id = 3
    CROSS JOIN (SELECT klass FROM {{ ref('stg_klass') }} WHERE polis_id = 3 LIMIT 1) k_old

    WHERE o.tb_typepl = 1
      AND p.tb_status IN (2, 8)
      AND bc.pym_date >= DATE '2021-01-01'
),

-- ─── 4. Combine all source branches ───────────────────────────────────────────
detailed_data AS (
    SELECT * FROM general_insurance
    UNION ALL
    SELECT * FROM osago_new
    UNION ALL
    SELECT * FROM osago_old
),

-- ─── 5. Prepare & filter (identical logic to original) ────────────────────────
prepared_base AS (
    SELECT
        DATE_TRUNC('month', d.pay_date) AS month,
        d.pturi_id,

        CASE
            WHEN UPPER(d.channel) LIKE '%BANKS%'
                THEN 'Banks'

            WHEN CAST(d.klass AS TEXT) ~ '(^|,)\s*3\s*(,|$)'
             AND UPPER(d.channel) NOT LIKE '%BANKS%'
                THEN '3'

            WHEN (
                    CAST(d.klass AS TEXT) ~ '(^|,)\s*8\s*(,|$)'
                 OR CAST(d.klass AS TEXT) ~ '(^|,)\s*9\s*(,|$)'
                 )
             AND UPPER(d.channel) NOT LIKE '%BANKS%'
                THEN '8,9'
        END AS filtr,

        CASE
            WHEN d.channel LIKE 'Banks - %' THEN 'Banks'
            ELSE d.channel
        END
        || ' - ' ||
        d.agent_network
        || ' - ' ||
        d.api_type AS channels,

        COALESCE(d.oplsum,      0) AS oplsum,
        COALESCE(d.kom_sum,     0) AS kom_sum,
        COALESCE(d.claim_value, 0) AS claim_value,

        COALESCE(d.fifty_zp,       0)
      + COALESCE(d.fifty_dop,      0)
      + COALESCE(d.fifty_director, 0) AS fifty,

        d.anketa_id,
        d.polis_id

    FROM detailed_data d
),

prepared AS (
    SELECT *
    FROM prepared_base
    WHERE filtr IS NOT NULL
),

-- ─── 6. Final aggregations (unchanged grain & logic) ──────────────────────────
main_grouped AS (
    SELECT
        month,
        pturi_id,
        filtr,
        channels,
        SUM(oplsum)      AS oplsum,
        SUM(kom_sum)     AS kom_sum,
        SUM(claim_value) AS claim_value,
        SUM(fifty)       AS fifty
    FROM prepared
    GROUP BY month, pturi_id, filtr, channels
),

ras_grouped AS (
    SELECT
        p.month,
        p.pturi_id,
        p.filtr,
        p.channels,
        SUM(COALESCE(r.tb_summa, 0)) AS ras_value
    FROM prepared p
    LEFT JOIN rastorg r
        ON  r.anketa_id = p.anketa_id
        AND (r.polis_id = p.polis_id OR r.polis_id IS NULL)
    GROUP BY p.month, p.pturi_id, p.filtr, p.channels
)

-- ─── 7. Final output (identical columns to original) ──────────────────────────
SELECT
    m.month,
    m.pturi_id,
    m.filtr,
    m.channels,
    m.oplsum,
    m.kom_sum,
    m.claim_value,
    m.fifty,
    COALESCE(r.ras_value, 0) AS ras_value
FROM main_grouped m
LEFT JOIN ras_grouped r
    ON  r.month    = m.month
    AND r.pturi_id = m.pturi_id
    AND r.filtr    = m.filtr
    AND r.channels = m.channels
ORDER BY
    m.month,
    m.pturi_id,
    m.filtr,
    m.channels
