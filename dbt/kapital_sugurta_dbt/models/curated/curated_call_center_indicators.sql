{{ config(materialized='view') }}

/*
  Call Center Channel Indicators (Curated)
  -----------------------------------------
  Isolates all sales attributed to the Call Center division
  (raw.dec_division(sp_id, 1) = 'ОТДЕЛ РАЗВИТИЯ КАНАЛОВ ПРОДАЖ')
  and computes:
    - oplsum        : Premium in UZS (currency-converted)
    - strah_summa   : Sum Insured in UZS (currency-converted)
    - kom_sum       : Commission (0 — Call Center has no agent commission)
    - claim_value   : Claims (0 — not tracked at this level)
    - fifty         : Motivation (0 — not applicable)
    - ras_value     : Reinsurance cession (0 — not applicable)

  Schema is intentionally identical to curated_partner_indicators
  so that mart_own_sales can UNION ALL without column mapping.

  Source types (product_category):
    - 'GENERAL_INSURANCE' : Standard new policies (ins_type <> 3)
    - 'OSAGO'             : OSAGO policies — both OLD and NEW paths
*/

-- ──────────────────────────────────────────────
-- 1. Payment date → report_month bucketing
-- ──────────────────────────────────────────────
WITH bc_filtered AS MATERIALIZED (

    SELECT
        ins_id,
        status,
        DATE_TRUNC('month', pym_date)::date AS report_month
    FROM {{ source('raw', 'ins_bank_client_oracle') }}
    WHERE pym_date >= DATE '2021-01-01'

),

-- ──────────────────────────────────────────────
-- 2. Call Center division members
-- ──────────────────────────────────────────────
division_map AS MATERIALIZED (

    SELECT
        sp_id
    FROM {{ source('raw', 'sp_division_oracle') }}
    WHERE raw.dec_division(sp_id, 1) = 'ОТДЕЛ РАЗВИТИЯ КАНАЛОВ ПРОДАЖ'

),

-- ──────────────────────────────────────────────
-- 3. Valid policy statuses (same filter as curated_partner_indicators)
-- ──────────────────────────────────────────────
valid_ins_polis AS MATERIALIZED (

    SELECT DISTINCT tb_anketa
    FROM {{ source('raw', 'ins_polis_oracle') }}
    WHERE tb_status IN (2, 9, 10)

),

-- ──────────────────────────────────────────────
-- 4. Exchange rates  (one row per calendar day)
-- ──────────────────────────────────────────────
kurs_map AS MATERIALIZED (

    SELECT DISTINCT ON (kurs_date::date)
        kurs_date::date AS kurs_day,
        kurs_usd,
        kurs_eur,
        kurs_rub,
        kurs_aed,
        kurs_aud,
        kurs_cad,
        kurs_chf,
        kurs_cny,
        kurs_dkk,
        kurs_egp,
        kurs_gbp,
        kurs_isk,
        kurs_jpy,
        kurs_krw,
        kurs_kwd,
        kurs_lbp,
        kurs_myr,
        kurs_nok,
        kurs_pln,
        kurs_sek,
        kurs_sgd,
        kurs_try,
        kurs_uah,
        kurs_kzt
    FROM {{ source('raw', 'ins_kurs_oracle') }}
    ORDER BY kurs_date::date, kurs_date

),

-- ──────────────────────────────────────────────
-- 5. Payments enriched with report_month + kurs
-- ──────────────────────────────────────────────
ins_oplata_prepared AS MATERIALIZED (

    SELECT
        o.*,
        bc.report_month,
        bc.status AS bc_status,
        CASE
            WHEN o.opl_val =  1 THEN 1
            WHEN o.opl_val =  2 THEN km.kurs_usd
            WHEN o.opl_val =  3 THEN km.kurs_eur
            WHEN o.opl_val =  4 THEN km.kurs_rub
            WHEN o.opl_val =  5 THEN km.kurs_aed
            WHEN o.opl_val =  6 THEN km.kurs_aud
            WHEN o.opl_val =  7 THEN km.kurs_cad
            WHEN o.opl_val =  8 THEN km.kurs_chf
            WHEN o.opl_val =  9 THEN km.kurs_cny
            WHEN o.opl_val = 10 THEN km.kurs_dkk
            WHEN o.opl_val = 11 THEN km.kurs_egp
            WHEN o.opl_val = 12 THEN km.kurs_gbp
            WHEN o.opl_val = 13 THEN km.kurs_isk
            WHEN o.opl_val = 14 THEN km.kurs_jpy
            WHEN o.opl_val = 15 THEN km.kurs_krw
            WHEN o.opl_val = 16 THEN km.kurs_kwd
            WHEN o.opl_val = 17 THEN km.kurs_lbp
            WHEN o.opl_val = 18 THEN km.kurs_myr
            WHEN o.opl_val = 19 THEN km.kurs_nok
            WHEN o.opl_val = 20 THEN km.kurs_pln
            WHEN o.opl_val = 21 THEN km.kurs_sek
            WHEN o.opl_val = 22 THEN km.kurs_sgd
            WHEN o.opl_val = 23 THEN km.kurs_try
            WHEN o.opl_val = 24 THEN km.kurs_uah
            WHEN o.opl_val = 26 THEN km.kurs_kzt
            ELSE NULL
        END AS kurs
    FROM {{ source('raw', 'ins_oplata_oracle') }} o
    INNER JOIN bc_filtered bc
        ON  bc.ins_id = o.bc_id
    LEFT JOIN kurs_map km
        ON  km.kurs_day = o.opl_data::date

),

-- ──────────────────────────────────────────────
-- 6. Raw rows per source_type
--    GENERAL_INSURANCE | OSAGO (old) | OSAGO (new)
-- ──────────────────────────────────────────────
call_center_detail AS (

    -- ── Path A: General Insurance (new system, non-OSAGO) ──────────────────
    SELECT
        o.report_month,
        'GENERAL_INSURANCE'                                             AS source_type,
        CASE
            WHEN o.opl_val = 1 THEN COALESCE(o.oplata, 0)
            ELSE COALESCE(o.opl_summa, 0) * COALESCE(o.kurs, 1)
        END                                                             AS oplsum,
        CASE
            WHEN o.opl_val = 1 THEN COALESCE(a.ins_otv, 0)
            ELSE COALESCE(a.ins_otv, 0) * COALESCE(o.kurs, 1)
        END                                                             AS strah_summa
    FROM ins_oplata_prepared o
    INNER JOIN {{ source('raw', 'ins_anketa_oracle') }}  a  ON  a.ins_id       = o.anketa_id
    INNER JOIN valid_ins_polis                           vp ON  vp.tb_anketa   = o.anketa_id
    INNER JOIN division_map                              d  ON  d.sp_id        = COALESCE(a.temp_div, a.ins_div)
    WHERE o.ins_type <> 3

    UNION ALL

    -- ── Path B: Old OSAGO (legacy tb_ tables) ─────────────────────────────
    SELECT
        bc.report_month,
        'OSAGO'                                                         AS source_type,
        0                                                               AS oplsum,
        COALESCE(p.tb_summa, 0)                                        AS strah_summa
    FROM {{ source('raw', 'tb_anketa_oracle') }} t
    INNER JOIN {{ source('raw', 'tb_polis_oracle') }}   p   ON  p.tb_anketa   = t.tb_id
    INNER JOIN {{ source('raw', 'tb_oplata_oracle') }}  o   ON  o.tb_anketa   = t.tb_id
    INNER JOIN bc_filtered                               bc  ON  bc.ins_id     = o.bc_id
                                                            AND bc.status      = 2
    INNER JOIN division_map                              d   ON  d.sp_id       = t.tb_division
    WHERE o.tb_typepl = 1
      AND p.tb_status IN (2, 8)

    UNION ALL

    -- ── Path C: New OSAGO (new system, ins_type = 3) ───────────────────────
    SELECT
        o.report_month,
        'OSAGO'                                                         AS source_type,
        CASE
            WHEN o.opl_val = 1 THEN COALESCE(o.oplata, 0)
            ELSE COALESCE(o.oplata, 0) * COALESCE(o.val_kurs, 1)
        END                                                             AS oplsum,
        COALESCE(a.ins_otv, 0)                                         AS strah_summa
    FROM ins_oplata_prepared o
    INNER JOIN {{ source('raw', 'ins_anketa_oracle') }}  a  ON  a.ins_id       = o.anketa_id
    INNER JOIN valid_ins_polis                           vp ON  vp.tb_anketa   = o.anketa_id
    INNER JOIN division_map                              d  ON  d.sp_id        = COALESCE(a.temp_div, a.ins_div)
    WHERE o.ins_type  = 3
      AND o.bc_status = 2

)

-- ──────────────────────────────────────────────
-- 7. Final output — aggregated, schema-aligned with curated_partner_indicators
-- ──────────────────────────────────────────────
SELECT
    report_month                        AS month,

    -- Channel / dimension columns (fixed values for Call Center)
    'Call Center'                       AS channels,
    'Call Center'                       AS insurance_type,
    source_type                         AS product_category,  -- 'GENERAL_INSURANCE' or 'OSAGO'
    'Call Center'                       AS product_name,

    -- Metrics
    SUM(oplsum)                         AS oplsum,
    0::NUMERIC                          AS kom_sum,
    0::NUMERIC                          AS claim_value,
    0::NUMERIC                          AS fifty,
    0::NUMERIC                          AS ras_value,

    -- Call Center-specific: Sum Insured
    SUM(strah_summa)                    AS strah_summa

FROM call_center_detail
GROUP BY
    report_month,
    source_type

ORDER BY
    report_month,
    source_type
