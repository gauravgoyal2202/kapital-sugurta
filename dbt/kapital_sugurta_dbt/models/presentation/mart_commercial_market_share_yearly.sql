{{ config(materialized='table') }}

/*
  Dashboard 11 — Commercial Development: Market Share (YEARLY)
  ------------------------------------------------------------
  One row per year per priority area (Motor / Property).
  Stores company premium and market premium side-by-side.
  DAX calculates the share percentage.
  market_share_pct = 0 when market premium is unavailable.

  Company side  → raw.ins_oplata_oracle and others directly
  Market side   → raw.market_share_insurance_class_stats (total_premium)
*/

-- ────────────────────────────────────────────────────────────────────────────
-- 1. Company yearly premium (Motor + Property only)
-- ────────────────────────────────────────────────────────────────────────────
WITH detailed_data AS (

    /* =========================
       1) GENERAL INSURANCE
       ========================= */
    SELECT
        bc.pym_date AS pay_date,
        raw.f_ins_pturiklass(o.ins_type) AS klass,
        CASE
            WHEN o.opl_val = 1 THEN COALESCE(o.oplata, 0)
            ELSE COALESCE(o.opl_summa, 0)
                 * raw.f_ins_getkurs(o.opl_val, bc.pym_date)
        END AS oplsum
    FROM {{ source('raw', 'ins_oplata_oracle') }} o
    LEFT JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc
        ON o.bc_id = bc.ins_id
    WHERE o.ins_type <> 3
      AND EXISTS (
            SELECT 1
            FROM {{ source('raw', 'ins_polis_oracle') }} p
            WHERE p.tb_status IN (2, 9, 10)
              AND p.tb_anketa = o.anketa_id
      )
      AND bc.pym_date >= DATE '2021-01-01'

    UNION ALL

    /* =========================
       2) OSAGO NEW TABLE
       ========================= */
    SELECT
        bc.pym_date AS pay_date,
        raw.f_ins_pturiklass(o.ins_type) AS klass,
        CASE
            WHEN o.opl_val = 1 THEN COALESCE(o.oplata, 0)
            ELSE COALESCE(o.opl_summa, 0)
                 * raw.f_ins_getkurs(o.opl_val, bc.pym_date)
        END AS oplsum
    FROM {{ source('raw', 'ins_oplata_oracle') }} o
    LEFT JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc
        ON o.bc_id = bc.ins_id
       AND bc.status = 2
    WHERE o.ins_type = 3
      AND EXISTS (
            SELECT 1
            FROM {{ source('raw', 'ins_polis_oracle') }} p
            WHERE p.tb_status IN (2, 9, 10)
              AND p.tb_anketa = o.anketa_id
      )
      AND bc.pym_date >= DATE '2021-01-01'

    UNION ALL

    /* =========================
       3) OSAGO OLD TABLE
       ========================= */
    SELECT
        bc.pym_date AS pay_date,
        raw.f_ins_pturiklass(3) AS klass,
        COALESCE(o.tb_summa, 0) AS oplsum
    FROM {{ source('raw', 'tb_anketa_oracle') }} t
    INNER JOIN {{ source('raw', 'tb_polis_oracle') }} p
        ON t.tb_id = p.tb_anketa
    INNER JOIN {{ source('raw', 'tb_oplata_oracle') }} o
        ON t.tb_id = o.tb_anketa
    INNER JOIN {{ source('raw', 'tb_avto_oracle') }} v
        ON t.tb_id = v.tb_anketa
    INNER JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc
        ON o.bc_id = bc.ins_id
       AND bc.status = 2
    WHERE o.tb_typepl = 1
      AND p.tb_status IN (2, 8)
      AND bc.pym_date >= DATE '2021-01-01'
),

company_yearly AS (
    SELECT
        EXTRACT(YEAR FROM d.pay_date)::INT AS report_year,
        CASE
            WHEN d.klass ~ '(^|,)\s*3\s*(,|$)' THEN 'Motor'
            WHEN (d.klass ~ '(^|,)\s*8\s*(,|$)' OR d.klass ~ '(^|,)\s*9\s*(,|$)') THEN 'Property'
        END AS priority_area,
        SUM(d.oplsum) AS co_premium
    FROM detailed_data d
    WHERE CASE
            WHEN d.klass ~ '(^|,)\s*3\s*(,|$)' THEN 'Motor'
            WHEN (d.klass ~ '(^|,)\s*8\s*(,|$)' OR d.klass ~ '(^|,)\s*9\s*(,|$)') THEN 'Property'
          END IS NOT NULL
    GROUP BY 1, 2
),

-- ────────────────────────────────────────────────────────────────────────────
-- 2. Market yearly premium from regulatory Excel data
-- ────────────────────────────────────────────────────────────────────────────
market_yearly AS (
    SELECT
        report_year,
        CASE
            WHEN insurance_type_name ILIKE '%1,3,10 klasslar%'
              OR insurance_type_name ILIKE '%3,14 klasslar%'
              OR insurance_type_name ILIKE '%3-klass – Yer usti transport vositalarini sug‘urta qilish%'
              OR insurance_type_name ILIKE '%transport vositalari egalarining fuqarolik javobgarligi%'
            THEN 'Motor'
            WHEN insurance_type_name ILIKE '%tashuvchilarning fuqarolik javobgarligi%'
              OR insurance_type_name ILIKE '%qurilish-montaj qaltisliklari%'
              OR insurance_type_name ILIKE '%8-klass – Mol-mulkni olovdan va tabiiy ofatlardan sug‘urta qilish%'
              OR insurance_type_name ILIKE '%9-klass – Mol-mulkni zarardan sug‘urta qilish%'
              OR insurance_type_name ILIKE '%8,9 klasslar%'
              OR insurance_type_name ILIKE '%7,8,9,13 klasslar%'
              OR insurance_type_name ILIKE '%8,9,13 klasslar%'
              OR insurance_type_name ILIKE '%8,9,16 klasslar%'
              OR insurance_type_name ILIKE '%8,9,13,16 klasslar%'
              OR insurance_type_name ILIKE '%1,2,8,9 klasslar%'
              OR insurance_type_name ILIKE '%1,8,9,13 klasslar%'
            THEN 'Property'
            ELSE 'Other'
        END AS priority_area,
        SUM(
            CASE
                WHEN report_year = EXTRACT(YEAR FROM CURRENT_DATE)::INT
                THEN COALESCE(total_premium, 0)
                ELSE 0
            END
        ) * 1000000 AS mkt_prem_curr_year
    FROM {{ source('raw', 'market_share_insurance_class_stats') }}
    GROUP BY 1, 2
    ORDER BY report_year DESC
)

-- ────────────────────────────────────────────────────────────────────────────
-- 3. One row per year + priority_area: company premium vs market premium
-- ────────────────────────────────────────────────────────────────────────────
SELECT
    (c.report_year::TEXT || '-01-01')::DATE AS report_year,
    c.priority_area,
    ROUND(c.co_premium::NUMERIC,               2) AS co_premium,
    ROUND(COALESCE(m.mkt_prem_curr_year, 0)::NUMERIC,  2) AS mkt_premium,
    CASE
        WHEN COALESCE(m.mkt_prem_curr_year, 0) > 0
        THEN ROUND((c.co_premium / m.mkt_prem_curr_year * 100)::NUMERIC, 4)
        ELSE 0
    END                                             AS market_share_pct,
    'Actual'                                        AS scenario

FROM company_yearly c
LEFT JOIN market_yearly m
    ON  m.report_year   = c.report_year
    AND m.priority_area = c.priority_area

ORDER BY c.report_year DESC, c.priority_area
