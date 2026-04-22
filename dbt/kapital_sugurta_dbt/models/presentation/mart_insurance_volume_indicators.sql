{{ config(materialized='table') }}

/*
  Dashboard 7 — Bottom Section: "Показатели объема премий и выплат"
  mart_insurance_volume_indicators
  ----------------------------------------------------------------
  Produces one row per (insurance_type, category, product_name) with
  prior-year (PY) and current-year (CY) side-by-side for all 5 tabs:
    Premiums | Claims | Loss Ratio | Insurance Liabilities | Terminated Contracts

  DATA SOURCES:
    Company data  — Oracle operational tables (ins_oplata, ins_sobitie,
                    ins_viplati, ins_rastorg) → ins_polis → ins_pturi → vertical
    Market data   — raw.market_share_insurance_class_stats (regulatory report)
                    Joined at Compulsory/Voluntary level (column: insurance_type).
                    Per-category market data = NULL pending Commercial Development
                    crosswalk (client mapping sheet row 36).

  FILTERS SUPPORTED IN THIS MART:
    insurance_type   Compulsory / Voluntary   (from KOD_NUM classification)
    category         Product vertical name    (from ins_vertical_oracle)
    product_name     Specific product         (from ins_pturi.polis_name_rus)

  FILTERS NOT YET SUPPORTED (pending Commercial Development mapping):
    Sales Channel, Partner Type, Partner

  YEAR LOGIC:
    prior_year   = current calendar year - 1
    current_year = current calendar year
    Market share columns in source are named _2024 / _2025 — mapped
    to PY / CY via the crosswalk CTE below. Update annually when new
    regulatory market data is loaded.

  UNIT NOTE:
    Market source values appear to be in mln UZS (millions of Uzbek sums).
    Divided by 1,000 to produce bn UZS. Verify against known totals
    (total market 2024 ≈ 2,214 bn UZS after conversion).
    Company values are in raw UZS from Oracle; divided by 1e9 for bn UZS.
*/

-- ================================================================
-- SECTION 1: YEAR PARAMETERS
-- ================================================================
WITH year_params AS (
    SELECT
        EXTRACT(YEAR FROM CURRENT_DATE)::INT - 1   AS prior_year,
        EXTRACT(YEAR FROM CURRENT_DATE)::INT        AS current_year,
        -- Hardcoded market data years — update when new regulatory file loaded
        2024                                        AS mkt_prior_year,
        2025                                        AS mkt_current_year
),

-- ================================================================
-- SECTION 2: PRODUCT DIMENSION
-- ================================================================
product_dim AS (
    SELECT * FROM {{ ref('curated_product_dimension') }}
),

-- ================================================================
-- SECTION 3: MARKET DATA FROM REGULATORY REPORT
-- Unpivot wide table → join at insurance_type level.
-- Category-level crosswalk = NULL until Commercial Dev mapping received.
-- ================================================================
market_base AS (
    SELECT
        insurance_type_name,
        CASE
            WHEN insurance_type_name ILIKE '%Majburiy sug%'    THEN 'Compulsory'
            WHEN insurance_type_name ILIKE '%majburiy sug%'    THEN 'Compulsory'
            WHEN insurance_type_name ILIKE '%Ixtiyoriy sug%'   THEN 'Voluntary'
            WHEN insurance_type_name ILIKE '%ixtiyoriy sug%'   THEN 'Voluntary'
            -- Specific compulsory sub-classes (Uzbek regulatory names)
            WHEN insurance_type_name ILIKE '%fuqarolik javobgarlik%' THEN 'Compulsory'
            WHEN insurance_type_name ILIKE '%OSAGO%'                 THEN 'Compulsory'
            WHEN insurance_type_name ILIKE '%osago%'                 THEN 'Compulsory'
            WHEN insurance_type_name ILIKE '%majburiy%'              THEN 'Compulsory'
            WHEN insurance_type_name ILIKE '%ixtiyoriy%'             THEN 'Voluntary'
            ELSE NULL  -- total / note rows → excluded
        END                                                          AS insurance_type,

        -- Category crosswalk: NULL pending Commercial Development mapping.
        -- Replace NULLs here once client provides the mapping file (row 36).
        CAST(NULL AS TEXT)                                           AS category,

        -- Market volumes: source in mln UZS → divide by 1,000 for bn UZS
        COALESCE(total_premium_2024,         0) / 1000.0            AS mkt_prem_py_bn,
        COALESCE(total_premium_2025,         0) / 1000.0            AS mkt_prem_cy_bn,
        COALESCE(claims_paid_2024,           0) / 1000.0            AS mkt_claims_py_bn,
        COALESCE(claims_paid_2025,           0) / 1000.0            AS mkt_claims_cy_bn,
        COALESCE(insurance_liabilities_2024, 0) / 1000.0            AS mkt_liab_py_bn,
        COALESCE(insurance_liabilities_2025, 0) / 1000.0            AS mkt_liab_cy_bn

    FROM {{ source('raw', 'market_share_insurance_class_stats') }}
    WHERE insurance_type_name IS NOT NULL
),

market_agg AS (
    SELECT
        insurance_type,
        category,
        SUM(mkt_prem_py_bn)   AS mkt_prem_py_bn,
        SUM(mkt_prem_cy_bn)   AS mkt_prem_cy_bn,
        SUM(mkt_claims_py_bn) AS mkt_claims_py_bn,
        SUM(mkt_claims_cy_bn) AS mkt_claims_cy_bn,
        SUM(mkt_liab_py_bn)   AS mkt_liab_py_bn,
        SUM(mkt_liab_cy_bn)   AS mkt_liab_cy_bn
    FROM market_base
    WHERE insurance_type IS NOT NULL
    GROUP BY insurance_type, category
),

-- ================================================================
-- SECTION 4: COMPANY PREMIUMS
-- Join path: ins_oplata_oracle → ins_polis_oracle (polis_id) → product_dim
-- ================================================================
co_prem_raw AS (
    -- Type 1: Standard insurance products
    SELECT
        EXTRACT(YEAR FROM bc.pym_date)::INT  AS pay_year,
        p.pturi_id,
        SUM(
            CASE WHEN o.opl_val = 1
                THEN COALESCE(o.oplata, 0)
                ELSE COALESCE(o.opl_summa, 0) * COALESCE(o.val_kurs, 1)
            END
        )                                    AS premium_uzs,
        SUM(
            CASE WHEN o.opl_val = 1
                THEN COALESCE(a.ins_otv, 0)
                ELSE COALESCE(a.ins_otv, 0) * COALESCE(o.val_kurs, 1)
            END
        )                                    AS liability_uzs
    FROM {{ source('raw', 'ins_oplata_oracle') }}      o
    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }}     a  ON a.ins_id  = o.anketa_id
    LEFT JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc ON bc.ins_id = o.bc_id
    LEFT JOIN {{ source('raw', 'ins_polis_oracle') }}      p  ON p.tb_id   = o.polis_id
    WHERE o.ins_type <> 3
      AND bc.pym_date IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM {{ source('raw', 'ins_polis_oracle') }} px
          WHERE px.tb_status IN (2, 9, 10) AND px.tb_anketa = o.anketa_id
      )
    GROUP BY 1, 2

    UNION ALL

    -- Type 2: OSAGO / bank-linked products (ins_type = 3)
    SELECT
        EXTRACT(YEAR FROM bc.pym_date)::INT  AS pay_year,
        p.pturi_id,
        SUM(
            CASE WHEN o.opl_val = 1
                THEN COALESCE(o.oplata, 0)
                ELSE COALESCE(o.oplata, 0) * COALESCE(o.val_kurs, 1)
            END
        )                                    AS premium_uzs,
        SUM(COALESCE(a.ins_otv, 0))          AS liability_uzs
    FROM {{ source('raw', 'ins_oplata_oracle') }}      o
    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }}     a  ON a.ins_id  = o.anketa_id
    LEFT JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc ON bc.ins_id = o.bc_id
                                                            AND bc.status = 2
    LEFT JOIN {{ source('raw', 'ins_polis_oracle') }}      p  ON p.tb_id   = o.polis_id
    WHERE o.ins_type = 3
      AND bc.pym_date IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM {{ source('raw', 'ins_polis_oracle') }} px
          WHERE px.tb_status IN (2, 9, 10) AND px.tb_anketa = o.anketa_id
      )
    GROUP BY 1, 2
),

co_premiums AS (
    SELECT
        r.pay_year,
        pd.insurance_type,
        pd.category,
        pd.product_name,
        SUM(r.premium_uzs)  / 1e9 AS co_prem_bn,
        SUM(r.liability_uzs)/ 1e9 AS co_liab_bn
    FROM co_prem_raw r
    INNER JOIN product_dim pd ON pd.pturi_id = r.pturi_id
    WHERE r.pturi_id IS NOT NULL
    GROUP BY r.pay_year, pd.insurance_type, pd.category, pd.product_name
),

-- ================================================================
-- SECTION 5: COMPANY CLAIMS
-- Join path: ins_viplati_oracle → ins_sobitie_oracle → ins_polis_oracle → product_dim
-- ================================================================
co_claims_raw AS (
    SELECT
        EXTRACT(YEAR FROM v.date_viplata)::INT  AS pay_year,
        p.pturi_id,
        SUM(
            CASE WHEN COALESCE(v.val_type, 1) = 1
                THEN COALESCE(v.viplate, 0)
                ELSE COALESCE(v.viplate, 0) * COALESCE(v.val_kurs, 1)
            END
        )                                       AS claims_uzs
    FROM {{ source('raw', 'ins_viplati_oracle') }}  v
    LEFT JOIN {{ source('raw', 'ins_sobitie_oracle') }} s ON s.ins_id = v.sobitie_id
    LEFT JOIN {{ source('raw', 'ins_polis_oracle') }}   p ON p.tb_id  = s.polis_id
    WHERE v.date_viplata IS NOT NULL
    GROUP BY 1, 2
),

co_claims AS (
    SELECT
        r.pay_year,
        pd.insurance_type,
        pd.category,
        pd.product_name,
        SUM(r.claims_uzs) / 1e9 AS co_claims_bn
    FROM co_claims_raw r
    INNER JOIN product_dim pd ON pd.pturi_id = r.pturi_id
    WHERE r.pturi_id IS NOT NULL
    GROUP BY r.pay_year, pd.insurance_type, pd.category, pd.product_name
),

-- ================================================================
-- SECTION 6: COMPANY TERMINATED CONTRACTS
-- Join path: ins_rastorg_oracle → ins_polis_oracle (tb_polis) → product_dim
-- ================================================================
co_terminated_raw AS (
    SELECT
        EXTRACT(YEAR FROM r.tb_dateras)::INT AS pay_year,
        p.pturi_id,
        SUM(COALESCE(r.tb_summa, 0))         AS terminated_uzs
    FROM {{ source('raw', 'ins_rastorg_oracle') }} r
    LEFT JOIN {{ source('raw', 'ins_polis_oracle') }} p ON p.tb_id = r.tb_polis
    WHERE r.tb_dateras IS NOT NULL
    GROUP BY 1, 2
),

co_terminated AS (
    SELECT
        r.pay_year,
        pd.insurance_type,
        pd.category,
        pd.product_name,
        SUM(r.terminated_uzs) / 1e9 AS co_term_bn
    FROM co_terminated_raw r
    INNER JOIN product_dim pd ON pd.pturi_id = r.pturi_id
    WHERE r.pturi_id IS NOT NULL
    GROUP BY r.pay_year, pd.insurance_type, pd.category, pd.product_name
),

-- ================================================================
-- SECTION 7: MERGE COMPANY DATA — FULL OUTER JOIN ALL METRICS
-- ================================================================
company_all AS (
    SELECT
        COALESCE(pr.pay_year,       cl.pay_year,       tr.pay_year)       AS report_year,
        COALESCE(pr.insurance_type, cl.insurance_type, tr.insurance_type) AS insurance_type,
        COALESCE(pr.category,       cl.category,       tr.category)       AS category,
        COALESCE(pr.product_name,   cl.product_name,   tr.product_name)   AS product_name,
        COALESCE(pr.co_prem_bn,  0) AS co_prem_bn,
        COALESCE(pr.co_liab_bn,  0) AS co_liab_bn,
        COALESCE(cl.co_claims_bn,0) AS co_claims_bn,
        COALESCE(tr.co_term_bn,  0) AS co_term_bn
    FROM co_premiums pr
    FULL OUTER JOIN co_claims cl
        ON  cl.pay_year      = pr.pay_year
        AND cl.insurance_type= pr.insurance_type
        AND cl.category      = pr.category
        AND cl.product_name  = pr.product_name
    FULL OUTER JOIN co_terminated tr
        ON  tr.pay_year      = COALESCE(pr.pay_year,      cl.pay_year)
        AND tr.insurance_type= COALESCE(pr.insurance_type,cl.insurance_type)
        AND tr.category      = COALESCE(pr.category,      cl.category)
        AND tr.product_name  = COALESCE(pr.product_name,  cl.product_name)
),

-- ================================================================
-- SECTION 8: SPLIT INTO PRIOR YEAR / CURRENT YEAR
-- ================================================================
py AS (
    SELECT ca.* FROM company_all ca
    CROSS JOIN year_params yp
    WHERE ca.report_year = yp.prior_year
),

cy AS (
    SELECT ca.* FROM company_all ca
    CROSS JOIN year_params yp
    WHERE ca.report_year = yp.current_year
)

-- ================================================================
-- SECTION 9: FINAL SELECT — Company + Market + YoY Changes
-- ================================================================
SELECT
    COALESCE(cy.insurance_type, py.insurance_type)   AS insurance_type,
    COALESCE(cy.category,       py.category)         AS category,
    COALESCE(cy.product_name,   py.product_name)     AS product_name,
    yp.prior_year,
    yp.current_year,

    -- ── PREMIUMS ────────────────────────────────────────────────────────
    ROUND(m.mkt_prem_py_bn::NUMERIC,  2)             AS market_premium_volume_py_bn,
    ROUND(COALESCE(py.co_prem_bn, 0)::NUMERIC, 2)   AS co_premium_volume_py_bn,
    CASE WHEN COALESCE(m.mkt_prem_py_bn, 0) = 0 THEN NULL
         ELSE ROUND((COALESCE(py.co_prem_bn,0)/m.mkt_prem_py_bn*100)::NUMERIC, 2)
    END                                              AS co_prem_market_share_py_pct,

    ROUND(m.mkt_prem_cy_bn::NUMERIC,  2)             AS market_premium_volume_cy_bn,
    ROUND(COALESCE(cy.co_prem_bn, 0)::NUMERIC, 2)   AS co_premium_volume_cy_bn,
    CASE WHEN COALESCE(m.mkt_prem_cy_bn, 0) = 0 THEN NULL
         ELSE ROUND((COALESCE(cy.co_prem_bn,0)/m.mkt_prem_cy_bn*100)::NUMERIC, 2)
    END                                              AS co_prem_market_share_cy_pct,

    -- YoY Premiums
    CASE WHEN COALESCE(m.mkt_prem_py_bn,0)=0 OR COALESCE(m.mkt_prem_cy_bn,0)=0 THEN NULL
         ELSE ROUND((
               COALESCE(cy.co_prem_bn,0)/m.mkt_prem_cy_bn*100
             - COALESCE(py.co_prem_bn,0)/m.mkt_prem_py_bn*100)::NUMERIC, 2)
    END                                              AS co_prem_share_change_pp,
    CASE WHEN COALESCE(py.co_prem_bn, 0) = 0 THEN NULL
         ELSE ROUND(((COALESCE(cy.co_prem_bn,0)-COALESCE(py.co_prem_bn,0))
                    / COALESCE(py.co_prem_bn,1)*100)::NUMERIC, 2)
    END                                              AS co_prem_volume_change_pct,

    -- ── CLAIMS ──────────────────────────────────────────────────────────
    ROUND(m.mkt_claims_py_bn::NUMERIC, 2)            AS market_claims_volume_py_bn,
    ROUND(COALESCE(py.co_claims_bn,0)::NUMERIC, 2)  AS co_claims_volume_py_bn,
    CASE WHEN COALESCE(m.mkt_claims_py_bn,0)=0 THEN NULL
         ELSE ROUND((COALESCE(py.co_claims_bn,0)/m.mkt_claims_py_bn*100)::NUMERIC, 2)
    END                                              AS co_claims_market_share_py_pct,

    ROUND(m.mkt_claims_cy_bn::NUMERIC, 2)            AS market_claims_volume_cy_bn,
    ROUND(COALESCE(cy.co_claims_bn,0)::NUMERIC, 2)  AS co_claims_volume_cy_bn,
    CASE WHEN COALESCE(m.mkt_claims_cy_bn,0)=0 THEN NULL
         ELSE ROUND((COALESCE(cy.co_claims_bn,0)/m.mkt_claims_cy_bn*100)::NUMERIC, 2)
    END                                              AS co_claims_market_share_cy_pct,

    CASE WHEN COALESCE(m.mkt_claims_py_bn,0)=0 OR COALESCE(m.mkt_claims_cy_bn,0)=0 THEN NULL
         ELSE ROUND((
               COALESCE(cy.co_claims_bn,0)/m.mkt_claims_cy_bn*100
             - COALESCE(py.co_claims_bn,0)/m.mkt_claims_py_bn*100)::NUMERIC, 2)
    END                                              AS co_claims_share_change_pp,
    CASE WHEN COALESCE(py.co_claims_bn,0)=0 THEN NULL
         ELSE ROUND(((COALESCE(cy.co_claims_bn,0)-COALESCE(py.co_claims_bn,0))
                    / COALESCE(py.co_claims_bn,1)*100)::NUMERIC, 2)
    END                                              AS co_claims_volume_change_pct,

    -- ── LOSS RATIO ───────────────────────────────────────────────────────
    CASE WHEN COALESCE(m.mkt_prem_py_bn,0)=0 THEN NULL
         ELSE ROUND((m.mkt_claims_py_bn/m.mkt_prem_py_bn*100)::NUMERIC, 2)
    END                                              AS market_loss_ratio_py_pct,
    CASE WHEN COALESCE(py.co_prem_bn,0)=0 THEN NULL
         ELSE ROUND((COALESCE(py.co_claims_bn,0)/COALESCE(py.co_prem_bn,1)*100)::NUMERIC, 2)
    END                                              AS co_loss_ratio_py_pct,

    CASE WHEN COALESCE(m.mkt_prem_cy_bn,0)=0 THEN NULL
         ELSE ROUND((m.mkt_claims_cy_bn/m.mkt_prem_cy_bn*100)::NUMERIC, 2)
    END                                              AS market_loss_ratio_cy_pct,
    CASE WHEN COALESCE(cy.co_prem_bn,0)=0 THEN NULL
         ELSE ROUND((COALESCE(cy.co_claims_bn,0)/COALESCE(cy.co_prem_bn,1)*100)::NUMERIC, 2)
    END                                              AS co_loss_ratio_cy_pct,

    -- ── INSURANCE LIABILITIES ────────────────────────────────────────────
    ROUND(m.mkt_liab_py_bn::NUMERIC, 2)              AS market_liabilities_py_bn,
    ROUND(COALESCE(py.co_liab_bn,0)::NUMERIC, 2)    AS co_liabilities_py_bn,
    CASE WHEN COALESCE(m.mkt_liab_py_bn,0)=0 THEN NULL
         ELSE ROUND((COALESCE(py.co_liab_bn,0)/m.mkt_liab_py_bn*100)::NUMERIC, 2)
    END                                              AS co_liab_market_share_py_pct,

    ROUND(m.mkt_liab_cy_bn::NUMERIC, 2)              AS market_liabilities_cy_bn,
    ROUND(COALESCE(cy.co_liab_bn,0)::NUMERIC, 2)    AS co_liabilities_cy_bn,
    CASE WHEN COALESCE(m.mkt_liab_cy_bn,0)=0 THEN NULL
         ELSE ROUND((COALESCE(cy.co_liab_bn,0)/m.mkt_liab_cy_bn*100)::NUMERIC, 2)
    END                                              AS co_liab_market_share_cy_pct,

    CASE WHEN COALESCE(m.mkt_liab_py_bn,0)=0 OR COALESCE(m.mkt_liab_cy_bn,0)=0 THEN NULL
         ELSE ROUND((
               COALESCE(cy.co_liab_bn,0)/m.mkt_liab_cy_bn*100
             - COALESCE(py.co_liab_bn,0)/m.mkt_liab_py_bn*100)::NUMERIC, 2)
    END                                              AS co_liab_share_change_pp,
    CASE WHEN COALESCE(py.co_liab_bn,0)=0 THEN NULL
         ELSE ROUND(((COALESCE(cy.co_liab_bn,0)-COALESCE(py.co_liab_bn,0))
                    / COALESCE(py.co_liab_bn,1)*100)::NUMERIC, 2)
    END                                              AS co_liab_volume_change_pct,

    -- ── TERMINATED CONTRACTS ─────────────────────────────────────────────
    -- Note: Market data does not include terminated contract volumes.
    ROUND(COALESCE(py.co_term_bn,0)::NUMERIC, 2)    AS co_terminated_py_bn,
    ROUND(COALESCE(cy.co_term_bn,0)::NUMERIC, 2)    AS co_terminated_cy_bn,
    CASE WHEN COALESCE(py.co_term_bn,0)=0 THEN NULL
         ELSE ROUND(((COALESCE(cy.co_term_bn,0)-COALESCE(py.co_term_bn,0))
                    / COALESCE(py.co_term_bn,1)*100)::NUMERIC, 2)
    END                                              AS co_terminated_volume_change_pct

FROM cy
CROSS JOIN year_params yp
FULL OUTER JOIN py
    ON  py.insurance_type = cy.insurance_type
    AND py.category       = cy.category
    AND py.product_name   = cy.product_name
-- Market joined at insurance_type level (category IS NULL = top-level aggregation)
LEFT JOIN market_agg m
    ON  m.insurance_type  = COALESCE(cy.insurance_type, py.insurance_type)
    AND m.category        IS NULL

ORDER BY
    CASE COALESCE(cy.insurance_type, py.insurance_type)
        WHEN 'Compulsory' THEN 1 ELSE 2
    END,
    COALESCE(cy.category,     py.category)     NULLS LAST,
    COALESCE(cy.product_name, py.product_name) NULLS LAST
