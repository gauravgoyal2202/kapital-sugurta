{{ config(materialized='table') }}

/*
  Dashboard 7 — Bottom Section: "Показатели объема премий и выплат"
  mart_insurance_volume_indicators
  ----------------------------------------------------------------
  Grain: (report_year, insurance_type, category, product_name,
          sales_channel, partner_type, partner_name)

  Multi-Year Support:
  Produces (CY vs PY) comparison for every year found in the company data.
  The 'report_year' column should be used as a filter in the dashboard.

  FILTER COLUMNS:
    report_year     Primary year for comparison (CY)
    insurance_type  Compulsory / Voluntary
    category        Product vertical
    product_name    POLIS_NAME_RUS
    sales_channel   Website / Agent Network / Banks / Marketplace / API Partner / In-House
    partner_type    API / Internal
    partner_name    Specific partner entity name

  MARKET DATA:
    Regulatory source (market_share_insurance_class_stats) currently only has
    data for 2024 and 2025. Market columns will be NULL for other years.

  UNITS:
    Market values : mln UZS → / 1,000 → bn UZS
    Company values: raw UZS → / 1e9   → bn UZS
*/

-- ================================================================
-- SECTION 1: DIMENSIONS
-- ================================================================
WITH product_dim AS (
    SELECT * FROM {{ ref('curated_product_dimension') }}
),

channel_dim AS (
    SELECT * FROM {{ ref('curated_channel_partner_dimension') }}
),

-- ================================================================
-- SECTION 2: MARKET DATA (Unpivoted)
-- ================================================================
market_long AS (
    -- 2024 Market Data
    SELECT
        2024 AS report_year,
        CASE
            WHEN insurance_type_name ILIKE '%Majburiy sug%'           THEN 'Compulsory'
            WHEN insurance_type_name ILIKE '%majburiy sug%'           THEN 'Compulsory'
            WHEN insurance_type_name ILIKE '%Ixtiyoriy sug%'          THEN 'Voluntary'
            WHEN insurance_type_name ILIKE '%ixtiyoriy sug%'          THEN 'Voluntary'
            WHEN insurance_type_name ILIKE '%fuqarolik javobgarlik%'  THEN 'Compulsory'
            WHEN insurance_type_name ILIKE '%OSAGO%'                  THEN 'Compulsory'
            WHEN insurance_type_name ILIKE '%majburiy%'               THEN 'Compulsory'
            WHEN insurance_type_name ILIKE '%ixtiyoriy%'              THEN 'Voluntary'
            ELSE NULL
        END AS insurance_type,
        SUM(COALESCE(total_premium_2024,         0)) / 1000.0 AS mkt_prem_bn,
        SUM(COALESCE(claims_paid_2024,           0)) / 1000.0 AS mkt_claims_bn,
        SUM(COALESCE(insurance_liabilities_2024, 0)) / 1000.0 AS mkt_liab_bn
    FROM {{ source('raw', 'market_share_insurance_class_stats') }}
    WHERE insurance_type_name IS NOT NULL
    GROUP BY 1, 2

    UNION ALL

    -- 2025 Market Data
    SELECT
        2025 AS report_year,
        CASE
            WHEN insurance_type_name ILIKE '%Majburiy sug%'           THEN 'Compulsory'
            WHEN insurance_type_name ILIKE '%majburiy sug%'           THEN 'Compulsory'
            WHEN insurance_type_name ILIKE '%Ixtiyoriy sug%'          THEN 'Voluntary'
            WHEN insurance_type_name ILIKE '%ixtiyoriy sug%'          THEN 'Voluntary'
            WHEN insurance_type_name ILIKE '%fuqarolik javobgarlik%'  THEN 'Compulsory'
            WHEN insurance_type_name ILIKE '%OSAGO%'                  THEN 'Compulsory'
            WHEN insurance_type_name ILIKE '%majburiy%'               THEN 'Compulsory'
            WHEN insurance_type_name ILIKE '%ixtiyoriy%'              THEN 'Voluntary'
            ELSE NULL
        END AS insurance_type,
        SUM(COALESCE(total_premium_2025,         0)) / 1000.0 AS mkt_prem_bn,
        SUM(COALESCE(claims_paid_2025,           0)) / 1000.0 AS mkt_claims_bn,
        SUM(COALESCE(insurance_liabilities_2025, 0)) / 1000.0 AS mkt_liab_bn
    FROM {{ source('raw', 'market_share_insurance_class_stats') }}
    WHERE insurance_type_name IS NOT NULL
    GROUP BY 1, 2
),

-- ================================================================
-- SECTION 3: COMPANY DATA AGGREGATION
-- ================================================================
co_prem_raw AS (
    SELECT
        EXTRACT(YEAR FROM bc.pym_date)::INT AS report_year,
        p.pturi_id,
        o.anketa_id,
        SUM(CASE WHEN o.opl_val = 1 THEN COALESCE(o.oplata, 0) ELSE COALESCE(o.opl_summa, 0) * COALESCE(o.val_kurs, 1) END) AS premium_uzs,
        SUM(CASE WHEN o.opl_val = 1 THEN COALESCE(a.ins_otv, 0) ELSE COALESCE(a.ins_otv, 0) * COALESCE(o.val_kurs, 1) END) AS liability_uzs
    FROM {{ source('raw', 'ins_oplata_oracle') }} o
    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a ON a.ins_id = o.anketa_id
    LEFT JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc ON bc.ins_id = o.bc_id
    LEFT JOIN {{ source('raw', 'ins_polis_oracle') }} p ON p.tb_id = o.polis_id
    WHERE bc.pym_date IS NOT NULL
    GROUP BY 1, 2, 3
),

co_claims_raw AS (
    SELECT
        EXTRACT(YEAR FROM v.date_viplata)::INT AS report_year,
        p.pturi_id,
        s.anketa_id,
        SUM(CASE WHEN COALESCE(v.val_type, 1) = 1 THEN COALESCE(v.viplate, 0) ELSE COALESCE(v.viplate, 0) * COALESCE(v.val_kurs, 1) END) AS claims_uzs
    FROM {{ source('raw', 'ins_viplati_oracle') }} v
    LEFT JOIN {{ source('raw', 'ins_sobitie_oracle') }} s ON s.ins_id = v.sobitie_id
    LEFT JOIN {{ source('raw', 'ins_polis_oracle') }} p ON p.tb_id = s.polis_id
    WHERE v.date_viplata IS NOT NULL
    GROUP BY 1, 2, 3
),

co_term_raw AS (
    SELECT
        EXTRACT(YEAR FROM r.tb_dateras)::INT AS report_year,
        p.pturi_id,
        p.tb_anketa AS anketa_id,
        SUM(COALESCE(r.tb_summa, 0)) AS terminated_uzs
    FROM {{ source('raw', 'ins_rastorg_oracle') }} r
    LEFT JOIN {{ source('raw', 'ins_polis_oracle') }} p ON p.tb_id = r.tb_polis
    WHERE r.tb_dateras IS NOT NULL
    GROUP BY 1, 2, 3
),

company_long AS (
    -- Combine all metrics into a long format by Year
    SELECT
        COALESCE(pr.report_year, cl.report_year, tr.report_year) AS report_year,
        COALESCE(pr.pturi_id, cl.pturi_id, tr.pturi_id) AS pturi_id,
        COALESCE(pr.anketa_id, cl.anketa_id, tr.anketa_id) AS anketa_id,
        SUM(COALESCE(pr.premium_uzs, 0)) / 1e9 AS co_prem_bn,
        SUM(COALESCE(pr.liability_uzs, 0)) / 1e9 AS co_liab_bn,
        SUM(COALESCE(cl.claims_uzs, 0)) / 1e9 AS co_claims_bn,
        SUM(COALESCE(tr.terminated_uzs, 0)) / 1e9 AS co_term_bn
    FROM co_prem_raw pr
    FULL OUTER JOIN co_claims_raw cl ON cl.report_year = pr.report_year AND cl.pturi_id = pr.pturi_id AND cl.anketa_id = pr.anketa_id
    FULL OUTER JOIN co_term_raw tr ON tr.report_year = COALESCE(pr.report_year, cl.report_year) AND tr.pturi_id = COALESCE(pr.pturi_id, cl.pturi_id) AND tr.anketa_id = COALESCE(pr.anketa_id, cl.anketa_id)
    GROUP BY 1, 2, 3
),

company_agg AS (
    -- Enrich with dimensions and aggregate
    SELECT
        cl.report_year,
        pd.insurance_type,
        pd.category,
        pd.product_name,
        COALESCE(ch.sales_channel, 'In-House') AS sales_channel,
        COALESCE(ch.partner_type, 'Internal') AS partner_type,
        ch.partner_name,
        SUM(cl.co_prem_bn) AS co_prem_bn,
        SUM(cl.co_liab_bn) AS co_liab_bn,
        SUM(cl.co_claims_bn) AS co_claims_bn,
        SUM(cl.co_term_bn) AS co_term_bn
    FROM company_long cl
    INNER JOIN product_dim pd ON pd.pturi_id = cl.pturi_id
    LEFT JOIN channel_dim ch ON ch.anketa_id = cl.anketa_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

-- ================================================================
-- SECTION 4: PAIRING CY AND PY
-- ================================================================
final_pairs AS (
    SELECT
        cy.report_year,
        cy.report_year - 1 AS prior_year,
        cy.insurance_type,
        cy.category,
        cy.product_name,
        cy.sales_channel,
        cy.partner_type,
        cy.partner_name,
        
        -- Current Year Metrics
        cy.co_prem_bn AS co_prem_cy_bn,
        cy.co_liab_bn AS co_liab_cy_bn,
        cy.co_claims_bn AS co_claims_cy_bn,
        cy.co_term_bn AS co_term_cy_bn,
        
        -- Prior Year Metrics (from self-join)
        py.co_prem_bn AS co_prem_py_bn,
        py.co_liab_bn AS co_liab_py_bn,
        py.co_claims_bn AS co_claims_py_bn,
        py.co_term_bn AS co_term_py_bn
    FROM company_agg cy
    LEFT JOIN company_agg py 
        ON  py.report_year = cy.report_year - 1
        AND py.insurance_type = cy.insurance_type
        AND py.category = cy.category
        AND py.product_name = cy.product_name
        AND py.sales_channel = cy.sales_channel
        AND py.partner_type = cy.partner_type
        AND COALESCE(py.partner_name, '') = COALESCE(cy.partner_name, '')
)

-- ================================================================
-- SECTION 5: FINAL JOIN WITH MARKET DATA
-- ================================================================
SELECT
    fp.report_year,
    fp.prior_year,
    fp.insurance_type,
    fp.category,
    fp.product_name,
    fp.sales_channel,
    fp.partner_type,
    fp.partner_name,

    -- PREMIUMS
    ROUND(m_cy.mkt_prem_bn::NUMERIC, 2) AS market_premium_volume_cy_bn,
    ROUND(COALESCE(fp.co_prem_cy_bn, 0)::NUMERIC, 2) AS co_premium_volume_cy_bn,
    CASE WHEN COALESCE(m_cy.mkt_prem_bn, 0) = 0 THEN NULL ELSE ROUND((COALESCE(fp.co_prem_cy_bn, 0) / m_cy.mkt_prem_bn * 100)::NUMERIC, 2) END AS co_prem_market_share_cy_pct,
    
    ROUND(m_py.mkt_prem_bn::NUMERIC, 2) AS market_premium_volume_py_bn,
    ROUND(COALESCE(fp.co_prem_py_bn, 0)::NUMERIC, 2) AS co_premium_volume_py_bn,
    CASE WHEN COALESCE(m_py.mkt_prem_bn, 0) = 0 THEN NULL ELSE ROUND((COALESCE(fp.co_prem_py_bn, 0) / m_py.mkt_prem_bn * 100)::NUMERIC, 2) END AS co_prem_market_share_py_pct,

    CASE WHEN COALESCE(m_cy.mkt_prem_bn, 0) = 0 OR COALESCE(m_py.mkt_prem_bn, 0) = 0 THEN NULL 
         ELSE ROUND(((COALESCE(fp.co_prem_cy_bn, 0) / m_cy.mkt_prem_bn * 100) - (COALESCE(fp.co_prem_py_bn, 0) / m_py.mkt_prem_bn * 100))::NUMERIC, 2) END AS co_prem_share_change_pp,
    CASE WHEN COALESCE(fp.co_prem_py_bn, 0) = 0 THEN NULL ELSE ROUND(((COALESCE(fp.co_prem_cy_bn, 0) - COALESCE(fp.co_prem_py_bn, 0)) / fp.co_prem_py_bn * 100)::NUMERIC, 2) END AS co_prem_volume_change_pct,

    -- CLAIMS
    ROUND(m_cy.mkt_claims_bn::NUMERIC, 2) AS market_claims_volume_cy_bn,
    ROUND(COALESCE(fp.co_claims_cy_bn, 0)::NUMERIC, 2) AS co_claims_volume_cy_bn,
    CASE WHEN COALESCE(m_cy.mkt_claims_bn, 0) = 0 THEN NULL ELSE ROUND((COALESCE(fp.co_claims_cy_bn, 0) / m_cy.mkt_claims_bn * 100)::NUMERIC, 2) END AS co_claims_market_share_cy_pct,

    ROUND(m_py.mkt_claims_bn::NUMERIC, 2) AS market_claims_volume_py_bn,
    ROUND(COALESCE(fp.co_claims_py_bn, 0)::NUMERIC, 2) AS co_claims_volume_py_bn,
    CASE WHEN COALESCE(m_py.mkt_claims_bn, 0) = 0 THEN NULL ELSE ROUND((COALESCE(fp.co_claims_py_bn, 0) / m_py.mkt_claims_bn * 100)::NUMERIC, 2) END AS co_claims_market_share_py_pct,

    CASE WHEN COALESCE(m_cy.mkt_claims_bn, 0) = 0 OR COALESCE(m_py.mkt_claims_bn, 0) = 0 THEN NULL 
         ELSE ROUND(((COALESCE(fp.co_claims_cy_bn, 0) / m_cy.mkt_claims_bn * 100) - (COALESCE(fp.co_claims_py_bn, 0) / m_py.mkt_claims_bn * 100))::NUMERIC, 2) END AS co_claims_share_change_pp,
    CASE WHEN COALESCE(fp.co_claims_py_bn, 0) = 0 THEN NULL ELSE ROUND(((COALESCE(fp.co_claims_cy_bn, 0) - COALESCE(fp.co_claims_py_bn, 0)) / fp.co_claims_py_bn * 100)::NUMERIC, 2) END AS co_claims_volume_change_pct,

    -- LOSS RATIO
    CASE WHEN COALESCE(m_cy.mkt_prem_bn, 0) = 0 THEN NULL ELSE ROUND((m_cy.mkt_claims_bn / m_cy.mkt_prem_bn * 100)::NUMERIC, 2) END AS market_loss_ratio_cy_pct,
    CASE WHEN COALESCE(fp.co_prem_cy_bn, 0) = 0 THEN NULL ELSE ROUND((COALESCE(fp.co_claims_cy_bn, 0) / fp.co_prem_cy_bn * 100)::NUMERIC, 2) END AS co_loss_ratio_cy_pct,
    
    CASE WHEN COALESCE(m_py.mkt_prem_bn, 0) = 0 THEN NULL ELSE ROUND((m_py.mkt_claims_bn / m_py.mkt_prem_bn * 100)::NUMERIC, 2) END AS market_loss_ratio_py_pct,
    CASE WHEN COALESCE(fp.co_prem_py_bn, 0) = 0 THEN NULL ELSE ROUND((COALESCE(fp.co_claims_py_bn, 0) / fp.co_prem_py_bn * 100)::NUMERIC, 2) END AS co_loss_ratio_py_pct,

    -- LIABILITIES
    ROUND(m_cy.mkt_liab_bn::NUMERIC, 2) AS market_liabilities_cy_bn,
    ROUND(COALESCE(fp.co_liab_cy_bn, 0)::NUMERIC, 2) AS co_liabilities_cy_bn,
    CASE WHEN COALESCE(m_cy.mkt_liab_bn, 0) = 0 THEN NULL ELSE ROUND((COALESCE(fp.co_liab_cy_bn, 0) / m_cy.mkt_liab_bn * 100)::NUMERIC, 2) END AS co_liab_market_share_cy_pct,

    ROUND(m_py.mkt_liab_bn::NUMERIC, 2) AS market_liabilities_py_bn,
    ROUND(COALESCE(fp.co_liab_py_bn, 0)::NUMERIC, 2) AS co_liabilities_py_bn,
    CASE WHEN COALESCE(m_py.mkt_liab_bn, 0) = 0 THEN NULL ELSE ROUND((COALESCE(fp.co_liab_py_bn, 0) / m_py.mkt_liab_bn * 100)::NUMERIC, 2) END AS co_liab_market_share_py_pct,

    CASE WHEN COALESCE(m_cy.mkt_liab_bn, 0) = 0 OR COALESCE(m_py.mkt_liab_bn, 0) = 0 THEN NULL 
         ELSE ROUND(((COALESCE(fp.co_liab_cy_bn, 0) / m_cy.mkt_liab_bn * 100) - (COALESCE(fp.co_liab_py_bn, 0) / m_py.mkt_liab_bn * 100))::NUMERIC, 2) END AS co_liab_share_change_pp,
    CASE WHEN COALESCE(fp.co_liab_py_bn, 0) = 0 THEN NULL ELSE ROUND(((COALESCE(fp.co_liab_cy_bn, 0) - COALESCE(fp.co_liab_py_bn, 0)) / fp.co_liab_py_bn * 100)::NUMERIC, 2) END AS co_liab_volume_change_pct,

    -- TERMINATED
    ROUND(COALESCE(fp.co_term_cy_bn, 0)::NUMERIC, 2) AS co_terminated_cy_bn,
    ROUND(COALESCE(fp.co_term_py_bn, 0)::NUMERIC, 2) AS co_terminated_py_bn,
    CASE WHEN COALESCE(fp.co_term_py_bn, 0) = 0 THEN NULL ELSE ROUND(((COALESCE(fp.co_term_cy_bn, 0) - COALESCE(fp.co_term_py_bn, 0)) / fp.co_term_py_bn * 100)::NUMERIC, 2) END AS co_terminated_volume_change_pct

FROM final_pairs fp
-- Join market data for Current Year (CY)
LEFT JOIN market_long m_cy ON m_cy.report_year = fp.report_year AND m_cy.insurance_type = fp.insurance_type
-- Join market data for Prior Year (PY)
LEFT JOIN market_long m_py ON m_py.report_year = fp.prior_year AND m_py.insurance_type = fp.insurance_type

ORDER BY fp.report_year DESC, fp.insurance_type, fp.category, fp.product_name
