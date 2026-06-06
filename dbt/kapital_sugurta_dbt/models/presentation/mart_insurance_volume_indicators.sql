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

  METRICS (CY = Current Year, PY = Prior Year):
    Premiums, Claims, Liabilities, Terminated Contracts
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
-- SECTION 2: COMPANY DATA AGGREGATION
-- ================================================================
co_prem_raw AS (
    SELECT
        EXTRACT(YEAR FROM payment_date)::INT AS report_year,
        pturi_id,
        anketa_id,
        SUM(premium_amount)   AS premium_uzs,
        SUM(liability_amount) AS liability_uzs
    FROM {{ ref('curated_insurance_premium') }}
    GROUP BY 1, 2, 3
),

co_claims_raw AS (
    SELECT
        EXTRACT(YEAR FROM payment_date)::INT AS report_year,
        pturi_id,
        anketa_id,
        SUM(claim_amount) AS claims_uzs
    FROM {{ ref('curated_insurance_claims') }}
    GROUP BY 1, 2, 3
),

co_term_raw AS (
    SELECT
        EXTRACT(YEAR FROM termination_date)::INT AS report_year,
        pturi_id,
        anketa_id,
        SUM(terminated_amount) AS terminated_uzs
    FROM {{ ref('curated_terminated_contracts') }}
    GROUP BY 1, 2, 3
),

company_long AS (
    -- Combine all metrics into a long format by Year
    SELECT
        COALESCE(pr.report_year, cl.report_year, tr.report_year) AS report_year,
        COALESCE(pr.pturi_id,   cl.pturi_id,   tr.pturi_id)   AS pturi_id,
        COALESCE(pr.anketa_id,  cl.anketa_id,  tr.anketa_id)  AS anketa_id,
        SUM(COALESCE(pr.premium_uzs,   0)) AS co_prem,
        SUM(COALESCE(pr.liability_uzs, 0)) AS co_liab,
        SUM(COALESCE(cl.claims_uzs,    0)) AS co_claims,
        SUM(COALESCE(tr.terminated_uzs,0)) AS co_term
    FROM co_prem_raw pr
    FULL OUTER JOIN co_claims_raw cl
        ON  cl.report_year = pr.report_year
        AND cl.pturi_id    = pr.pturi_id
        AND cl.anketa_id   = pr.anketa_id
    FULL OUTER JOIN co_term_raw tr
        ON  tr.report_year = COALESCE(pr.report_year, cl.report_year)
        AND tr.pturi_id    = COALESCE(pr.pturi_id,   cl.pturi_id)
        AND tr.anketa_id   = COALESCE(pr.anketa_id,  cl.anketa_id)
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
        COALESCE(ch.partner_type,  'Internal') AS partner_type,
        ch.partner_name,
        SUM(cl.co_prem)   AS co_prem,
        SUM(cl.co_liab)   AS co_liab,
        SUM(cl.co_claims) AS co_claims,
        SUM(cl.co_term)   AS co_term
    FROM company_long cl
    INNER JOIN product_dim pd ON pd.pturi_id  = cl.pturi_id
    LEFT JOIN  channel_dim ch ON ch.anketa_id = cl.anketa_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

-- ================================================================
-- SECTION 3: PAIRING CY AND PY
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
        cy.co_prem   AS co_prem_cy,
        cy.co_liab   AS co_liab_cy,
        cy.co_claims AS co_claims_cy,
        cy.co_term   AS co_term_cy,

        -- Prior Year Metrics (from self-join)
        py.co_prem   AS co_prem_py,
        py.co_liab   AS co_liab_py,
        py.co_claims AS co_claims_py,
        py.co_term   AS co_term_py
    FROM company_agg cy
    LEFT JOIN company_agg py
        ON  py.report_year   = cy.report_year - 1
        AND py.insurance_type = cy.insurance_type
        AND py.category       = cy.category
        AND py.product_name   = cy.product_name
        AND py.sales_channel  = cy.sales_channel
        AND py.partner_type   = cy.partner_type
        AND COALESCE(py.partner_name, '') = COALESCE(cy.partner_name, '')
)

-- ================================================================
-- SECTION 4: FINAL OUTPUT
-- ================================================================
SELECT
    fp.report_year,
    (CASE WHEN fp.report_year < 100 THEN fp.report_year + 2000 ELSE fp.report_year END || '-01-01')::DATE AS report_month,
    fp.prior_year,
    fp.insurance_type,
    fp.category,
    fp.product_name,
    fp.sales_channel,
    fp.partner_type,
    fp.partner_name,

    -- PREMIUMS (CY & PY)
    ROUND(COALESCE(fp.co_prem_cy, 0)::NUMERIC, 2) AS co_premium_volume_cy,
    ROUND(COALESCE(fp.co_prem_py, 0)::NUMERIC, 2) AS co_premium_volume_py,

    -- CLAIMS (CY & PY)
    ROUND(COALESCE(fp.co_claims_cy, 0)::NUMERIC, 2) AS co_claims_volume_cy,
    ROUND(COALESCE(fp.co_claims_py, 0)::NUMERIC, 2) AS co_claims_volume_py,

    -- LIABILITIES (CY & PY)
    ROUND(COALESCE(fp.co_liab_cy, 0)::NUMERIC, 2) AS co_liabilities_cy,
    ROUND(COALESCE(fp.co_liab_py, 0)::NUMERIC, 2) AS co_liabilities_py,

    -- TERMINATED CONTRACTS (CY & PY)
    ROUND(COALESCE(fp.co_term_cy, 0)::NUMERIC, 2) AS co_terminated_cy,
    ROUND(COALESCE(fp.co_term_py, 0)::NUMERIC, 2) AS co_terminated_py

FROM final_pairs fp
ORDER BY fp.report_year DESC, fp.insurance_type, fp.category, fp.product_name
