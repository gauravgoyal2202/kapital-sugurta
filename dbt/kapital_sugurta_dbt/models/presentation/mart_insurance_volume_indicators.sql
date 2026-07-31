{{ config(materialized='table') }}

/*
  Dashboard 7 -- Bottom Section: "Insurance Premium & Claims Volume Indicators"
  mart_insurance_volume_indicators
  ----------------------------------------------------------------
  Grain: (report_year, report_month, insurance_type, category, product_name,
          sales_channel, partner_type, partner_name)

  Month + Year Support:
  Produces CY vs PY comparison at MONTHLY level for every month found in
  the company data. 'report_year' and 'report_month' should be used as
  filters in the dashboard.

  CLAIMS SOURCE NOTE:
  Claims are pulled from curated_claims_portfolio (Excel + Oracle 2026 hybrid)
  to match the KPI card in mart_insurance_operations_monthly exactly.
  They are joined at (report_month, insurance_type) grain since
  curated_claims_portfolio has no pturi_id/anketa_id.

  FILTER COLUMNS:
    report_year     Year extracted from the month (for year-level filters)
    report_month    Truncated to month (YYYY-MM-01) -- primary time filter
    insurance_type  Compulsory / Voluntary
    category        Product vertical
    product_name    POLIS_NAME_RUS
    sales_channel   Website / Agent Network / Banks / Marketplace / API Partner / In-House
    partner_type    API / Internal
    partner_name    Specific partner entity name

  METRICS (CY = Current Month, PY = Same Month Prior Year):
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
-- SECTION 2: COMPANY DATA AGGREGATION  (monthly grain)
-- ================================================================

-- Premiums & Liabilities: joined at pturi_id + anketa_id level
co_prem_raw AS (
    SELECT
        DATE_TRUNC('month', payment_date)::DATE  AS report_month,
        EXTRACT(YEAR FROM payment_date)::INT      AS report_year,
        pturi_id,
        anketa_id,
        SUM(premium_amount)   AS premium_uzs,
        SUM(liability_amount) AS liability_uzs
    FROM {{ ref('curated_insurance_premium') }}
    GROUP BY 1, 2, 3, 4
),

co_term_raw AS (
    SELECT
        DATE_TRUNC('month', termination_date)::DATE  AS report_month,
        EXTRACT(YEAR FROM termination_date)::INT      AS report_year,
        pturi_id,
        anketa_id,
        SUM(terminated_amount) AS terminated_uzs
    FROM {{ ref('curated_terminated_contracts') }}
    GROUP BY 1, 2, 3, 4
),

-- Claims: use curated_claims_portfolio to match the KPI card exactly.
-- Joined at report_month grain only. The insurance_type in curated_claims_portfolio
-- is Cyrillic Russian text and cannot be matched to the English values in product_dim.
-- Claims are distributed proportionally by premium share across all products in the month.
co_claims_by_month AS (
    SELECT
        DATE_TRUNC('month', payout_date)::DATE AS report_month,
        SUM(payout_total)                      AS claims_uzs
    FROM {{ ref('curated_claims_portfolio') }}
    WHERE payout_date IS NOT NULL
    GROUP BY 1
),

company_long AS (
    -- Combine premiums + terminated contracts at pturi_id level
    SELECT
        COALESCE(pr.report_month, tr.report_month) AS report_month,
        COALESCE(pr.report_year,  tr.report_year)  AS report_year,
        COALESCE(pr.pturi_id,     tr.pturi_id)     AS pturi_id,
        COALESCE(pr.anketa_id,    tr.anketa_id)    AS anketa_id,
        SUM(COALESCE(pr.premium_uzs,   0)) AS co_prem,
        SUM(COALESCE(pr.liability_uzs, 0)) AS co_liab,
        SUM(COALESCE(tr.terminated_uzs,0)) AS co_term
    FROM co_prem_raw pr
    FULL OUTER JOIN co_term_raw tr
        ON  tr.report_month = pr.report_month
        AND tr.pturi_id     = pr.pturi_id
        AND tr.anketa_id    = pr.anketa_id
    GROUP BY 1, 2, 3, 4
),

company_agg AS (
    -- Enrich with dimensions and aggregate to (month, insurance_type, category, product, channel) grain.
    -- LEFT JOIN ensures transactions with unmapped pturi_id are NOT dropped
    -- (INNER JOIN was silently excluding them, causing totals to be lower than the KPI card).
    SELECT
        cl.report_month,
        cl.report_year,
        COALESCE(pd.insurance_type, 'Unknown') AS insurance_type,
        COALESCE(pd.insurance_type_ru, 'Unknown') AS insurance_type_ru,
        COALESCE(pd.insurance_type_uz_cyrl, 'Unknown') AS insurance_type_uz_cyrl,
        COALESCE(pd.insurance_type_uz_latn, 'Unknown') AS insurance_type_uz_latn,
        COALESCE(pd.category, 'Unclassified') AS category,
        COALESCE(pd.category_ru, 'Unclassified') AS category_ru,
        COALESCE(pd.category_uz, 'Unclassified') AS category_uz,
        COALESCE(pd.category_uz_latn, 'Unclassified') AS category_uz_latn,
        COALESCE(pd.product_name, 'Unknown Product') AS product_name,
        COALESCE(pd.product_name_ru, 'Unknown Product') AS product_name_ru,
        COALESCE(pd.product_name_uz, 'Unknown Product') AS product_name_uz,
        COALESCE(pd.product_name_uz_latn, 'Unknown Product') AS product_name_uz_latn,
        COALESCE(ch.sales_channel,  'In-House')   AS sales_channel,
        COALESCE(ch.partner_type,   'Internal')   AS partner_type,
        ch.partner_name,
        SUM(cl.co_prem)  AS co_prem,
        SUM(cl.co_liab)  AS co_liab,
        SUM(cl.co_term)  AS co_term
    FROM company_long cl
    LEFT JOIN product_dim pd ON pd.pturi_id  = cl.pturi_id
    LEFT JOIN channel_dim ch ON ch.anketa_id = cl.anketa_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
),

-- Attach claims from curated_claims_portfolio by report_month only.
-- Distribute proportionally by premium share so the month total = KPI card total.
company_with_claims AS (
    SELECT
        ca.report_month,
        ca.report_year,
        ca.insurance_type,
        ca.insurance_type_ru,
        ca.insurance_type_uz_cyrl,
        ca.insurance_type_uz_latn,
        ca.category,
        ca.category_ru,
        ca.category_uz,
        ca.category_uz_latn,
        ca.product_name,
        ca.product_name_ru,
        ca.product_name_uz,
        ca.product_name_uz_latn,
        ca.sales_channel,
        ca.partner_type,
        ca.partner_name,
        ca.co_prem,
        ca.co_liab,
        ca.co_term,
        CASE
            -- If no premium at all for this month, split claims equally across rows
            WHEN SUM(ca.co_prem) OVER (PARTITION BY ca.report_month) = 0
            THEN COALESCE(
                cc.claims_uzs
                / NULLIF(COUNT(*) OVER (PARTITION BY ca.report_month), 0),
                0
            )
            -- Otherwise distribute proportionally by each row's share of monthly premium
            ELSE COALESCE(
                cc.claims_uzs
                * ca.co_prem
                / NULLIF(SUM(ca.co_prem) OVER (PARTITION BY ca.report_month), 0),
                0
            )
        END AS co_claims
    FROM company_agg ca
    LEFT JOIN co_claims_by_month cc
        ON cc.report_month = ca.report_month
),

-- ================================================================
-- SECTION 3: PAIRING CY AND PY  (same calendar month, prior year)
-- ================================================================
final_pairs AS (
    SELECT
        COALESCE(cy.report_month, (py.report_month + INTERVAL '1 year')::DATE) AS report_month,
        COALESCE(cy.report_year, py.report_year + 1) AS report_year,
        COALESCE(cy.report_year - 1, py.report_year) AS prior_year,
        COALESCE((cy.report_month - INTERVAL '1 year')::DATE, py.report_month) AS prior_year_month,
        COALESCE(cy.insurance_type, py.insurance_type) AS insurance_type,
        COALESCE(cy.insurance_type_ru, py.insurance_type_ru) AS insurance_type_ru,
        COALESCE(cy.insurance_type_uz_cyrl, py.insurance_type_uz_cyrl) AS insurance_type_uz_cyrl,
        COALESCE(cy.insurance_type_uz_latn, py.insurance_type_uz_latn) AS insurance_type_uz_latn,
        COALESCE(cy.category, py.category) AS category,
        COALESCE(cy.category_ru, py.category_ru) AS category_ru,
        COALESCE(cy.category_uz, py.category_uz) AS category_uz,
        COALESCE(cy.category_uz_latn, py.category_uz_latn) AS category_uz_latn,
        COALESCE(cy.product_name, py.product_name) AS product_name,
        COALESCE(cy.product_name_ru, py.product_name_ru) AS product_name_ru,
        COALESCE(cy.product_name_uz, py.product_name_uz) AS product_name_uz,
        COALESCE(cy.product_name_uz_latn, py.product_name_uz_latn) AS product_name_uz_latn,
        COALESCE(cy.sales_channel, py.sales_channel) AS sales_channel,
        COALESCE(cy.partner_type, py.partner_type) AS partner_type,
        COALESCE(cy.partner_name, py.partner_name) AS partner_name,

        -- Current Month Metrics
        cy.co_prem   AS co_prem_cy,
        cy.co_liab   AS co_liab_cy,
        cy.co_claims AS co_claims_cy,
        cy.co_term   AS co_term_cy,

        -- Prior Year Same Month Metrics (from self-join on month - 12 months)
        py.co_prem   AS co_prem_py,
        py.co_liab   AS co_liab_py,
        py.co_claims AS co_claims_py,
        py.co_term   AS co_term_py
    FROM company_with_claims cy
    FULL OUTER JOIN company_with_claims py
        ON  py.report_month    = (cy.report_month - INTERVAL '1 year')::DATE
        AND py.insurance_type  = cy.insurance_type
        AND py.category        = cy.category
        AND py.product_name    = cy.product_name
        AND py.sales_channel   = cy.sales_channel
        AND py.partner_type    = cy.partner_type
        AND COALESCE(py.partner_name, '') = COALESCE(cy.partner_name, '')
)

-- ================================================================
-- SECTION 4: FINAL OUTPUT
-- ================================================================
SELECT
    fp.report_month,
    fp.report_year,
    fp.prior_year,
    fp.prior_year_month,
    fp.insurance_type,
    fp.insurance_type_ru,
    fp.insurance_type_uz_cyrl,
    fp.insurance_type_uz_latn,
    fp.category,
    fp.category_ru,
    fp.category_uz,
    fp.category_uz_latn,
    fp.product_name,
    fp.product_name_ru,
    fp.product_name_uz,
    fp.product_name_uz_latn,
    fp.sales_channel,
    fp.partner_type,
    fp.partner_name,

    -- PREMIUMS (CY & PY)
    ROUND(COALESCE(fp.co_prem_cy, 0)::NUMERIC, 2)   AS co_premium_volume_cy,
    ROUND(COALESCE(fp.co_prem_py, 0)::NUMERIC, 2)   AS co_premium_volume_py,

    -- CLAIMS (CY & PY) -- matches KPI card exactly when summed
    ROUND(COALESCE(fp.co_claims_cy, 0)::NUMERIC, 2) AS co_claims_volume_cy,
    ROUND(COALESCE(fp.co_claims_py, 0)::NUMERIC, 2) AS co_claims_volume_py,

    -- LIABILITIES (CY & PY)
    ROUND(COALESCE(fp.co_liab_cy, 0)::NUMERIC, 2)   AS co_liabilities_cy,
    ROUND(COALESCE(fp.co_liab_py, 0)::NUMERIC, 2)   AS co_liabilities_py,

    -- TERMINATED CONTRACTS (CY & PY)
    ROUND(COALESCE(fp.co_term_cy, 0)::NUMERIC, 2)   AS co_terminated_cy,
    ROUND(COALESCE(fp.co_term_py, 0)::NUMERIC, 2)   AS co_terminated_py

FROM final_pairs fp
ORDER BY fp.report_month DESC, fp.insurance_type, fp.category, fp.product_name
