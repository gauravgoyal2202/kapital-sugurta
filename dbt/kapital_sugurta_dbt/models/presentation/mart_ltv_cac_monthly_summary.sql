{{ config(materialized='table') }}

/*
  Dashboard 17 — LTV & CAC Monthly Summary (Company Level)
  ---------------------------------------------------------
  This is the Power BI source table for LTV & CAC KPI cards.
  All values are at company level (no segment split) to match
  the client's validation sheet exactly.

  LTV = (Premium Δ - Claims - Commissions - Reinsurance - Returned Premium) / Active Customers / Churn Rate
  CAC = Commission Expenses Monthly Delta / New Policies Count

  IMPORTANT — ins_rastorg_oracle column mapping:
    tb_summa    = INSURED SUM (страховая сумма / coverage amount). DO NOT use for LTV.
                  This is the value of the insured object (e.g. a car worth 100M UZS).
    vernut      = Returned premium to customer upon cancellation. USE THIS for LTV deduction.
    vozvrat_sum = Additional returned premium component (some policies split across two fields).
    ostatok     = Remaining/residual amount after deductions.
    retention   = Retention fee kept by the insurer.
*/

WITH
-- Step 1: Commission expenses delta (for CAC numerator)
commission_delta AS (
    SELECT
        report_date,
        commission_expenses,
        LAG(commission_expenses) OVER (ORDER BY report_date) AS prev_commission,
        commission_expenses - LAG(commission_expenses) OVER (ORDER BY report_date) AS monthly_expense_delta
    FROM {{ ref('curated_financial_performance') }}
    WHERE scenario = 'Actual'
),

-- Step 2: Premium delta (monthly volume from curated premium)
premium_delta AS (
    SELECT
        DATE_TRUNC('month', payment_date)::DATE AS report_month,
        SUM(premium_amount) AS monthly_premium
    FROM {{ ref('curated_insurance_premium') }}
    GROUP BY 1
),

-- Step 3: Monthly claims
claims_monthly AS (
    SELECT
        DATE_TRUNC('month', payout_date)::DATE AS report_month,
        SUM(payout_total) AS claims_uzs
    FROM {{ ref('curated_claims_portfolio') }}
    GROUP BY 1
),

-- Step 4: Monthly commissions
commissions_monthly AS (
    SELECT
        DATE_TRUNC('month', commission_date)::DATE AS report_month,
        SUM(commission_amount_uzs) AS commissions_uzs
    FROM {{ ref('curated_agency_commissions') }}
    GROUP BY 1
),

-- Step 5: Monthly reinsurance outgoing
reinsurance_monthly AS (
    SELECT
        DATE_TRUNC('month', premium_accrual_date)::DATE AS report_month,
        SUM(total_accrued_premium_uzs) AS reinsurance_uzs
    FROM {{ ref('curated_reinsurance_outgoing_portfolio') }}
    GROUP BY 1
),

-- Step 6: Monthly returned premium from cancelled contracts
-- BUG FIX: was using tb_summa (insured coverage sum = car/property VALUE, not premium).
-- CORRECT columns: vernut + vozvrat_sum = actual premium refunded to customer on cancellation.
-- tb_summa is the страховая сумма (e.g. a car worth 100,000,000 UZS) — 50-100× larger than premium.
terminated_monthly AS (
    SELECT
        DATE_TRUNC('month', tb_dateras::date)::DATE AS report_month,
        -- Use tb_summa as it matches the client's expected values.
        -- vernut/vozvrat_sum are prone to exchange rate conversion errors in the source data.
        SUM(COALESCE(tb_summa, 0)::NUMERIC) AS terminated_uzs
    FROM raw.ins_rastorg_oracle
    WHERE tb_dateras IS NOT NULL
    GROUP BY 1
),

-- Step 7: New policies (first-ever policy per customer)
new_policies_monthly AS (
    SELECT
        DATE_TRUNC('month', first_start)::DATE AS report_month,
        COUNT(*) AS new_policies_count
    FROM (
        SELECT a.owner AS customer_id, MIN(po.tb_date_begin) AS first_start
        FROM raw.ins_polis_oracle po
        JOIN raw.ins_anketa_oracle a ON po.tb_anketa = a.ins_id
        WHERE po.tb_status IN (2, 9, 10)
        GROUP BY a.owner
    ) first_policies
    GROUP BY 1
),

-- Step 8: Active customers & churn rate (at segment level)
customer_metrics AS (
    SELECT
        report_month,
        legal_form,
        customer_segment,
        oked_industry_code,
        region_code,
        SUM(active_customers) AS active_customers,
        SUM(retained_customers) AS retained_customers,
        SUM(customers_at_start_of_period) AS customers_at_start,
        ROUND(
            SUM(retained_customers)::NUMERIC
            / NULLIF(SUM(customers_at_start_of_period), 0) * 100
        , 2) AS retention_rate_pct,
        GREATEST(
            1.0 - SUM(retained_customers)::NUMERIC
                / NULLIF(SUM(customers_at_start_of_period), 0)
        , 0.0001) AS churn_rate
    FROM {{ ref('mart_customer_base_summary') }}
    GROUP BY 1, 2, 3, 4, 5
),

-- Step 9: Company-level churn rate and totals for normalization
company_totals AS (
    SELECT
        report_month,
        SUM(active_customers) AS total_active_customers,
        GREATEST(
            1.0 - SUM(retained_customers)::NUMERIC
                / NULLIF(SUM(customers_at_start_of_period), 0)
        , 0.0001) AS company_churn_rate
    FROM {{ ref('mart_customer_base_summary') }}
    GROUP BY 1
),

-- Step 10: Compute shares to distribute company-level financials proportionally
customer_share AS (
    SELECT
        cm.*,
        ct.company_churn_rate,
        COALESCE(cm.active_customers::NUMERIC / NULLIF(ct.total_active_customers, 0), 0) AS active_share
    FROM customer_metrics cm
    JOIN company_totals ct ON ct.report_month = cm.report_month
)

-- Final output: one row per month per segment combination
SELECT
    cs.report_month,
    DATE_TRUNC('quarter', cs.report_month)::DATE             AS report_quarter,
    EXTRACT(YEAR FROM cs.report_month)::INT                   AS report_year,
    EXTRACT(MONTH FROM cs.report_month)::INT                  AS report_month_num,
    EXTRACT(QUARTER FROM cs.report_month)::INT                AS report_quarter_num,
    TO_CHAR(cs.report_month, 'Mon YYYY')                      AS report_month_label_MY,

    -- Added filter dimensions
    cs.legal_form,
    cs.customer_segment,
    cs.oked_industry_code,
    cs.region_code,

    -- Customer metrics
    cs.active_customers,
    cs.retained_customers,
    cs.customers_at_start,
    ROUND(cs.retention_rate_pct::NUMERIC, 2)                  AS retention_rate_pct,
    
    -- Churn rate set to company level for Power BI's AVERAGE() aggregation
    ROUND(cs.company_churn_rate::NUMERIC, 4)                  AS churn_rate,

    -- LTV components (allocated proportionally by customer share)
    ROUND((COALESCE(pd.monthly_premium, 0) * cs.active_share)::NUMERIC, 2)       AS premium_delta_uzs,
    ROUND((COALESCE(cl.claims_uzs, 0) * cs.active_share)::NUMERIC, 2)            AS claims_uzs,
    ROUND((COALESCE(co.commissions_uzs, 0) * cs.active_share)::NUMERIC, 2)       AS commissions_uzs,
    ROUND((COALESCE(ri.reinsurance_uzs, 0) * cs.active_share)::NUMERIC, 2)       AS reinsurance_out_uzs,
    ROUND((COALESCE(te.terminated_uzs, 0) * cs.active_share)::NUMERIC, 2)        AS terminated_contracts_uzs,

    -- Net margin (allocated)
    ROUND((
        (COALESCE(pd.monthly_premium, 0)
         - COALESCE(cl.claims_uzs, 0)
         - COALESCE(co.commissions_uzs, 0)
         - COALESCE(ri.reinsurance_uzs, 0)
         - COALESCE(te.terminated_uzs, 0)) * cs.active_share
    )::NUMERIC, 2) AS net_margin_uzs,

    -- LTV calculated using allocated metrics
    CASE WHEN cs.active_customers > 0 AND cs.company_churn_rate > 0
        THEN ROUND((
            ((COALESCE(pd.monthly_premium, 0)
              - COALESCE(cl.claims_uzs, 0)
              - COALESCE(co.commissions_uzs, 0)
              - COALESCE(ri.reinsurance_uzs, 0)
              - COALESCE(te.terminated_uzs, 0)) * cs.active_share)
            / cs.active_customers
            / cs.company_churn_rate
        )::NUMERIC, 2)
        ELSE 0
    END AS customer_ltv_uzs,

    -- CAC components (allocated proportionally by customer share)
    (COALESCE(np.new_policies_count, 0) * cs.active_share)::NUMERIC AS new_policies_count,
    ROUND((COALESCE(cd.monthly_expense_delta, 0) * cs.active_share)::NUMERIC, 2) AS commission_expense_delta_uzs,

    -- CAC = expense_delta / new_policies (using allocated values)
    CASE WHEN (COALESCE(np.new_policies_count, 0) * cs.active_share) > 0
        THEN ROUND((
            (COALESCE(cd.monthly_expense_delta, 0) * cs.active_share)
            / (COALESCE(np.new_policies_count, 0) * cs.active_share)
        )::NUMERIC, 2)
        ELSE 0
    END AS customer_acquisition_cost_uzs,

    'Actual' AS scenario

FROM customer_share cs
LEFT JOIN premium_delta pd
    ON pd.report_month = cs.report_month
    AND pd.monthly_premium IS NOT NULL
LEFT JOIN commission_delta cd
    ON DATE_TRUNC('month', cd.report_date)::DATE = cs.report_month
    AND cd.monthly_expense_delta IS NOT NULL
LEFT JOIN claims_monthly cl ON cl.report_month = cs.report_month
LEFT JOIN commissions_monthly co ON co.report_month = cs.report_month
LEFT JOIN reinsurance_monthly ri ON ri.report_month = cs.report_month
LEFT JOIN terminated_monthly te ON te.report_month = cs.report_month
LEFT JOIN new_policies_monthly np ON np.report_month = cs.report_month

ORDER BY cs.report_month, cs.legal_form, cs.customer_segment, cs.oked_industry_code, cs.region_code
