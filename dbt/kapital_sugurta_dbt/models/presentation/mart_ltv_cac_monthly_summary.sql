{{ config(materialized='table') }}

/*
  Dashboard 17 — LTV & CAC Monthly Summary (Company Level)
  ---------------------------------------------------------
  This is the Power BI source table for LTV & CAC KPI cards.
  All values are at company level (no segment split) to match
  the client's validation sheet exactly.

  LTV = (Premium Δ - Claims - Commissions - Reinsurance - Terminated) / Active Customers / Churn Rate
  CAC = Commission Expenses Monthly Delta / New Policies Count
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

-- Step 2: Premium delta (YTD → monthly)
premium_delta AS (
    SELECT
        report_date,
        premium_income,
        LAG(premium_income) OVER (ORDER BY report_date) AS prev_premium,
        premium_income - LAG(premium_income) OVER (ORDER BY report_date) AS monthly_premium
    FROM {{ ref('curated_financial_performance') }}
    WHERE scenario = 'Actual'
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

-- Step 6: Monthly terminated contracts (tb_summa = policy premium at cancellation)
terminated_monthly AS (
    SELECT
        DATE_TRUNC('month', tb_dateras::date)::DATE AS report_month,
        SUM(tb_summa::NUMERIC) AS terminated_uzs
    FROM raw.ins_rastorg_oracle
    WHERE tb_dateras IS NOT NULL
      AND tb_summa IS NOT NULL
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

-- Step 8: Active customers & churn rate (weighted company-level)
customer_metrics AS (
    SELECT
        report_month,
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
    GROUP BY 1
)

-- Final output: one row per month, company level
SELECT
    cm.report_month,
    DATE_TRUNC('quarter', cm.report_month)::DATE             AS report_quarter,
    EXTRACT(YEAR FROM cm.report_month)::INT                   AS report_year,
    EXTRACT(MONTH FROM cm.report_month)::INT                  AS report_month_num,
    EXTRACT(QUARTER FROM cm.report_month)::INT                AS report_quarter_num,
    TO_CHAR(cm.report_month, 'Mon YYYY')                      AS report_month_label,

    -- Customer metrics
    cm.active_customers,
    cm.retained_customers,
    cm.customers_at_start,
    ROUND(cm.retention_rate_pct::NUMERIC, 2)                  AS retention_rate_pct,
    ROUND(cm.churn_rate::NUMERIC, 4)                          AS churn_rate,

    -- LTV components
    ROUND(COALESCE(pd.monthly_premium, 0)::NUMERIC, 2)       AS premium_delta_uzs,
    ROUND(COALESCE(cl.claims_uzs, 0)::NUMERIC, 2)            AS claims_uzs,
    ROUND(COALESCE(co.commissions_uzs, 0)::NUMERIC, 2)       AS commissions_uzs,
    ROUND(COALESCE(ri.reinsurance_uzs, 0)::NUMERIC, 2)       AS reinsurance_out_uzs,
    ROUND(COALESCE(te.terminated_uzs, 0)::NUMERIC, 2)        AS terminated_contracts_uzs,

    -- Net margin
    ROUND((
        COALESCE(pd.monthly_premium, 0)
        - COALESCE(cl.claims_uzs, 0)
        - COALESCE(co.commissions_uzs, 0)
        - COALESCE(ri.reinsurance_uzs, 0)
        - COALESCE(te.terminated_uzs, 0)
    )::NUMERIC, 2) AS net_margin_uzs,

    -- LTV = net_margin / active_customers / churn_rate
    CASE WHEN cm.active_customers > 0 AND cm.churn_rate > 0
        THEN ROUND((
            (COALESCE(pd.monthly_premium, 0)
             - COALESCE(cl.claims_uzs, 0)
             - COALESCE(co.commissions_uzs, 0)
             - COALESCE(ri.reinsurance_uzs, 0)
             - COALESCE(te.terminated_uzs, 0))
            / cm.active_customers
            / cm.churn_rate
        )::NUMERIC, 2)
        ELSE 0
    END AS customer_ltv_uzs,

    -- CAC components
    COALESCE(np.new_policies_count, 0)                        AS new_policies_count,
    ROUND(COALESCE(cd.monthly_expense_delta, 0)::NUMERIC, 2) AS commission_expense_delta_uzs,

    -- CAC = expense_delta / new_policies
    CASE WHEN COALESCE(np.new_policies_count, 0) > 0
        THEN ROUND((
            COALESCE(cd.monthly_expense_delta, 0)
            / np.new_policies_count
        )::NUMERIC, 2)
        ELSE 0
    END AS customer_acquisition_cost_uzs

FROM customer_metrics cm
LEFT JOIN premium_delta pd
    ON DATE_TRUNC('month', pd.report_date)::DATE = cm.report_month
    AND pd.monthly_premium IS NOT NULL
LEFT JOIN commission_delta cd
    ON DATE_TRUNC('month', cd.report_date)::DATE = cm.report_month
    AND cd.monthly_expense_delta IS NOT NULL
LEFT JOIN claims_monthly cl ON cl.report_month = cm.report_month
LEFT JOIN commissions_monthly co ON co.report_month = cm.report_month
LEFT JOIN reinsurance_monthly ri ON ri.report_month = cm.report_month
LEFT JOIN terminated_monthly te ON te.report_month = cm.report_month
LEFT JOIN new_policies_monthly np ON np.report_month = cm.report_month

ORDER BY cm.report_month
