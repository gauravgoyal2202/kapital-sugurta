{{ config(materialized='table') }}

/*
  Dashboard 17 — Customer LTV & CAC (Quarterly) - Validated Client Logic
  -----------------------------------------------------------------------
  LTV  = (Premium - Claims - Commissions - Reinsurance_Out) / Active_Customers / churn_rate
         where churn_rate = 1 - retention_rate
         Premium is monthly DELTA of premium_income from curated_financial_performance (YTD values)

  CAC  = (commission_expenses_end_curr - commission_expenses_end_prev) / new_policies
         Source: curated_financial_performance.commission_expenses (1C "Расходы по реализации")
         This is the MONTHLY DELTA of cumulative sales/distribution expenses.
*/

WITH -- -----------------------------------------------------------------------
-- Step 1: Pull monthly financial performance data (YTD cumulative from 1C API)
-- -----------------------------------------------------------------------
fin_monthly_raw AS (
    SELECT
        report_date,
        premium_income,
        commission_expenses,
        LAG(premium_income)      OVER (ORDER BY report_date) AS prev_premium_income,
        LAG(commission_expenses) OVER (ORDER BY report_date) AS prev_commission_expenses
    FROM {{ ref('curated_financial_performance') }}
    WHERE scenario = 'Actual'
),

-- Monthly DELTA values (convert YTD cumulative to monthly amounts)
fin_monthly AS (
    SELECT
        DATE_TRUNC('month', report_date)::DATE AS report_month,
        -- Monthly premium = current YTD minus previous month YTD
        GREATEST(
            COALESCE(premium_income, 0) - COALESCE(prev_premium_income, 0),
            0
        ) AS monthly_premium_uzs,
        -- Commission expenses (Расходы по реализации) - raw YTD value for CAC delta
        COALESCE(commission_expenses, 0) AS commission_expenses_ytd,
        COALESCE(prev_commission_expenses, 0) AS prev_commission_expenses_ytd
    FROM fin_monthly_raw
),

-- -----------------------------------------------------------------------
-- Step 2: Monthly claims (payout_date basis)
-- -----------------------------------------------------------------------
claims_monthly AS (
    SELECT
        DATE_TRUNC('month', payout_date)::DATE AS report_month,
        SUM(payout_total) AS monthly_claims_uzs
    FROM {{ ref('curated_claims_portfolio') }}
    GROUP BY 1
),

-- -----------------------------------------------------------------------
-- Step 3: Monthly agent commissions (from curated_agency_commissions)
-- -----------------------------------------------------------------------
commissions_monthly AS (
    SELECT
        DATE_TRUNC('month', commission_date)::DATE AS report_month,
        SUM(commission_amount_uzs) AS monthly_commissions_uzs
    FROM {{ ref('curated_agency_commissions') }}
    GROUP BY 1
),

-- -----------------------------------------------------------------------
-- Step 4: Monthly reinsurance outgoing premiums
-- -----------------------------------------------------------------------
reinsurance_monthly AS (
    SELECT
        DATE_TRUNC('month', premium_accrual_date)::DATE AS report_month,
        SUM(total_accrued_premium_uzs) AS monthly_reinsurance_out_uzs
    FROM {{ ref('curated_reinsurance_outgoing_portfolio') }}
    GROUP BY 1
),

-- -----------------------------------------------------------------------
-- Step 5: Terminated contracts (policy premium returned on cancellation)
-- Use tb_summa (original policy premium) NOT vozvrat_sum (accounting reversal)
-- -----------------------------------------------------------------------
terminated_contracts_monthly AS (
    SELECT
        DATE_TRUNC('month', tb_dateras::date)::DATE AS report_month,
        SUM(tb_summa::NUMERIC) AS monthly_terminated_uzs
    FROM raw.ins_rastorg_oracle
    WHERE tb_dateras IS NOT NULL
      AND tb_summa IS NOT NULL
    GROUP BY 1
),

-- -----------------------------------------------------------------------
-- Step 6: New policies (customers whose first policy started this month)
-- -----------------------------------------------------------------------
new_policies_monthly AS (
    SELECT
        DATE_TRUNC('month', report_month)::DATE AS report_month,
        legal_form,
        customer_segment,
        oked_industry_code,
        region_code,
        COUNT(DISTINCT customer_id) AS new_policies_count
    FROM {{ ref('mart_customer_retention_spine') }}
    WHERE is_active_curr = 1 AND was_active_prev = 0
    GROUP BY 1, 2, 3, 4, 5
),

-- -----------------------------------------------------------------------
-- Step 6: Customer base metrics (active, retention) per month × segment
-- -----------------------------------------------------------------------
customer_base AS (
    SELECT
        report_month,
        legal_form,
        customer_segment,
        oked_industry_code,
        region_code,
        active_customers,
        retained_customers,
        customers_at_start_of_period,
        retention_rate_pct,
        -- Churn rate = 1 - retention_rate
        GREATEST(1.0 - retention_rate_pct / 100.0, 0.0001) AS churn_rate
    FROM {{ ref('mart_customer_base_summary') }}
),

-- -----------------------------------------------------------------------
-- Step 8: Monthly aggregation per segment (keeps monthly grain)
-- -----------------------------------------------------------------------
monthly_agg AS (
    SELECT
        cb.report_month,
        DATE_TRUNC('quarter', cb.report_month)::DATE AS report_quarter,
        cb.legal_form,
        cb.customer_segment,
        cb.oked_industry_code,
        cb.region_code,

        -- Active customers for this segment-month
        cb.active_customers,
        cb.retained_customers,
        cb.customers_at_start_of_period,
        cb.retention_rate_pct,

        -- Weighted churn rate (segment-level)
        GREATEST(
            1.0 - cb.retained_customers::NUMERIC
                / NULLIF(cb.customers_at_start_of_period, 0)
        , 0.0001) AS churn_rate,

        -- Net margin proportioned by segment share of active customers
        (
            COALESCE(fm.monthly_premium_uzs, 0)
            - COALESCE(cl.monthly_claims_uzs, 0)
            - COALESCE(co.monthly_commissions_uzs, 0)
            - COALESCE(ri.monthly_reinsurance_out_uzs, 0)
            - COALESCE(te.monthly_terminated_uzs, 0)
        ) * (
            CASE WHEN total_active.total_active > 0
                 THEN cb.active_customers::NUMERIC / total_active.total_active
                 ELSE 0 END
        ) AS monthly_net_margin_uzs,

        -- Raw financial components (for drill-down)
        COALESCE(fm.monthly_premium_uzs, 0) AS premium_uzs,
        COALESCE(cl.monthly_claims_uzs, 0) AS claims_uzs,
        COALESCE(co.monthly_commissions_uzs, 0) AS commissions_uzs,
        COALESCE(ri.monthly_reinsurance_out_uzs, 0) AS reinsurance_out_uzs,
        COALESCE(te.monthly_terminated_uzs, 0) AS terminated_contracts_uzs

    FROM customer_base cb
    LEFT JOIN fin_monthly fm ON fm.report_month = cb.report_month
    LEFT JOIN claims_monthly cl ON cl.report_month = cb.report_month
    LEFT JOIN commissions_monthly co ON co.report_month = cb.report_month
    LEFT JOIN reinsurance_monthly ri ON ri.report_month = cb.report_month
    LEFT JOIN terminated_contracts_monthly te ON te.report_month = cb.report_month
    LEFT JOIN (
        SELECT report_month, SUM(active_customers) AS total_active
        FROM {{ ref('mart_customer_base_summary') }}
        GROUP BY report_month
    ) total_active ON total_active.report_month = cb.report_month
),

-- -----------------------------------------------------------------------
-- Step 9: CAC per month per segment
-- -----------------------------------------------------------------------
cac_monthly AS (
    SELECT
        np.report_month,
        np.legal_form,
        np.customer_segment,
        np.oked_industry_code,
        np.region_code,
        np.new_policies_count,
        (fm.commission_expenses_ytd - fm.prev_commission_expenses_ytd) AS commission_expense_delta
    FROM new_policies_monthly np
    LEFT JOIN fin_monthly fm ON fm.report_month = np.report_month
)

-- -----------------------------------------------------------------------
-- Final SELECT — monthly grain with LTV & CAC
-- -----------------------------------------------------------------------
SELECT
    m.report_month,
    m.report_quarter,
    EXTRACT(YEAR  FROM m.report_month)::INT  AS report_year,
    EXTRACT(MONTH FROM m.report_month)::INT  AS report_month_num,
    EXTRACT(QUARTER FROM m.report_month)::INT AS report_quarter_num,
    m.legal_form,
    m.customer_segment,
    m.oked_industry_code,
    m.region_code,
    'Actual' AS scenario,

    -- Active Customers
    m.active_customers,
    m.retained_customers,
    m.customers_at_start_of_period,
    ROUND(m.retention_rate_pct::NUMERIC, 2) AS retention_rate_pct,
    ROUND(m.churn_rate::NUMERIC, 4)         AS churn_rate,

    -- Financial components (segment-proportioned)
    ROUND(m.monthly_net_margin_uzs::NUMERIC, 0) AS net_margin_uzs,

    -- LTV = Net Margin per Customer / Churn Rate
    CASE WHEN m.active_customers > 0 AND m.churn_rate > 0
        THEN ROUND(
            (
                (m.monthly_net_margin_uzs / NULLIF(m.active_customers, 0))
                / NULLIF(m.churn_rate, 0)
            )::NUMERIC
        , 2)
        ELSE 0
    END AS customer_ltv_uzs,

    -- CAC
    COALESCE(c.new_policies_count, 0)           AS new_policies_acquired,
    COALESCE(c.commission_expense_delta, 0)     AS commission_expense_delta_uzs,

    CASE WHEN COALESCE(c.new_policies_count, 0) > 0
        THEN ROUND(
            (
                COALESCE(c.commission_expense_delta, 0)
                / c.new_policies_count
            )::NUMERIC
        , 2)
        ELSE 0
    END AS customer_acquisition_cost_uzs,

    -- Raw components for Power BI drill-down
    ROUND(m.premium_uzs::NUMERIC, 0)              AS premium_uzs,
    ROUND(m.claims_uzs::NUMERIC, 0)                AS claims_uzs,
    ROUND(m.commissions_uzs::NUMERIC, 0)           AS commissions_uzs,
    ROUND(m.reinsurance_out_uzs::NUMERIC, 0)       AS reinsurance_out_uzs,
    ROUND(m.terminated_contracts_uzs::NUMERIC, 0)  AS terminated_contracts_uzs

FROM monthly_agg m
LEFT JOIN cac_monthly c
    ON  c.report_month        = m.report_month
    AND c.legal_form          = m.legal_form
    AND c.customer_segment    = m.customer_segment
    AND c.oked_industry_code  = m.oked_industry_code
    AND c.region_code         = m.region_code

ORDER BY m.report_month DESC, m.customer_segment

