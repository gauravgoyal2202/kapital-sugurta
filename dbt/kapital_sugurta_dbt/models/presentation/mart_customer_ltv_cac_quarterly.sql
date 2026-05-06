{{ config(materialized='table') }}

/*
  Dashboard 17 — Customer LTV & CAC (Quarterly)
  -----------------------------------------------
  LTV  = Avg Quarterly Premium per Customer
         × Avg Customer Lifetime in Quarters
         × Avg Profit Margin
  
  where: Profit Margin = Quarterly Net Profit / Quarterly Earned Premium (from API)
  
  CAC  = Total Marketing Expenses in Quarter / New Customers Acquired in Quarter
         (New Customer = active in quarter but NOT active in previous quarter)

  Sources:
    - curated_insurance_premium     → quarterly premium per customer
    - curated_financial_performance → quarterly net_profit + premium_income (API)
    - curated_active_customer_portfolio → customer lifetime + new customer flag
*/

-- Step 1: Quarterly premium aggregated per customer
WITH quarterly_premium_per_customer AS (
    SELECT
        DATE_TRUNC('quarter', p.payment_date)::DATE AS report_quarter,
        an.owner AS customer_id,
        k.tb_orgoked::VARCHAR AS oked_industry_code,
        CASE WHEN k.tb_fizyur = 0 THEN 'Physical' ELSE 'Juridical' END AS legal_form,
        CASE WHEN k.tb_fizyur = 0 THEN 'Retail' ELSE 'Corporate' END AS customer_segment,
        SUBSTRING(COALESCE(an.temp_div, an.ins_div)::VARCHAR, 1, 2) AS region_code,
        SUM(p.premium_amount) AS total_premium_uzs,
        COUNT(DISTINCT p.pturi_id) AS policy_count
    FROM {{ ref('curated_insurance_premium') }} p
    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} an ON an.ins_id = p.anketa_id
    LEFT JOIN {{ source('raw', 'ins_kontragent_oracle') }} k ON an.owner = k.tb_id
    WHERE p.payment_date IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6
),

-- Step 2: Active customers per quarter for lifetime calculation
quarterly_active_customers AS (
    SELECT
        DATE_TRUNC('quarter', report_month)::DATE AS report_quarter,
        customer_id,
        MAX(legal_form) AS legal_form,
        MAX(customer_segment) AS customer_segment,
        MAX(oked_industry_code) AS oked_industry_code,
        MAX(region_code) AS region_code
    FROM {{ ref('curated_active_customer_portfolio') }}
    GROUP BY 1, 2
),

-- Step 3: Calculate how many quarters each customer has been active (lifetime)
customer_lifetime_quarters AS (
    SELECT
        report_quarter,
        customer_id,
        legal_form,
        customer_segment,
        oked_industry_code,
        region_code,
        -- Count quarters active up to this quarter (rolling back as actual lifetime)
        COUNT(*) OVER (
            PARTITION BY customer_id
            ORDER BY report_quarter ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS lifetime_quarters_so_far
    FROM quarterly_active_customers
),

-- Step 4: Aggregate premium and lifetime per quarter
quarterly_ltv_components AS (
    SELECT
        q.report_quarter,
        q.legal_form,
        q.customer_segment,
        q.oked_industry_code,
        q.region_code,
        COUNT(DISTINCT q.customer_id)     AS active_customers,
        SUM(COALESCE(p.total_premium_uzs, 0)) AS total_premium_uzs,
        -- Avg Premium per Customer this quarter
        CASE WHEN COUNT(DISTINCT q.customer_id) > 0
            THEN SUM(COALESCE(p.total_premium_uzs, 0)) / COUNT(DISTINCT q.customer_id)
            ELSE 0
        END AS avg_quarterly_premium_per_customer,
        -- Avg Customer Lifetime in Quarters
        AVG(q.lifetime_quarters_so_far) AS avg_lifetime_quarters
    FROM customer_lifetime_quarters q
    LEFT JOIN quarterly_premium_per_customer p
        ON p.report_quarter = q.report_quarter
       AND p.customer_id = q.customer_id
    GROUP BY 1, 2, 3, 4, 5
),

-- Step 5: Grab quarterly profit margin from the financial API
quarterly_profit_margin AS (
    SELECT
        DATE_TRUNC('quarter', report_date)::DATE AS report_quarter,
        -- Client-confirmed: Profit Margin = Net Profit / Earned Premium
        CASE WHEN SUM(premium_income) > 0
            THEN SUM(net_profit) / SUM(premium_income)
            ELSE 0
        END AS avg_profit_margin,
        SUM(marketing_expenses) AS total_marketing_expenses_uzs
    FROM {{ ref('curated_financial_performance') }}
    GROUP BY 1
),

-- Step 6: Identify NEW customers per quarter (not active in the previous quarter)
new_customers_per_quarter AS (
    SELECT
        curr.report_quarter,
        curr.legal_form,
        curr.customer_segment,
        curr.oked_industry_code,
        curr.region_code,
        COUNT(DISTINCT curr.customer_id) AS new_customers_acquired
    FROM quarterly_active_customers curr
    WHERE NOT EXISTS (
        SELECT 1 FROM quarterly_active_customers prev
        WHERE prev.customer_id = curr.customer_id
          AND prev.report_quarter = (curr.report_quarter - INTERVAL '3 months')::DATE
    )
    GROUP BY 1, 2, 3, 4, 5
)

-- Final Output: LTV and CAC per quarter with filter dimensions
SELECT
    l.report_quarter,
    EXTRACT(YEAR FROM l.report_quarter)::INT     AS report_year,
    EXTRACT(QUARTER FROM l.report_quarter)::INT  AS report_quarter_num,

    -- Dimension Filters
    l.legal_form,
    l.customer_segment,
    l.region_code,
    l.oked_industry_code,

    -- Scenario flag (Actual = transactional data from Oracle/API; Plan = budget data when available)
    'Actual' AS scenario,

    -- Active Customer Base
    l.active_customers,
    l.avg_quarterly_premium_per_customer,
    ROUND(l.avg_lifetime_quarters::NUMERIC, 2)   AS avg_customer_lifetime_quarters,
    ROUND(COALESCE(m.avg_profit_margin, 0)::NUMERIC, 4) AS avg_profit_margin,

    -- ===== LTV Calculation =====
    -- LTV = Avg Quarterly Premium per Customer × Avg Lifetime in Quarters × Profit Margin
    ROUND(
        (l.avg_quarterly_premium_per_customer
         * l.avg_lifetime_quarters
         * COALESCE(m.avg_profit_margin, 0))::NUMERIC
    , 0) AS customer_ltv_uzs,

    -- New Customers Acquired this quarter
    COALESCE(nc.new_customers_acquired, 0) AS new_customers_acquired,

    -- Total Marketing Spend from API
    COALESCE(m.total_marketing_expenses_uzs, 0) AS total_marketing_expenses_uzs,

    -- ===== CAC Calculation =====
    -- CAC = Total Marketing Expenses / New Customers Acquired
    CASE WHEN COALESCE(nc.new_customers_acquired, 0) > 0
        THEN ROUND(
            (COALESCE(m.total_marketing_expenses_uzs, 0)
             / nc.new_customers_acquired)::NUMERIC
        , 0)
        ELSE 0
    END AS customer_acquisition_cost_uzs

FROM quarterly_ltv_components l
LEFT JOIN quarterly_profit_margin m
  ON m.report_quarter = l.report_quarter
LEFT JOIN new_customers_per_quarter nc
  ON nc.report_quarter = l.report_quarter
 AND nc.legal_form = l.legal_form
 AND nc.customer_segment = l.customer_segment
 AND nc.oked_industry_code = l.oked_industry_code
 AND nc.region_code = l.region_code
ORDER BY l.report_quarter DESC
