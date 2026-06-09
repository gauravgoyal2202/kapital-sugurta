{{ config(materialized='table') }}

/*
  Dashboard 11 — Bottom Section: Fully Generic Priority Areas Summary
  ------------------------------------------------------------------
  Groups by report_month, report_year, report_quarter, priority_area,
  insurance_type, product_category, and product_name.
  Tracks company premium, net profit, and their respective YoY growth percentages.
*/

WITH base_data AS (
    SELECT
        report_month,
        report_year,
        EXTRACT(QUARTER FROM report_month)::INT AS report_quarter,
        priority_area,
        insurance_type,
        product_category,
        product_name,
        SUM(co_premium) AS co_premium,
        SUM(co_claims) AS co_claims,
        SUM(co_expenses) AS co_expenses,
        SUM(co_fifty) AS co_fifty,
        SUM(co_ras) AS co_ras,
        SUM(co_premium - co_expenses - co_claims - co_fifty - co_ras) AS co_net_profit
    FROM {{ ref('mart_commercial_development_priority_areas_base') }}
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

calculations AS (
    SELECT
        report_month,
        report_year,
        report_quarter,
        priority_area,
        insurance_type,
        product_category,
        product_name,
        
        -- CY metrics
        co_premium AS company_premium_cy,
        co_net_profit AS company_net_profit_cy,
        
        -- PY metrics (12-month lag partitioned by dimensions)
        COALESCE(LAG(co_premium, 12) OVER (PARTITION BY priority_area, insurance_type, product_category, product_name ORDER BY report_month), 0) AS company_premium_py,
        COALESCE(LAG(co_net_profit, 12) OVER (PARTITION BY priority_area, insurance_type, product_category, product_name ORDER BY report_month), 0) AS company_net_profit_py,
        
        -- Profitability %
        CASE 
            WHEN co_premium > 0 
            THEN ROUND(((co_premium - co_expenses - co_claims - co_fifty) / co_premium * 100)::NUMERIC, 2)
            ELSE 0 
        END AS profitability_pct
        
    FROM base_data
)

SELECT
    report_month,
    report_year,
    report_quarter,
    priority_area,
    insurance_type,
    product_category,
    product_name,
    ROUND(company_premium_cy::NUMERIC, 2) AS company_premium_cy,
    ROUND(company_premium_py::NUMERIC, 2) AS company_premium_py,
    
    CASE 
        WHEN company_premium_py > 0 
        THEN ROUND(((company_premium_cy - company_premium_py) / company_premium_py * 100)::NUMERIC, 2)
        ELSE NULL 
    END AS premium_growth_pct,
    
    ROUND(company_net_profit_cy::NUMERIC, 2) AS company_net_profit_cy,
    ROUND(company_net_profit_py::NUMERIC, 2) AS company_net_profit_py,
    
    CASE 
        WHEN company_net_profit_py <> 0 
        THEN ROUND(((company_net_profit_cy - company_net_profit_py) / ABS(company_net_profit_py) * 100)::NUMERIC, 2)
        ELSE NULL 
    END AS net_profit_growth_pct,
    
    profitability_pct

FROM calculations
ORDER BY report_month DESC, priority_area, product_name
