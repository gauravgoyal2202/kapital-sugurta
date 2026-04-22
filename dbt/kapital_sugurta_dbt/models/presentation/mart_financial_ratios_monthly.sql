WITH claims_agg AS (
    SELECT
        DATE_TRUNC('month', payout_date)::DATE AS report_month,
        scenario,
        SUM(payout_total) AS total_claims
    FROM {{ ref('curated_claims_portfolio') }}
    WHERE payout_date IS NOT NULL
    GROUP BY 1, 2
),

reinsurance_out_agg AS (
    SELECT
        DATE_TRUNC('month', contract_conclusion_date)::DATE AS report_month,
        scenario,
        SUM(total_accrued_premium_uzs) AS outward_ceded_premium
    FROM {{ ref('curated_reinsurance_outgoing_portfolio') }}
    WHERE contract_conclusion_date IS NOT NULL
    GROUP BY 1, 2
),

premium_written_agg AS (
    SELECT
        DATE_TRUNC('month', payment_date)::DATE AS report_month,
        'Actual' AS scenario,
        SUM(premium_amount) AS premium_written
    FROM {{ ref('curated_insurance_premium') }}
    WHERE payment_date IS NOT NULL
    GROUP BY 1, 2
),

balance_sheet_monthly AS (
    SELECT
        DATE_TRUNC('month', report_date)::DATE AS report_month,
        scenario,
        -- Reserve for reported but unsettled claims
        SUM(rbns_reserve) AS p610_reserve, 
        -- P590 at the end of the reporting period
        SUM(unearned_premium_reserve) AS p590_end_of_period,
        -- Subtract LAG element (P590 at beginning of the reporting period)
        SUM(unearned_premium_reserve) - LAG(SUM(unearned_premium_reserve), 1, 0) OVER (PARTITION BY scenario ORDER BY DATE_TRUNC('month', report_date)::DATE) AS p590_change
    FROM {{ ref('curated_balance_sheet') }}
    WHERE report_date IS NOT NULL
    GROUP BY 1, 2
),

financial_perf_monthly AS (
    SELECT
        DATE_TRUNC('month', report_date)::DATE AS report_month,
        scenario,
        SUM(premium_income) AS premium_f011,      -- F011
        SUM(other_premium_income) AS premium_f013,       -- F013
        SUM(reinsurance_premium_ceded) AS reinsurance_f012,   -- F012
        SUM(costs_of_goods_sold) AS expenses_f070,       -- F070
        SUM(period_expenses) AS expenses_f090 -- F090
    FROM {{ ref('curated_financial_performance') }}
    WHERE report_date IS NOT NULL
    GROUP BY 1, 2
),

-- Constructing a master timeline explicitly representing all valid months across facts
all_months AS (
    SELECT DISTINCT report_month, scenario FROM balance_sheet_monthly
    UNION
    SELECT DISTINCT report_month, scenario FROM financial_perf_monthly
    UNION
    SELECT DISTINCT report_month, scenario FROM claims_agg
    UNION
    SELECT DISTINCT report_month, scenario FROM reinsurance_out_agg
    UNION
    SELECT DISTINCT report_month, scenario FROM premium_written_agg
),

final_kpi AS (
    SELECT
        m.report_month,
        m.scenario,
        
        -- Base Values mapping
        COALESCE(c.total_claims, 0) AS claims_payout,
        COALESCE(bs.p610_reserve, 0) AS p610_reserve,
        COALESCE(fp.premium_f011, 0) AS premium_f011,
        COALESCE(fp.premium_f013, 0) AS premium_f013,
        COALESCE(fp.reinsurance_f012, 0) AS reinsurance_f012,
        COALESCE(bs.p590_change, 0) AS p590_change,
        COALESCE(fp.expenses_f070, 0) AS expenses_f070,
        COALESCE(fp.expenses_f090, 0) AS expenses_f090,
        COALESCE(ro.outward_ceded_premium, 0) AS outward_ceded_premium,
        COALESCE(pw.premium_written, 0) AS premium_written,
        
        -- Master Calculation Denominator
        (COALESCE(fp.premium_f011, 0) + COALESCE(fp.premium_f013, 0) - COALESCE(fp.reinsurance_f012, 0)) - COALESCE(bs.p590_change, 0) AS denominator_net_earned_premium
        
    FROM all_months m
    LEFT JOIN claims_agg c ON m.report_month = c.report_month AND m.scenario = c.scenario
    LEFT JOIN balance_sheet_monthly bs ON m.report_month = bs.report_month AND m.scenario = bs.scenario
    LEFT JOIN financial_perf_monthly fp ON m.report_month = fp.report_month AND m.scenario = fp.scenario
    LEFT JOIN reinsurance_out_agg ro ON m.report_month = ro.report_month AND m.scenario = ro.scenario
    LEFT JOIN premium_written_agg pw ON m.report_month = pw.report_month AND m.scenario = pw.scenario
)

SELECT
    report_month,
    scenario,
    
    -- 1. Loss Ratio %
    -- Formula: (Claims + P610) / ( (011+013) - 012 - P590_change )
    CASE 
        WHEN denominator_net_earned_premium = 0 THEN 0 
        ELSE (claims_payout + p610_reserve) / denominator_net_earned_premium * 100.0 
    END AS loss_ratio_pct,
    
    -- 2. Expense Ratio %
    -- Formula: (F070 + F090) / ( (011+013) - 012 - P590_change )
    CASE 
        WHEN denominator_net_earned_premium = 0 THEN 0 
        ELSE (expenses_f070 + expenses_f090) / denominator_net_earned_premium * 100.0 
    END AS expense_ratio_pct,
    
    -- 3. Combined Ratio %
    -- Formula: Loss Ratio + Expense Ratio
    (
        CASE 
            WHEN denominator_net_earned_premium = 0 THEN 0 
            ELSE (claims_payout + p610_reserve) / denominator_net_earned_premium * 100.0 
        END 
        + 
        CASE 
            WHEN denominator_net_earned_premium = 0 THEN 0 
            ELSE (expenses_f070 + expenses_f090) / denominator_net_earned_premium * 100.0 
        END
    ) AS combined_ratio_pct,
    
    -- 4. Reinsurance Level %
    -- Formula: Outward Ceded Premium / Premium Written
    CASE 
        WHEN premium_written = 0 THEN 0 
        ELSE (outward_ceded_premium / premium_written) * 100.0 
    END AS reinsurance_level_pct,
    
    -- Exporting base columns to allow powerbi drill-downs natively in the visual table
    claims_payout,
    p610_reserve,
    premium_f011,
    premium_f013,
    reinsurance_f012,
    p590_change,
    expenses_f070,
    expenses_f090,
    outward_ceded_premium,
    premium_written,
    denominator_net_earned_premium
    
FROM final_kpi
ORDER BY report_month DESC, scenario ASC
