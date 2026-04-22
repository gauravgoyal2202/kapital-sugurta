WITH claims_agg AS (
    SELECT
        DATE_TRUNC('month', payout_date) AS report_month,
        scenario,
        SUM(payout_total) AS total_claims
    FROM {{ ref('curated_claims_portfolio') }}
    WHERE payout_date IS NOT NULL
    GROUP BY 1, 2
),

bs_agg AS (
    SELECT
        DATE_TRUNC('month', report_date) AS report_month,
        scenario,
        SUM(other_long_term_assets_two) AS a410_cash_equivalents
    FROM {{ ref('curated_balance_sheet') }}
    WHERE report_date IS NOT NULL
    GROUP BY 1, 2
),

solvency_quarterly AS (
    SELECT
        EXTRACT(YEAR FROM report_date) AS report_year,
        EXTRACT(QUARTER FROM report_date) AS report_quarter,
        scenario,
        MAX(actual_solvency_margin_adequacy_ratio) AS actual_solvency_ratio,
        MAX(required_solvency_margin_adequacy_ratio) AS required_solvency_ratio
    FROM {{ ref('curated_solvency_adequacy_ratio') }}
    WHERE report_date IS NOT NULL
    GROUP BY 1, 2, 3
),

all_months AS (
    SELECT DISTINCT report_month, scenario FROM claims_agg
    UNION
    SELECT DISTINCT report_month, scenario FROM bs_agg
),

final_view AS (
    SELECT
        m.report_month,
        m.scenario,
        
        -- Drilling base data
        COALESCE(bs.a410_cash_equivalents, 0) AS cash_and_equivalents_a410,
        COALESCE(c.total_claims, 0) AS total_claims_incurred,
        
        -- Cash-to-Claims Ratio (Actual vs Required)
        CASE 
            WHEN COALESCE(c.total_claims, 0) = 0 THEN 0
            ELSE COALESCE(bs.a410_cash_equivalents, 0) / c.total_claims
        END AS cash_to_claims_ratio_actual,
        
        2.5 AS cash_to_claims_ratio_required,
        
        -- Solvency Margin Adequacy Ratio (Quarterly Exploded to Monthly)
        COALESCE(sq.actual_solvency_ratio, 0) AS solvency_margin_adequacy_ratio_actual,
        COALESCE(sq.required_solvency_ratio, 0) AS solvency_margin_adequacy_ratio_required

    FROM all_months m
    LEFT JOIN claims_agg c 
        ON m.report_month = c.report_month AND m.scenario = c.scenario
    LEFT JOIN bs_agg bs 
        ON m.report_month = bs.report_month AND m.scenario = bs.scenario
    LEFT JOIN solvency_quarterly sq 
        ON EXTRACT(YEAR FROM m.report_month) = sq.report_year 
        AND EXTRACT(QUARTER FROM m.report_month) = sq.report_quarter
        AND m.scenario = sq.scenario
)

SELECT * 
FROM final_view
ORDER BY report_month DESC, scenario ASC