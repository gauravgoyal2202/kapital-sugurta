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

solvency_sparse AS (
    SELECT
        DATE_TRUNC('month', report_date) AS report_month,
        scenario,
        MAX(actual_solvency_margin_adequacy_ratio) AS actual_solvency_ratio,
        MAX(required_solvency_margin_adequacy_ratio) AS required_solvency_ratio
    FROM {{ ref('curated_solvency_adequacy_ratio') }}
    WHERE report_date IS NOT NULL
    GROUP BY 1, 2
),

all_months AS (
    SELECT DISTINCT report_month, scenario FROM claims_agg
    UNION
    SELECT DISTINCT report_month, scenario FROM bs_agg
    UNION
    SELECT DISTINCT report_month, scenario FROM {{ ref('curated_fm_plan_financial_monthly') }}
),

solvency_filled AS (
    SELECT
        m.report_month,
        m.scenario,
        s.actual_solvency_ratio,
        s.required_solvency_ratio,
        COUNT(s.actual_solvency_ratio) OVER (PARTITION BY m.scenario ORDER BY m.report_month) AS actual_grp,
        COUNT(s.required_solvency_ratio) OVER (PARTITION BY m.scenario ORDER BY m.report_month) AS required_grp
    FROM all_months m
    LEFT JOIN solvency_sparse s
        ON m.report_month = s.report_month
       AND m.scenario = s.scenario
),

solvency_final AS (
    SELECT
        report_month,
        scenario,
        FIRST_VALUE(actual_solvency_ratio) OVER (
            PARTITION BY scenario, actual_grp ORDER BY report_month
        ) AS actual_solvency_ratio,
        FIRST_VALUE(required_solvency_ratio) OVER (
            PARTITION BY scenario, required_grp ORDER BY report_month
        ) AS required_solvency_ratio
    FROM solvency_filled
),

actual_view AS (
    SELECT
        m.report_month,
        m.scenario,
        COALESCE(bs.a410_cash_equivalents, 0) AS cash_and_equivalents_a410,
        COALESCE(c.total_claims, 0) AS total_claims_incurred,
        CASE
            WHEN COALESCE(c.total_claims, 0) = 0 THEN 0
            ELSE (COALESCE(bs.a410_cash_equivalents, 0) / c.total_claims) * 100
        END AS cash_to_claims_ratio_actual,
        2.5 AS cash_to_claims_ratio_required,
        COALESCE(sq.actual_solvency_ratio, 0) AS solvency_margin_adequacy_ratio_actual,
        COALESCE(sq.required_solvency_ratio, 0) AS solvency_margin_adequacy_ratio_required
    FROM all_months m
    LEFT JOIN claims_agg c
        ON m.report_month = c.report_month
       AND m.scenario = c.scenario
    LEFT JOIN bs_agg bs
        ON m.report_month = bs.report_month
       AND m.scenario = bs.scenario
    LEFT JOIN solvency_final sq
        ON m.report_month = sq.report_month
       AND m.scenario = sq.scenario
    WHERE m.scenario = 'Actual'
),

plan_view AS (
    SELECT
        report_month,
        scenario,
        cash_and_equivalents_a410_uzs                                           AS cash_and_equivalents_a410,
        total_claims_uzs                                                        AS total_claims_incurred,
        cash_to_claims_ratio_actual,
        cash_to_claims_ratio_required,
        solvency_adequacy_ratio_actual                                          AS solvency_margin_adequacy_ratio_actual,
        solvency_adequacy_ratio_required                                        AS solvency_margin_adequacy_ratio_required
    FROM {{ ref('curated_fm_plan_financial_monthly') }}
)

SELECT * FROM actual_view
UNION ALL
SELECT * FROM plan_view
ORDER BY report_month DESC, scenario ASC
