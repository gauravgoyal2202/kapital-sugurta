{{ config(
    materialized = 'table',
    tags         = ['financial', 'ratios', 'monthly', 'fm_plan']
) }}

/*
  Financial Ratios — Monthly (Mart)
  ---------------------------------
  Actual: regulatory financial performance, balance sheet, claims, reinsurance.
  Plan:   FM Excel consolidated block (Jul 2025 – Dec 2028), scenario = 'Plan'.

  Plan notes:
    • FM claims line combines payout + commission (same as profitability plan)
    • No P590 / P610 reserve movement in FM consolidated block → 0
    • Plan expense ratio uses period_expenses / net earned premium (no claims
      subtraction — FM operating expenses are separate from the claims line)
*/

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
        DATE_TRUNC('month', premium_accrual_date)::DATE AS report_month,
        scenario,
        SUM(total_accrued_premium_uzs) AS outward_ceded_premium
    FROM {{ ref('curated_reinsurance_outgoing_portfolio') }}
    WHERE premium_accrual_date IS NOT NULL
    GROUP BY 1, 2
),

premium_written_agg AS (
    SELECT
        DATE_TRUNC('month', payment_date)::DATE AS report_month,
        'Actual'::TEXT AS scenario,
        SUM(premium_amount) AS premium_written
    FROM {{ ref('curated_insurance_premium') }}
    WHERE payment_date IS NOT NULL
    GROUP BY 1, 2
),

balance_sheet_monthly AS (
    SELECT
        DATE_TRUNC('month', report_date)::DATE AS report_month,
        scenario,
        SUM(rbns_reserve) AS p610_reserve,
        SUM(unearned_premium_reserve) AS p590_end_of_period,
        SUM(unearned_premium_reserve)
            - LAG(SUM(unearned_premium_reserve), 1, 0) OVER (
                PARTITION BY scenario
                ORDER BY DATE_TRUNC('month', report_date)::DATE
            ) AS p590_change
    FROM {{ ref('curated_balance_sheet') }}
    WHERE report_date IS NOT NULL
    GROUP BY 1, 2
),

financial_perf_monthly_base AS (
    SELECT
        DATE_TRUNC('month', report_date)::DATE AS report_month,
        scenario,
        SUM(premium_income) AS premium_f011,
        SUM(other_premium_income) AS premium_f013,
        SUM(reinsurance_premium_ceded) AS reinsurance_f012,
        SUM(costs_of_goods_sold) AS expenses_f070,
        SUM(period_expenses) AS expenses_f090
    FROM {{ ref('curated_financial_performance') }}
    WHERE report_date IS NOT NULL
    GROUP BY 1, 2
),

financial_perf_monthly AS (
    SELECT
        report_month,
        scenario,
        premium_f011
            - COALESCE(
                LAG(premium_f011) OVER (
                    PARTITION BY scenario, EXTRACT(YEAR FROM report_month)
                    ORDER BY report_month
                ),
                0
            ) AS premium_f011,
        premium_f013
            - COALESCE(
                LAG(premium_f013) OVER (
                    PARTITION BY scenario, EXTRACT(YEAR FROM report_month)
                    ORDER BY report_month
                ),
                0
            ) AS premium_f013,
        reinsurance_f012
            - COALESCE(
                LAG(reinsurance_f012) OVER (
                    PARTITION BY scenario, EXTRACT(YEAR FROM report_month)
                    ORDER BY report_month
                ),
                0
            ) AS reinsurance_f012,
        expenses_f070
            - COALESCE(
                LAG(expenses_f070) OVER (
                    PARTITION BY scenario, EXTRACT(YEAR FROM report_month)
                    ORDER BY report_month
                ),
                0
            ) AS expenses_f070,
        expenses_f090
            - COALESCE(
                LAG(expenses_f090) OVER (
                    PARTITION BY scenario, EXTRACT(YEAR FROM report_month)
                    ORDER BY report_month
                ),
                0
            ) AS expenses_f090
    FROM financial_perf_monthly_base
),

actual_months AS (
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

actual_kpi AS (
    SELECT
        m.report_month,
        m.scenario,

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

        (
            COALESCE(fp.premium_f011, 0)
            + COALESCE(fp.premium_f013, 0)
            - COALESCE(fp.reinsurance_f012, 0)
        ) - COALESCE(bs.p590_change, 0) AS denominator_net_earned_premium

    FROM actual_months m
    LEFT JOIN claims_agg c
        ON m.report_month = c.report_month
       AND m.scenario = c.scenario
    LEFT JOIN balance_sheet_monthly bs
        ON m.report_month = bs.report_month
       AND m.scenario = bs.scenario
    LEFT JOIN financial_perf_monthly fp
        ON m.report_month = fp.report_month
       AND m.scenario = fp.scenario
    LEFT JOIN reinsurance_out_agg ro
        ON m.report_month = ro.report_month
       AND m.scenario = ro.scenario
    LEFT JOIN premium_written_agg pw
        ON m.report_month = pw.report_month
       AND m.scenario = pw.scenario
    WHERE m.scenario = 'Actual'
),

plan_kpi AS (
    SELECT
        i.month_start_date                                                      AS report_month,
        i.scenario,

        i.paid_amount_uzs                                                       AS claims_payout,
        0::NUMERIC                                                              AS p610_reserve,
        i.earned_premium_uzs                                                    AS premium_f011,
        0::NUMERIC                                                              AS premium_f013,
        i.ceded_earned_reinsurance_premium_uzs                                  AS reinsurance_f012,
        0::NUMERIC                                                              AS p590_change,
        0::NUMERIC                                                              AS expenses_f070,
        COALESCE(f.period_expenses_uzs, 0)                                      AS expenses_f090,
        i.ceded_earned_reinsurance_premium_uzs                                  AS outward_ceded_premium,
        i.gross_direct_premium_uzs                                              AS premium_written,

        i.earned_premium_uzs - i.ceded_earned_reinsurance_premium_uzs           AS denominator_net_earned_premium

    FROM {{ ref('curated_fm_plan_insurance_monthly') }} i
    LEFT JOIN {{ ref('curated_fm_plan_financial_monthly') }} f
        ON f.month_start_date = i.month_start_date
       AND f.scenario = i.scenario
),

final_kpi AS (
    SELECT * FROM actual_kpi
    UNION ALL
    SELECT * FROM plan_kpi
)

SELECT
    report_month,
    scenario,

    CASE
        WHEN denominator_net_earned_premium = 0 THEN 0
        ELSE claims_payout / denominator_net_earned_premium * 100.0
    END AS loss_ratio_pct,

    CASE
        WHEN denominator_net_earned_premium = 0 THEN 0
        WHEN scenario = 'Plan'
        THEN expenses_f090 / denominator_net_earned_premium * 100.0
        ELSE (expenses_f070 + expenses_f090 - claims_payout) / denominator_net_earned_premium * 100.0
    END AS expense_ratio_pct,

    (
        CASE
            WHEN denominator_net_earned_premium = 0 THEN 0
            ELSE claims_payout / denominator_net_earned_premium * 100.0
        END
        +
        CASE
            WHEN denominator_net_earned_premium = 0 THEN 0
            WHEN scenario = 'Plan'
            THEN expenses_f090 / denominator_net_earned_premium * 100.0
            ELSE (expenses_f070 + expenses_f090 - claims_payout) / denominator_net_earned_premium * 100.0
        END
    ) AS combined_ratio_pct,

    CASE
        WHEN premium_written = 0 THEN 0
        ELSE outward_ceded_premium / premium_written * 100.0
    END AS reinsurance_level_pct,

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
