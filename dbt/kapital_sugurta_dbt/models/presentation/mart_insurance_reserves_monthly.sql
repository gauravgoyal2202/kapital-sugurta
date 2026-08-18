{{ config(
    materialized = 'table',
    tags         = ['insurance', 'reserves', 'monthly', 'fm_plan']
) }}

/*
  Insurance Reserves — Monthly (Mart)
  -----------------------------------
  Actual: curated_balance_sheet (regulatory P590 / P600 / P610 / P630 / P650).
  Plan:   FM Excel sheet 'ФО' reserve balances (Jul 2025 – Dec 2028).

  Plan mapping (FM row → mart column):
    311 UPR, 312 IBNR, 313 RBNS,
    314 preventive-measures reserve → stabilization_reserve_base,
    315 asset-mismatch reserve → stabilization_reserve_additional,
    316 total insurance reserves, 360 total assets.
*/

WITH monthly_bs AS (
    SELECT
        DATE_TRUNC('month', report_date)::DATE AS report_month,
        scenario,
        SUM(unearned_premium_reserve) AS upr,
        SUM(ibnr_reserve) AS ibnr,
        SUM(rbns_reserve) AS rbns,
        SUM(stabilization_reserve_base) AS stabilization_reserve_base,
        SUM(stabilization_reserve_additional) AS stabilization_reserve_additional,
        SUM(total_assets_final) AS total_assets,
        SUM(total_equity_reserves) AS total_reserves_denominator
    FROM {{ ref('curated_balance_sheet') }}
    WHERE report_date IS NOT NULL
    GROUP BY 1, 2
),

actual_flags AS (
    SELECT
        report_month,
        scenario,
        upr,
        ibnr,
        rbns,
        stabilization_reserve_base,
        stabilization_reserve_additional,
        total_assets,
        total_reserves_denominator,
        CASE
            WHEN report_month = MAX(report_month) OVER (
                PARTITION BY scenario, EXTRACT(YEAR FROM report_month), EXTRACT(QUARTER FROM report_month)
            ) THEN 1 ELSE 0
        END AS is_qtr_end,
        CASE
            WHEN report_month = MAX(report_month) OVER (
                PARTITION BY scenario, EXTRACT(YEAR FROM report_month)
            ) THEN 1 ELSE 0
        END AS is_year_end
    FROM monthly_bs
),

plan_flags AS (
    SELECT
        report_month,
        scenario,
        upr,
        ibnr,
        rbns,
        stabilization_reserve_base,
        stabilization_reserve_additional,
        total_assets,
        total_reserves_denominator,
        CASE
            WHEN report_month = MAX(report_month) OVER (
                PARTITION BY scenario, EXTRACT(YEAR FROM report_month), EXTRACT(QUARTER FROM report_month)
            ) THEN 1 ELSE 0
        END AS is_qtr_end,
        CASE
            WHEN report_month = MAX(report_month) OVER (
                PARTITION BY scenario, EXTRACT(YEAR FROM report_month)
            ) THEN 1 ELSE 0
        END AS is_year_end
    FROM {{ ref('curated_fm_plan_reserves_monthly') }}
),

period_flags AS (
    SELECT * FROM actual_flags
    UNION ALL
    SELECT * FROM plan_flags
)

SELECT
    report_month,
    scenario,

    upr,
    ibnr,
    rbns,

    CASE WHEN is_qtr_end = 1 THEN upr ELSE 0 END AS upr_qtr,
    CASE WHEN is_qtr_end = 1 THEN ibnr ELSE 0 END AS ibnr_qtr,
    CASE WHEN is_qtr_end = 1 THEN rbns ELSE 0 END AS rbns_qtr,

    CASE WHEN is_year_end = 1 THEN upr ELSE 0 END AS upr_year,
    CASE WHEN is_year_end = 1 THEN ibnr ELSE 0 END AS ibnr_year,
    CASE WHEN is_year_end = 1 THEN rbns ELSE 0 END AS rbns_year,

    (upr + ibnr + rbns) AS calculated_p580_check,
    (
        COALESCE(stabilization_reserve_base, 0)
        + COALESCE(stabilization_reserve_additional, 0)
    ) AS stabilization_reserve,

    CASE
        WHEN COALESCE(total_reserves_denominator, 0) = 0 THEN 0
        ELSE (COALESCE(total_assets, 0) / COALESCE(total_reserves_denominator, 0)) * 100.0
    END AS allocated_assets_to_reserves_pct,

    total_assets,
    total_reserves_denominator

FROM period_flags
ORDER BY report_month DESC, scenario ASC
