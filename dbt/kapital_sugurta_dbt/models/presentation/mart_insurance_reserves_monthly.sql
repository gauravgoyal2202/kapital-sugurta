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

period_flags AS (
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
        CASE WHEN report_month = MAX(report_month) OVER (PARTITION BY scenario, EXTRACT(year FROM report_month), EXTRACT(quarter FROM report_month)) THEN 1 ELSE 0 END AS is_qtr_end,
        CASE WHEN report_month = MAX(report_month) OVER (PARTITION BY scenario, EXTRACT(year FROM report_month)) THEN 1 ELSE 0 END AS is_year_end
    FROM monthly_bs
)

SELECT
    report_month,
    scenario,
    
    -- Core KPIs (Monthly Snapshot)
    upr,
    ibnr,
    rbns,
    
    -- Quarterly Aggregation Helpers (Only populated on the last available month of the quarter)
    -- Power BI can SUM these safely at the quarter level
    CASE WHEN is_qtr_end = 1 THEN upr ELSE 0 END AS upr_qtr,
    CASE WHEN is_qtr_end = 1 THEN ibnr ELSE 0 END AS ibnr_qtr,
    CASE WHEN is_qtr_end = 1 THEN rbns ELSE 0 END AS rbns_qtr,

    -- Yearly Aggregation Helpers (Only populated on the last available month of the year)
    -- Power BI can SUM these safely at the year level
    CASE WHEN is_year_end = 1 THEN upr ELSE 0 END AS upr_year,
    CASE WHEN is_year_end = 1 THEN ibnr ELSE 0 END AS ibnr_year,
    CASE WHEN is_year_end = 1 THEN rbns ELSE 0 END AS rbns_year,
    
    -- Stabilization Reserve = P630 + P650
    (upr + ibnr + rbns) AS calculated_p580_check, -- Added for audit, usually P580 is sum of these
    (COALESCE(stabilization_reserve_base, 0) + COALESCE(stabilization_reserve_additional, 0)) AS stabilization_reserve,
    
    -- Allocated Assets to Reserves % = A490 / P580
    CASE 
        WHEN COALESCE(total_reserves_denominator, 0) = 0 THEN 0
        ELSE (COALESCE(total_assets, 0) / COALESCE(total_reserves_denominator, 0)) * 100.0
    END AS allocated_assets_to_reserves_pct,
    
    -- Base values for drill-down
    total_assets,
    total_reserves_denominator

FROM period_flags
ORDER BY report_month DESC
