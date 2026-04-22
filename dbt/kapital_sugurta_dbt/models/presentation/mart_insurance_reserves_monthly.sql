WITH monthly_bs AS (
    SELECT
        DATE_TRUNC('month', report_date) AS report_month,
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
)

SELECT
    report_month,
    scenario,
    
    -- Core KPIs
    upr,
    ibnr,
    rbns,
    
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

FROM monthly_bs
ORDER BY report_month DESC
