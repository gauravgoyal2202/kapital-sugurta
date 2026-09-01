/*
  Financial KPIs — ROA / ROE
  --------------------------
  Actual and Plan both store ROA/ROE as percentage points (e.g. 3.44 = 3.44%),
  not decimals (0.0344). Power BI should use the same number format for both
  scenarios (no extra ×100 on one series only).
*/

SELECT
    fp.report_date,
    fp.net_profit,
    COALESCE(bs.total_assets_final, 0)                                              AS average_assets_a490,
    COALESCE(bs.equity, 0) + COALESCE(bs.retained_earnings, 0)                    AS average_equity,
    ROUND(
        fp.net_profit / NULLIF(bs.total_assets_final, 0) * 100,
        4
    )                                                                               AS roa,
    ROUND(
        fp.net_profit / NULLIF(COALESCE(bs.equity, 0) + COALESCE(bs.retained_earnings, 0), 0) * 100,
        4
    )                                                                               AS roe,
    fp.scenario                                                                     AS scenario
FROM {{ ref('curated_financial_performance') }} fp
JOIN {{ ref('curated_balance_sheet') }} bs
    ON fp.report_date = bs.report_date
   AND fp.scenario = bs.scenario

UNION ALL

SELECT
    report_month                                                                    AS report_date,
    net_profit_uzs                                                                  AS net_profit,
    average_assets_uzs                                                              AS average_assets_a490,
    average_equity_uzs                                                              AS average_equity,
    roa,
    roe,
    scenario
FROM {{ ref('curated_fm_plan_financial_monthly') }}

ORDER BY report_date DESC, scenario ASC
