SELECT
    fp.report_date,
    fp.net_profit,

    -- Average Assets (A490)
    COALESCE(bs.total_assets_final, 0)                                              AS average_assets_a490,

    -- Average Equity = Equity (P570) + Insurance Reserve (P720)
    COALESCE(bs.equity, 0) + COALESCE(bs.retained_earnings, 0)                    AS average_equity,

    -- ROA = Net Profit / A490
    fp.net_profit / NULLIF(bs.total_assets_final, 0)                                AS roa,

    -- ROE = Net Profit / P570
    fp.net_profit / NULLIF(COALESCE(bs.equity, 0), 0) AS roe,

    fp.scenario                                                                     AS scenario

FROM {{ ref('curated_financial_performance') }} fp
JOIN {{ ref('curated_balance_sheet') }} bs
    ON fp.report_date = bs.report_date
   AND fp.scenario = bs.scenario