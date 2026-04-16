SELECT
    fp.report_date,
    fp.net_profit,
    -- ROA
    fp.net_profit / bs.total_assets_final AS roa,
    -- ROE
    fp.net_profit / (bs.equity + bs.retained_earnings) AS roe,
    fp.scenario as scenario

FROM {{ ref('curated_financial_performance') }} fp
JOIN {{ ref('curated_balance_sheet') }} bs
    ON fp.report_date = bs.report_date