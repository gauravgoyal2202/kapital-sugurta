WITH parsed AS (
    SELECT
        report_date,
        payload_json -> 'OperationResult' -> 'Balance' AS bal
    FROM {{ source('raw', 'balance_sheet_api_response') }}
)

SELECT
    report_date,
    'Actual' as scenario,

    -- =========================
    -- ASSETS (A codes)
    -- =========================
    (bal ->> 'A010')::NUMERIC AS total_assets,
    (bal ->> 'A011')::NUMERIC AS current_assets,
    (bal ->> 'A012')::NUMERIC AS non_current_assets,

    (bal ->> 'A020')::NUMERIC AS cash_and_equivalents,
    (bal ->> 'A021')::NUMERIC AS cash,
    (bal ->> 'A022')::NUMERIC AS bank_accounts,

    (bal ->> 'A030')::NUMERIC AS investments,
    (bal ->> 'A040')::NUMERIC AS financial_assets,

    (bal ->> 'A100')::NUMERIC AS receivables,

    (bal ->> 'A130')::NUMERIC AS total_assets_check,

    (bal ->> 'A190')::NUMERIC AS total_assets_alt,
    (bal ->> 'A200')::NUMERIC AS current_assets_total,

    (bal ->> 'A240')::NUMERIC AS loans_issued,
    (bal ->> 'A250')::NUMERIC AS long_term_investments,

    (bal ->> 'A380')::NUMERIC AS reserves_assets,
    (bal ->> 'A390')::NUMERIC AS accumulated_assets,

    (bal ->> 'A430')::NUMERIC AS fixed_assets,

    (bal ->> 'A460')::NUMERIC AS total_operating_assets,

    (bal ->> 'A480')::NUMERIC AS total_balance_assets,
    (bal ->> 'A490')::NUMERIC AS total_assets_final,

    -- =========================
    -- LIABILITIES (P codes)
    -- =========================
    (bal ->> 'P500')::NUMERIC AS share_capital,
    (bal ->> 'P510')::NUMERIC AS additional_capital,

    (bal ->> 'P520')::NUMERIC AS retained_earnings_prev,

    (bal ->> 'P570')::NUMERIC AS equity,
    (bal ->> 'P580')::NUMERIC AS total_equity,

    (bal ->> 'P590')::NUMERIC AS total_liabilities_equity,

    (bal ->> 'P600')::NUMERIC AS reserves,

    (bal ->> 'P630')::NUMERIC AS accounts_payable,

    (bal ->> 'P670')::NUMERIC AS total_liabilities,

    (bal ->> 'P680')::NUMERIC AS short_term_liabilities,

    (bal ->> 'P700')::NUMERIC AS long_term_liabilities,

    (bal ->> 'P720')::NUMERIC AS retained_earnings,

    (bal ->> 'P900')::NUMERIC AS profit_loss,

    (bal ->> 'P930')::NUMERIC AS equity_total_check,
    (bal ->> 'P931')::NUMERIC AS equity_subtotal,

    (bal ->> 'P950')::NUMERIC AS reserves_additional,
    (bal ->> 'P1000')::NUMERIC AS capital_total,
    (bal ->> 'P1090')::NUMERIC AS liabilities_total_alt,
    (bal ->> 'P1190')::NUMERIC AS total_equity_final,
    (bal ->> 'P1200')::NUMERIC AS total_balance_liabilities
FROM parsed