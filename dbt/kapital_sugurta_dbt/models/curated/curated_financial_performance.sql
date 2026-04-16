WITH parsed AS (
    SELECT
        report_date,
        payload_json -> 'OperationResult' AS op
    FROM {{ source('raw', 'financial_performance_api_response') }}
)

SELECT
    report_date,
    'Actual' as scenario,

    -- =========================
    -- CORE FINANCIALS
    -- =========================
    (op ->> 'F010')::NUMERIC AS total_income,
    (op ->> 'F011')::NUMERIC AS total_expense,
    (op ->> 'F012')::NUMERIC AS gross_profit,
    (op ->> 'F013')::NUMERIC AS other_income,

    (op ->> 'F060')::NUMERIC AS total_revenue,
    (op ->> 'F070')::NUMERIC AS total_costs,

    (op ->> 'F110')::NUMERIC AS operating_income,
    (op ->> 'F120')::NUMERIC AS operating_expense,

    (op ->> 'F140')::NUMERIC AS financial_income,
    (op ->> 'F160')::NUMERIC AS financial_expense,

    -- =========================
    -- PROFIT (IMPORTANT)
    -- =========================
    (op ->> 'F320')::NUMERIC AS net_profit,

    -- =========================
    -- INSURANCE-SPECIFIC SPLIT
    -- =========================
    -- P = Profit side
    -- L = Loss side
    -- =========================

    (op ->> 'F080P')::NUMERIC AS premium_income,
    (op ->> 'F080L')::NUMERIC AS premium_loss,

    (op ->> 'F150P')::NUMERIC AS claims_paid,
    (op ->> 'F150L')::NUMERIC AS claims_loss,

    (op ->> 'F270P')::NUMERIC AS underwriting_profit,
    (op ->> 'F270L')::NUMERIC AS underwriting_loss,

    (op ->> 'F290P')::NUMERIC AS technical_result,
    (op ->> 'F290L')::NUMERIC AS technical_loss,

    -- =========================
    -- ADDITIONAL METRICS
    -- =========================
    (op ->> 'F200')::NUMERIC AS tax_expense,
    (op ->> 'F220')::NUMERIC AS other_expense,
    (op ->> 'F250')::NUMERIC AS administrative_costs

FROM parsed    