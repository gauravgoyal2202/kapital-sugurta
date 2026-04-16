WITH source AS (
    SELECT
        report_date,
        payload_json -> 'OperationResult' AS op
    FROM {{ source('raw', 'investment_activity_api_response') }}
),

-- =========================
-- Bonds
-- =========================
bonds AS (
    SELECT
        report_date,
        'BOND' AS investment_type,
        b->>'partner' AS partner_name,
        b->>'contract' AS contract,
        (b->>'amount')::NUMERIC AS amount
    FROM source,
    LATERAL jsonb_array_elements(op -> 'Bonds') AS b
),

-- =========================
-- Deposits
-- =========================
deposits AS (
    SELECT
        report_date,
        'DEPOSIT' AS investment_type,
        d->>'partner' AS partner_name,
        d->>'contract' AS contract,
        (d->>'amount')::NUMERIC AS amount
    FROM source,
    LATERAL jsonb_array_elements(op -> 'Deposits') AS d
),

-- =========================
-- Foreign Currency Deposits
-- =========================
fx_deposits AS (
    SELECT
        report_date,
        'FX_DEPOSIT' AS investment_type,
        f->>'partner' AS partner_name,
        f->>'contract' AS contract,
        (f->>'amount')::NUMERIC AS amount
    FROM source,
    LATERAL jsonb_array_elements(op -> 'ForeignCurrencyDeposits') AS f
),

-- =========================
-- Loans (single value)
-- =========================
loans AS (
    SELECT
        report_date,
        'LOAN' AS investment_type,
        NULL AS partner_name,
        NULL AS contract,
        (op ->> 'Loans')::NUMERIC AS amount
    FROM source
),

-- =========================
-- Shares (single value)
-- =========================
shares AS (
    SELECT
        report_date,
        'SHARE' AS investment_type,
        NULL AS partner_name,
        NULL AS contract,
        (op ->> 'Shares')::NUMERIC AS amount
    FROM source
)

-- =========================
-- FINAL UNION
-- =========================
SELECT *,'Actual' as scenario FROM bonds
UNION ALL
SELECT *,'Actual' as scenario  FROM deposits
UNION ALL
SELECT *,'Actual' as scenario FROM fx_deposits
UNION ALL
SELECT *,'Actual' as scenario FROM loans
UNION ALL
SELECT *,'Actual' as scenario FROM shares