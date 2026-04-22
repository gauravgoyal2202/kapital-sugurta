WITH source AS (
    SELECT
        NULLIF(TRIM(report_date::text), '')::DATE AS report_date,
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
        NULLIF(b->>'amount', '')::NUMERIC AS amount
    FROM source,
    LATERAL jsonb_array_elements(
        CASE 
            WHEN jsonb_typeof(op -> 'Bonds') = 'array' THEN op -> 'Bonds' 
            ELSE '[]'::jsonb 
        END
    ) AS b
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
        NULLIF(d->>'amount', '')::NUMERIC AS amount
    FROM source,
    LATERAL jsonb_array_elements(
        CASE 
            WHEN jsonb_typeof(op -> 'Deposits') = 'array' THEN op -> 'Deposits' 
            ELSE '[]'::jsonb 
        END
    ) AS d
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
        NULLIF(f->>'amount', '')::NUMERIC AS amount
    FROM source,
    LATERAL jsonb_array_elements(
        CASE 
            WHEN jsonb_typeof(op -> 'ForeignCurrencyDeposits') = 'array' THEN op -> 'ForeignCurrencyDeposits' 
            ELSE '[]'::jsonb 
        END
    ) AS f
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
        NULLIF(op ->> 'Loans', '')::NUMERIC AS amount
    FROM source
    WHERE NULLIF(op ->> 'Loans', '') IS NOT NULL
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
        NULLIF(op ->> 'Shares', '')::NUMERIC AS amount
    FROM source
    WHERE NULLIF(op ->> 'Shares', '') IS NOT NULL
)

-- =========================
-- FINAL UNION
-- =========================
SELECT *, 'Actual' AS scenario, CURRENT_TIMESTAMP AS loaded_at, CURRENT_TIMESTAMP AS updated_at FROM bonds
UNION ALL
SELECT *, 'Actual' AS scenario, CURRENT_TIMESTAMP AS loaded_at, CURRENT_TIMESTAMP AS updated_at FROM deposits
UNION ALL
SELECT *, 'Actual' AS scenario, CURRENT_TIMESTAMP AS loaded_at, CURRENT_TIMESTAMP AS updated_at FROM fx_deposits
UNION ALL
SELECT *, 'Actual' AS scenario, CURRENT_TIMESTAMP AS loaded_at, CURRENT_TIMESTAMP AS updated_at FROM loans
UNION ALL
SELECT *, 'Actual' AS scenario, CURRENT_TIMESTAMP AS loaded_at, CURRENT_TIMESTAMP AS updated_at FROM shares