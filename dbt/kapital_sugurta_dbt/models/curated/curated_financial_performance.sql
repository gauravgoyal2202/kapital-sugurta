WITH parsed AS (
    SELECT
        report_date,
        payload_json -> 'OperationResult' AS op
    FROM {{ source('raw', 'financial_performance_api_response') }}
)
SELECT
    report_date,
    'Actual' as scenario,
    (op ->> 'F320')::NUMERIC AS net_profit
FROM parsed