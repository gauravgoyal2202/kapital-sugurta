WITH parsed AS (
    SELECT
        report_date,
        payload_json -> 'OperationResult' AS op
    FROM {{ source('raw', 'financial_performance_api_response') }}
)

SELECT
    report_date,
    (op ->> 'Result')::INT AS result,
    (op ->> 'F010')::NUMERIC AS f010,
    (op ->> 'F011')::NUMERIC AS f011
FROM parsed