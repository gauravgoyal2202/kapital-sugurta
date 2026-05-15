{{ config(materialized='table') }}

WITH source_data AS (
    SELECT * FROM {{ source('raw', 'oracle_reinsurance_combined') }}
)

SELECT
    reinsurance_id,
    dog_id,
    division_id,
    TRIM(division_name) AS division_name,
    direction,
    TRIM(direction_name) AS direction_name,
    TRIM(slip_contract_number) AS slip_contract_number,
    slip_contract_issue_date::DATE AS slip_contract_issue_date,
    reinsurance_start_date::DATE AS reinsurance_start_date,
    reinsurance_end_date::DATE AS reinsurance_end_date,
    reinsurer_is_foreign,
    TRIM(reinsurer_is_foreign_name) AS reinsurer_is_foreign_name,
    reinsurer_org_id,
    reinsurer_foreign_org_id,
    reinsurant_ins_org_id,
    reinsurant_foreign_ins_org_id,
    TRIM(slip_reinsurer) AS slip_reinsurer,
    TRIM(slip_reinsurant) AS slip_reinsurant,
    TRIM(reinsurance_form_name) AS reinsurance_form_name,
    TRIM(reinsurance_type_name) AS reinsurance_type_name,
    TRIM(broker_name) AS broker_name,
    broker_commission,
    TRIM(slip_currency_name) AS slip_currency_name,
    exchange_rate,
    reinsurant_share,
    reinsurer_limit,
    brutto_ins_premium,
    commission,
    netto_ins_premium,
    brutto_accrual_premium,
    netto_accrual_premium,
    netto_accrual_date::DATE AS netto_accrual_date,
    paid_premium,
    premium_paid_date::DATE AS premium_paid_date,
    TRIM(created_user) AS created_user,
    created_date::TIMESTAMP AS created_date,
    slip_status,
    TRIM(fond_status_name) AS fond_status_name,
    
    -- Related Contract Info
    contract_id,
    contract_is_foreign,
    TRIM(contract_is_foreign_name) AS contract_is_foreign_name,
    insurance_sum,
    TRIM(contract_number) AS contract_number,
    contract_issue_date::DATE AS contract_issue_date,
    TRIM(insurance_product_name) AS insurance_product_name,
    TRIM(insurant_name) AS insurant_name,
    TRIM(classes) AS insurance_classes,
    TRIM(contract_reinsurer) AS contract_reinsurer,
    contract_start_date::DATE AS contract_start_date,
    contract_end_date::DATE AS contract_end_date,
    TRIM(contract_currency_name) AS contract_currency_name,
    contract_exchange_rate,

    -- Calculated Fields
    CASE 
        WHEN direction = 1 THEN 'Outgoing'
        ELSE 'Incoming'
    END AS reinsurance_category,
    
    -- Currency Conversion (Example: if exchange_rate is provided)
    (COALESCE(brutto_ins_premium, 0) * COALESCE(exchange_rate, 1)) AS brutto_ins_premium_uzs,
    (COALESCE(netto_ins_premium, 0) * COALESCE(exchange_rate, 1)) AS netto_ins_premium_uzs,
    (COALESCE(commission, 0) * COALESCE(exchange_rate, 1)) AS commission_uzs,
    
    etl_loaded_at AS loaded_at

FROM source_data
