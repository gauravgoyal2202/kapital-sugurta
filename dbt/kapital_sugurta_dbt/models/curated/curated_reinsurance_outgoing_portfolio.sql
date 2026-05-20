WITH excel_source AS (
    SELECT * FROM {{ source('raw', 'reinsurance_outgoing_portfolio') }}
    WHERE EXTRACT(YEAR FROM premium_accrual_date) < 2026
),

oracle_source AS (
    SELECT
        r.id AS reinsurance_id,
        r.direction,
        r.contract_number AS slip_contract_number,
        r.contract_issue_date AS slip_contract_issue_date,
        r.slip_date, -- Added missing column
        r.reinsurance_start_date,
        r.reinsurance_end_date,
        r.reinsurer_is_foreign,
        r.brutto_ins_premium,
        r.exchange_rate,
        r.commission,
        r.netto_ins_premium,
        r.brutto_accrual_premium,
        r.netto_accrual_premium, -- Added for validation
        r.broker_commission,
        r.reinsurant_share,
        r.reinsurer_limit,
        
        c.insurance_product_name,
        c.insurant_name,
        c.classes AS insurance_classes,
        c.insurance_sum,
        c.contract_number,
        c.contract_issue_date,
        
        div.sp_name1 AS division_name,
        frm.name AS reinsurance_form_name,
        typ.name AS reinsurance_type_name,
        brk.name AS broker_name,
        cur.sp_name1 AS currency_name
        
    FROM {{ source('raw', 'ins_reinsurance_oracle') }} r
    LEFT JOIN {{ source('raw', 'ins_reins_contract_oracle') }} c ON c.reinsurance_id = r.id
    LEFT JOIN {{ source('raw', 'sp_division_oracle') }} div ON div.sp_id = r.division_id
    LEFT JOIN {{ source('raw', 'sp_reinsurance_form_oracle') }} frm ON frm.id = r.reinsurance_form_id
    LEFT JOIN {{ source('raw', 'sp_reinsurance_type_oracle') }} typ ON typ.id = r.reinsurance_type_id
    LEFT JOIN {{ source('raw', 'sp_reinsurance_brokers_oracle') }} brk ON brk.id = r.broker_id
    LEFT JOIN {{ source('raw', 'p_sp_currency_oracle') }} cur ON cur.sp_id = r.currency_id
    WHERE r.direction = 1 -- Outgoing
      AND EXTRACT(YEAR FROM r.slip_date) >= 2026
),

combined AS (
    -- 1. Historical Excel Data
    SELECT
        id::TEXT,
        insurance_contract_number, -- Added for downstream models
        policyholder,
        insurance_type,
        voluntary_insurance_type,
        mandatory_insurance_type,
        premium_accrual_date,      -- REPLACED contract_conclusion_date
        total_accrued_premium_uzs,
        total_premiums_ceded_uzs,  -- Added for downstream models
        reinsurer,
        reinsurance_broker,
        reinsurance_type,
        'Excel' AS source_system
    FROM excel_source

    UNION ALL

    -- 2. New Oracle Data
    SELECT
        reinsurance_id::TEXT,
        contract_number, -- Mapped to insurance_contract_number
        insurant_name,
        insurance_product_name,
        insurance_classes,
        'N/A', -- Category split not direct in Oracle query
        slip_date AS premium_accrual_date, -- REPLACED contract_conclusion_date mapping
        COALESCE(netto_accrual_premium, 0) AS total_accrued_premium_uzs,
        COALESCE(netto_accrual_premium, 0) AS total_premiums_ceded_uzs, -- Assuming same for outgoing
        contract_reinsurer_name,
        broker_name,
        reinsurance_type_name,
        'Oracle' AS source_system
    FROM (
        -- Inner select to handle the complex contract_reinsurer logic if needed
        SELECT 
            *,
            NULL AS contract_reinsurer_name 
        FROM oracle_source
    ) os
)

SELECT
    *,
    'Actual' AS scenario,
    CURRENT_TIMESTAMP AS loaded_at
FROM combined
