WITH excel_source AS (
    SELECT * FROM {{ source('raw', 'reinsurance_incoming_portfolio') }}
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
        
        c.insurance_product_name,
        c.insurant_name,
        c.classes AS insurance_classes,
        c.insurance_sum,
        c.contract_number,
        c.contract_issue_date,
        
        div.sp_name1 AS division_name,
        div.sp_name2 AS division_name_uz,
        frm.name AS reinsurance_form_name,
        frm.name_uz AS reinsurance_form_name_uz,
        typ.name AS reinsurance_type_name,
        typ.name_uz AS reinsurance_type_name_uz,
        brk.name AS broker_name,
        cur.sp_name1 AS currency_name,
        cur.sp_name2 AS currency_name_uz
        
    FROM {{ source('raw', 'ins_reinsurance_oracle') }} r
    LEFT JOIN {{ source('raw', 'ins_reins_contract_oracle') }} c ON c.reinsurance_id = r.id
    LEFT JOIN {{ source('raw', 'sp_division_oracle') }} div ON div.sp_id = r.division_id
    LEFT JOIN {{ source('raw', 'sp_reinsurance_form_oracle') }} frm ON frm.id = r.reinsurance_form_id
    LEFT JOIN {{ source('raw', 'sp_reinsurance_type_oracle') }} typ ON typ.id = r.reinsurance_type_id
    LEFT JOIN {{ source('raw', 'sp_reinsurance_brokers_oracle') }} brk ON brk.id = r.broker_id
    LEFT JOIN {{ source('raw', 'p_sp_currency_oracle') }} cur ON cur.sp_id = r.currency_id
    WHERE r.direction IN (2, 3) -- Incoming (Foreign/Local)
),

combined AS (
    -- 1. Historical Excel Data
    SELECT
        id::TEXT,
        policyholder,
        insurance_type,
        voluntary_insurance_type,
        mandatory_insurance_type,
        contract_conclusion_date,
        actual_premium_uzs AS total_accrued_premium_uzs,
        'Excel' AS source_system
    FROM excel_source

    UNION ALL

    -- 2. New Oracle Data
    SELECT
        reinsurance_id::TEXT,
        insurant_name,
        insurance_product_name,
        insurance_classes,
        'N/A', -- Category split
        slip_date AS contract_conclusion_date,
        COALESCE(netto_accrual_premium, 0) AS total_accrued_premium_uzs,
        'Oracle' AS source_system
    FROM oracle_source
)

SELECT
    *,
    'Actual' AS scenario,
    CURRENT_TIMESTAMP AS loaded_at
FROM combined
