{{ config(materialized='view') }}

WITH all_recoveries AS (
    -- Extract the base 'Total Amount' payments logic independent of specific regression matches
    SELECT 
        b.ins_id AS transaction_id,
        b.pym_date::DATE AS recovery_payment_date,
        COALESCE(b.pym_summa, 0)::NUMERIC AS total_recovery_amount
    FROM {{ source('raw', 'ins_bank_client_oracle') }} b
    WHERE b.bc_type = 2
),

exact_recoveries AS (
    -- Isolate strictly verified paths linked deeply through the registry
    SELECT 
        b.ins_id AS transaction_id,
        p.tb_anketa AS anketa_id, -- Explicitly passing anketa_id for bank linkage
        COALESCE(h.ins_name1, k.tb_name, 'Direct/No Partner') AS bank_partner_name,
        COALESCE(m.priority_area, 'Other') AS product_category,
        COALESCE(SUM(rb.recovery_sum), 0)::NUMERIC AS exact_recovery_amount,
        a.fizyur,
        
        -- Calculate processing time: Delta between subrogation debt creation and the actual payment
        AVG(EXTRACT(EPOCH FROM (b.pym_date - r.create_date)) / 86400)::NUMERIC AS recovery_processing_time_days
        
    FROM {{ source('raw', 'ins_bank_client_oracle') }} b
    INNER JOIN {{ source('raw', 'ins_regress_bank_oracle') }} rb ON rb.bc_id = b.ins_id
    LEFT JOIN {{ source('raw', 'ins_regress_oracle') }} r ON rb.regress_id = r.ins_id
    LEFT JOIN {{ source('raw', 'ins_polis_oracle') }} p ON r.polis_id = p.tb_id
    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a ON a.ins_id = p.tb_anketa
    LEFT JOIN {{ source('raw', 'ins_kontragent_oracle') }} k ON a.beneficiary = k.tb_id
    LEFT JOIN {{ source('raw', 'ins_headbanks_oracle') }} h ON k.head_id = h.ins_id
    LEFT JOIN {{ ref('curated_priority_area_mapping') }} m ON a.ins_type = m.pturi_id
    WHERE b.bc_type = 2
    GROUP BY 1, 2, 3, 4, a.fizyur
)

SELECT 
    a.transaction_id,
    e.anketa_id,
    a.recovery_payment_date,
    COALESCE(e.bank_partner_name, 'Direct/No Partner') AS bank_partner_name,
    COALESCE(e.product_category, 'Other') AS product_category,
    e.fizyur,
    a.total_recovery_amount,
    COALESCE(e.exact_recovery_amount, 0) AS exact_recovery_amount,
    -- Undefined Subrogation perfectly calculated on a transactional basis dynamically
    (a.total_recovery_amount - COALESCE(e.exact_recovery_amount, 0)) AS undefined_recovery_amount,
    COALESCE(e.recovery_processing_time_days, 0) AS recovery_processing_time_days
FROM all_recoveries a
LEFT JOIN exact_recoveries e ON a.transaction_id = e.transaction_id
WHERE a.recovery_payment_date IS NOT NULL
