{{ config(materialized='view') }}

WITH source_data AS (
    SELECT
        i.ins_id,
        knt.tb_orgname AS client_name,
        i.loan_num,
        i.dog_num,
        i.dog_date,
        i.loandate_from,
        i.loandate_to,
        i.loan_day,
        i.prof_rate,
        i.loan_sum,
        "raw".f_ins_valtype(i.val_type) AS val_type_desc,
        "raw".f_ins_valtype(i.val_vozvrat) AS val_vozvrat_desc,
        "raw".f_ins_realloan(i.ins_id) AS postup,
        "raw".f_ins_viplloan(i.ins_id) AS kvipl,
        i.status,
        i.close_date
    FROM {{ source('raw', 'ins_invloan_oracle') }} i
    LEFT JOIN {{ source('raw', 'ins_kontragent_oracle') }} knt
           ON i.client_id = knt.tb_id
)

SELECT 
    s.ins_id AS loan_id,
    COALESCE(m.standardized_partner_name, s.client_name) AS client_name,
    s.loan_num AS loan_number,
    s.dog_num AS contract_number,
    s.dog_date AS contract_date,
    s.loandate_from AS loan_start_date,
    s.loandate_to AS loan_end_date,
    s.loan_day AS loan_duration_days,
    s.prof_rate AS interest_rate,
    s.loan_sum AS loan_amount,
    s.val_type_desc AS currency_type,
    s.val_vozvrat_desc AS return_currency_type,
    s.postup AS paid_amount,
    s.kvipl AS payable_amount,
    (s.kvipl - s.postup) AS remaining_balance,
    CASE 
        WHEN NULLIF(s.kvipl, 0) = 0 THEN 0
        ELSE (s.postup * 100.0) / s.kvipl 
    END AS repayment_progress_pct,
    CASE 
        WHEN s.status = 0 THEN 'Актив'
        WHEN s.status = 1 THEN 'Исполненный'
        WHEN s.status = 2 THEN 'Досрочное погашение'
        ELSE ''
    END AS loan_status,
    s.close_date AS loan_end_date_actual,
    'Actual' AS scenario,
    CURRENT_TIMESTAMP AS updated_at
FROM source_data s
LEFT JOIN {{ ref('partner_mapping') }} m
    ON REGEXP_REPLACE(UPPER(s.client_name), '[^[:alnum:]]', '', 'g') = REGEXP_REPLACE(UPPER(m.raw_partner_name), '[^[:alnum:]]', '', 'g')
