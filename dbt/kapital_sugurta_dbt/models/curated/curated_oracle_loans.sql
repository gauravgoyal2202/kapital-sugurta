{{ config(materialized='view') }}

WITH source_data AS (
    SELECT
        i.ins_id,
        "raw".kontragent_name(i.client_id) AS client_name,
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
        i.status
    FROM {{ source('raw', 'ins_invloan_oracle') }} i
)

SELECT 
    ins_id AS loan_id,
    client_name,
    loan_num AS loan_number,
    dog_num AS contract_number,
    dog_date AS contract_date,
    loandate_from AS loan_start_date,
    loandate_to AS loan_end_date,
    loan_day AS loan_duration_days,
    prof_rate AS interest_rate,
    loan_sum AS loan_amount,
    val_type_desc AS currency_type,
    val_vozvrat_desc AS return_currency_type,
    postup AS paid_amount,
    kvipl AS payable_amount,
    (kvipl - postup) AS remaining_balance,
    CASE 
        WHEN NULLIF(kvipl, 0) = 0 THEN 0
        ELSE (postup * 100.0) / kvipl 
    END AS repayment_progress_pct,
    CASE 
        WHEN status = 0 THEN 'Актив'
        WHEN status = 1 THEN 'Исполненный'
        WHEN status = 2 THEN 'Досрочное погашение'
        ELSE ''
    END AS loan_status,
    'Actual' AS scenario,
    CURRENT_TIMESTAMP AS updated_at
FROM source_data
