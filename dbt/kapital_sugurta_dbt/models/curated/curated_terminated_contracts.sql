{{ config(materialized='view') }}

SELECT
    tb_dateras::DATE AS termination_date,
    COALESCE(tb_summa, 0)::NUMERIC AS terminated_amount,
    COALESCE(vernut, 0)::NUMERIC AS returned_amount,
    COALESCE(ostatok, 0)::NUMERIC AS remainder_amount,
    COALESCE(vozvrat_sum, 0)::NUMERIC AS returned_sum,
    COALESCE(retention, 0)::NUMERIC AS retention_amount
    
FROM {{ source('raw', 'ins_rastorg_oracle') }}
WHERE tb_dateras IS NOT NULL
