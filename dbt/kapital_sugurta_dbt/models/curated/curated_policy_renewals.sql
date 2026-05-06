{{ config(materialized='view') }}

/*
  Curated Policy Renewals
  ------------------------------------
  Ultra-Optimized version: Utilizes Window Functions to calculate renewals.
  A policy is "renewed" if the same client opens a new policy 
  for the same product type within 30 days of the expiration
  of the old policy, OR if explicitly linked via oldank_id.
*/

WITH active_policies AS (
    SELECT 
        po.tb_anketa as anketa_id,
        a.owner as client_id,
        a.ins_type as product_type_id,
        po.tb_date_begin as start_date,
        po.tb_date_end as end_date,
        a.oldank_id,
        po.tb_status,
        -- Product dimensions
        CASE WHEN pt.mandatory = 1 THEN 'Mandatory' ELSE 'Voluntary' END as insurance_type,
        COALESCE(v_cat.name3, 'Other') as product_category,
        COALESCE(pt.polis_name, 'Other') as product_name
    FROM {{ source('raw', 'ins_polis_oracle') }} po
    JOIN {{ source('raw', 'ins_anketa_oracle') }} a ON po.tb_anketa = a.ins_id
    LEFT JOIN {{ source('raw', 'ins_pturi_oracle') }} pt ON pt.ins_id = a.ins_type
    LEFT JOIN {{ source('raw', 'ins_vertical_oracle') }} v_cat ON v_cat.ins_id = pt.vertical
    WHERE po.tb_status IN (2, 9, 10)
      AND po.tb_date_end BETWEEN '2020-01-01' AND '2026-12-31'
),

-- Use Window Functions to find the very next policy's start date 
-- for the same client and same product type
policies_with_next_date AS (
    SELECT 
        *,
        LEAD(start_date) OVER (
            PARTITION BY client_id, product_type_id 
            ORDER BY start_date ASC
        ) as next_policy_start_date
    FROM active_policies
),

renewed_implicit AS (
    SELECT anketa_id as old_anketa_id
    FROM policies_with_next_date
    WHERE next_policy_start_date >= end_date
      AND next_policy_start_date <= end_date + INTERVAL '30 days'
),

renewed_explicit AS (
    SELECT e.anketa_id as old_anketa_id
    FROM active_policies e
    JOIN active_policies n ON n.oldank_id = e.anketa_id
),

unique_renewals AS (
    SELECT old_anketa_id FROM renewed_explicit
    UNION
    SELECT old_anketa_id FROM renewed_implicit
)

SELECT 
    e.anketa_id as old_anketa_id,
    e.end_date as expiration_date,
    DATE_TRUNC('month', e.end_date)::DATE as report_month,
    e.client_id,
    e.product_type_id,
    e.insurance_type,
    e.product_category,
    e.product_name,
    CASE WHEN r.old_anketa_id IS NOT NULL THEN 1 ELSE 0 END as is_renewed
FROM active_policies e
LEFT JOIN unique_renewals r ON r.old_anketa_id = e.anketa_id
WHERE e.end_date IS NOT NULL
