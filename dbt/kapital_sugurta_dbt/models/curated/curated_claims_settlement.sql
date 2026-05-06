{{ config(materialized='view') }}

/*
  Curated Claims Settlement
  ------------------------------------
  Calculates the efficiency of claim resolution.
  Settlement time is measured from event creation 
  to the final payout date.
*/

WITH event_viplati AS (
    SELECT
        s.ins_id as claim_event_id,
        s.anketa_id as policy_anketa_id,
        s.created_date as event_created_date,
        MAX(v.date_viplata) as final_payout_date
    FROM {{ source('raw', 'ins_sobitie_oracle') }} s
    JOIN {{ source('raw', 'ins_viplati_oracle') }} v ON v.ins_id = s.ins_id
    WHERE s.created_date IS NOT NULL
      AND v.date_viplata IS NOT NULL
      AND v.date_viplata >= s.created_date
    GROUP BY 1, 2, 3
)

SELECT
    ev.claim_event_id,
    ev.policy_anketa_id,
    ev.event_created_date,
    ev.final_payout_date,
    DATE_TRUNC('month', ev.final_payout_date)::DATE as report_month,
    
    -- Calculate duration in full days
    EXTRACT(EPOCH FROM (ev.final_payout_date - ev.event_created_date)) / 86400.0 as settlement_days,
    
    -- Product Dimensions
    CASE WHEN pt.mandatory = 1 THEN 'Mandatory' ELSE 'Voluntary' END as insurance_type,
    COALESCE(v_cat.name3, 'Other') as product_category,
    COALESCE(pt.polis_name, 'Other') as product_name

FROM event_viplati ev
JOIN {{ source('raw', 'ins_anketa_oracle') }} a ON a.ins_id = ev.policy_anketa_id
LEFT JOIN {{ source('raw', 'ins_pturi_oracle') }} pt ON pt.ins_id = a.ins_type
LEFT JOIN {{ source('raw', 'ins_vertical_oracle') }} v_cat ON v_cat.ins_id = pt.vertical
