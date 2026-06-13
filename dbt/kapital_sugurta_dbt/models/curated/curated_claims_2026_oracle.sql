{{ config(materialized='view') }}

/*
  2026+ Claims Data from Oracle
  -----------------------------
  This model supplements the historical Excel-based claims data for periods 
  from 2026 onwards, using the client's validated SQL logic.
*/

SELECT
    vipl.ins_id::TEXT                                       AS record_id,
    COALESCE(div.sp_name1, 'Head Office')                   AS branch_name,
    COALESCE(prod.insurance_type, 'Voluntary')              AS insurance_type,
    COALESCE(prod.product_name, 'Unclassified')              AS insurance_class,
    s.decision_date::DATE                                   AS incident_date,
    s.decision_date::DATE                                   AS settlement_decision_date,
    s.decision_date::DATE                                   AS payout_date,
    vipl.decision_summa                                     AS payout_total,
    'Actual'                                                AS scenario,
    CURRENT_TIMESTAMP                                       AS updated_at
FROM {{ source('raw', 'ins_loss_oracle') }} vipl
LEFT JOIN {{ source('raw', 'ins_sobitie_oracle') }} s ON vipl.sobitie_id = s.ins_id
LEFT JOIN {{ source('raw', 'sp_division_oracle') }} div ON vipl.division_id = div.sp_id
LEFT JOIN {{ ref('curated_product_dimension') }} prod ON vipl.pturi_id = prod.pturi_id
WHERE s.decision_date >= '2021-01-01'
  AND vipl.decision_summa > 0
  AND COALESCE(s.active, 0) < 2
