{{ config(materialized='view') }}

/*
  Curated Claims Settlement
  ------------------------------------
  Calculates the efficiency of claim resolution.
  Aligned with Oracle Logic:
  - Measures days as DATE(final_payout) - DATE(event_created)
  - Groups by the month of the FINAL payout.
*/

WITH event_viplati AS (
    SELECT
        s.ins_id as claim_event_id,
        s.anketa_id as policy_anketa_id,
        s.created_date::DATE as event_created_date,
        MAX(v.date_viplata)::DATE as final_payout_date
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

    (ev.final_payout_date - ev.event_created_date)::INT as settlement_days,

    COALESCE(pd.insurance_type, 'Voluntary')              AS insurance_type,
    COALESCE(pd.insurance_type_ru, 'Добровольное')        AS insurance_type_ru,
    COALESCE(pd.insurance_type_uz_cyrl, 'Ихтиёрий')       AS insurance_type_uz_cyrl,
    COALESCE(pd.insurance_type_uz_latn, 'Ixtiyoriy')      AS insurance_type_uz_latn,

    COALESCE(pd.category, 'Other')                          AS product_category,
    COALESCE(pd.category_ru, 'Other')                       AS product_category_ru,
    COALESCE(pd.category_uz, 'Other')                       AS product_category_uz,
    COALESCE(pd.category_uz_latn, 'Other')                  AS product_category_uz_latn,

    COALESCE(pd.product_name, 'Other')                      AS product_name,
    COALESCE(pd.product_name_ru, 'Other')                   AS product_name_ru,
    COALESCE(pd.product_name_uz, 'Other')                   AS product_name_uz,
    COALESCE(pd.product_name_uz_latn, 'Other')              AS product_name_uz_latn

FROM event_viplati ev
LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a ON a.ins_id = ev.policy_anketa_id
LEFT JOIN {{ ref('curated_product_dimension') }} pd ON pd.pturi_id = a.ins_type
