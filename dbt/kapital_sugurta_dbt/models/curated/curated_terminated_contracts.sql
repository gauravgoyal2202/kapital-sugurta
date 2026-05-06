{{ config(materialized='view') }}

/*
  Terminated Contracts (Curated)
  --------------------------------
  Enriched with product and channel join keys so downstream presentation
  marts can break down terminations by insurance_type, category, product,
  and sales channel.

  Join path:
    ins_rastorg_oracle.tb_polis → ins_polis_oracle.tb_id
      → pturi_id  (product key for curated_product_dimension)
      → tb_anketa (anketa_id key for curated_channel_partner_dimension)
*/

SELECT
    r.tb_dateras::DATE                      AS termination_date,
    COALESCE(r.tb_summa,    0)::NUMERIC     AS terminated_amount,
    COALESCE(r.vernut,      0)::NUMERIC     AS returned_amount,
    COALESCE(r.ostatok,     0)::NUMERIC     AS remainder_amount,
    COALESCE(r.vozvrat_sum, 0)::NUMERIC     AS returned_sum,
    COALESCE(r.retention,   0)::NUMERIC     AS retention_amount,

    -- Product key (joins to curated_product_dimension)
    p.pturi_id,

    -- Anketa key (joins to curated_channel_partner_dimension)
    p.tb_anketa                             AS anketa_id

FROM {{ source('raw', 'ins_rastorg_oracle') }} r
LEFT JOIN {{ source('raw', 'ins_polis_oracle') }} p
    ON p.tb_id = r.tb_polis
WHERE r.tb_dateras IS NOT NULL
