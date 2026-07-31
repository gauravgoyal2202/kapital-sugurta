{{ config(materialized='table') }}

/*
  Partner Performance & Profitability (Curated)
  Aggregates pre-computed payment rows from curated_partner_payment_base.
  Fifty / kurs are resolved once in the base table (no per-row PL/pgSQL).
*/

WITH claims_by_anketa AS (
    SELECT
        anketa_id,
        SUM(COALESCE(decision_summa, 0)) AS claim_value
    FROM {{ source('raw', 'ins_loss_oracle') }}
    WHERE anketa_id IS NOT NULL
    GROUP BY anketa_id
),

rastorg AS (
    SELECT
        tb_anketa AS anketa_id,
        tb_polis  AS polis_id,
        SUM(COALESCE(tb_summa, 0)) AS tb_summa
    FROM {{ source('raw', 'ins_rastorg_oracle') }}
    WHERE tb_anketa IS NOT NULL
      AND tb_dateras IS NOT NULL
      AND COALESCE(tb_summa, 0) <> 0
    GROUP BY tb_anketa, tb_polis
),

aggregated AS (
    SELECT
        DATE_TRUNC('month', pb.pym_date)::DATE                            AS month,
        pb.channels,
        pb.insurance_type,
        pb.product_category,
        pb.product_name,
        SUM(pb.oplsum)                                                    AS oplsum,
        SUM(pb.kom_sum)                                                   AS kom_sum,
        SUM(COALESCE(cla.claim_value, 0))                                 AS claim_value,
        SUM(pb.fifty_total)                                               AS fifty,
        SUM(COALESCE(r.tb_summa, 0))                                      AS ras_value
    FROM {{ ref('curated_partner_payment_base') }} pb
    LEFT JOIN claims_by_anketa cla
        ON cla.anketa_id = pb.anketa_id
    LEFT JOIN rastorg r
        ON r.anketa_id = pb.anketa_id
       AND (r.polis_id = pb.polis_id OR r.polis_id IS NULL)
    GROUP BY
        DATE_TRUNC('month', pb.pym_date)::DATE,
        pb.channels,
        pb.insurance_type,
        pb.product_category,
        pb.product_name
)

SELECT
    month,
    channels,
    insurance_type,
    product_category,
    product_name,
    oplsum,
    kom_sum,
    claim_value,
    fifty,
    ras_value
FROM aggregated
