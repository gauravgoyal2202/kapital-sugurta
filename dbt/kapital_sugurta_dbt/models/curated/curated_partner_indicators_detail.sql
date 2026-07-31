{{ config(materialized='table') }}

/*
  Partner indicators at user grain for partner name enrichment in presentation marts.
  Source: curated_partner_payment_base (inline fifty / kurs, no per-row functions).
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
        pb.user_id,
        pb.channels,
        pb.insurance_type,
        pb.product_category,
        pb.product_name,
        pb.bank_name,
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
        pb.user_id,
        pb.channels,
        pb.insurance_type,
        pb.product_category,
        pb.product_name,
        pb.bank_name
)

SELECT
    month,
    user_id,
    channels,
    insurance_type,
    product_category,
    product_name,
    oplsum,
    kom_sum,
    claim_value,
    fifty,
    bank_name,
    ras_value
FROM aggregated
