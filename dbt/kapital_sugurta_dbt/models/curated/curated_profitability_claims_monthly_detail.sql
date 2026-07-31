{{ config(
    materialized = 'table',
    tags         = ['profitability', 'dashboard', 'claims'],
    post_hook    = [
        "CREATE INDEX IF NOT EXISTS idx_cpcamd_month   ON {{ this }} (report_month)",
        "CREATE INDEX IF NOT EXISTS idx_cpcamd_branch  ON {{ this }} (branch_name)",
        "CREATE INDEX IF NOT EXISTS idx_cpcamd_product ON {{ this }} (product_name)"
    ]
) }}

/*
  Actuarial claims metrics (monthly) by branch and product.
  Same filter logic as curated_profitability_claims_monthly.
*/

WITH claims_params AS (
    SELECT
        '3'::TEXT           AS p1_tb_direktor_filial,
        0::NUMERIC          AS p1_div,
        '1'::TEXT           AS p240_sstat,
        NULL::NUMERIC       AS p1_userid
),

claims_base AS (
    SELECT
        s.ins_id                                                            AS sobitie_id,
        s.sluch_date::DATE                                                  AS sluch_date,
        COALESCE(div.sp_name1, 'Head Office')                               AS branch_name,
        COALESCE(div.sp_name2, 'Head Office')                               AS branch_name_uz,
        COALESCE(pd.product_name, 'Other')                                AS product_name,
        COALESCE(pd.product_name_uz, 'Other')                             AS product_name_uz,
        COALESCE(pd.insurance_type, 'Voluntary')                          AS insurance_type,
        COALESCE(pd.category, 'Unclassified')                             AS product_category,
        COALESCE(pd.category_uz, 'Unclassified')                          AS product_category_uz,
        COALESCE(vipl.loss, 0)                                              AS claimed_loss_amount,
        CASE
            WHEN COALESCE(vipl.decision_summa, 0) > 0
            THEN COALESCE(vipl.decision_summa, 0)
            ELSE 0
        END                                                                 AS paid_loss_amount,
        CASE
            WHEN COALESCE(vipl.decision_summa, 0) <= 0
            THEN COALESCE(vipl.loss, 0)
            ELSE 0
        END                                                                 AS unpaid_claimed_loss_amount
    FROM {{ source('raw', 'ins_sobitie_oracle') }} s
    LEFT JOIN {{ source('raw', 'ins_loss_oracle') }} vipl ON vipl.sobitie_id = s.ins_id
    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a ON s.anketa_id = a.ins_id
    LEFT JOIN {{ source('raw', 'sp_division_oracle') }} div ON div.sp_id = s.division_id
    LEFT JOIN {{ ref('curated_product_dimension') }} pd ON pd.pturi_id = a.ins_type
    CROSS JOIN claims_params prm
    WHERE COALESCE(s.active, 0) < 2
      AND s.sluch_date IS NOT NULL
      AND s.sluch_date >= '2021-01-01'
      AND (
            (
                prm.p1_tb_direktor_filial = '2'
                AND FLOOR(s.division_id / 1000) * 1000 = FLOOR(prm.p1_div / 1000) * 1000
            )
            OR
            (
                prm.p1_tb_direktor_filial <> '2'
                AND s.division_id =
                    CASE prm.p1_tb_direktor_filial
                        WHEN '0' THEN prm.p1_div
                        WHEN '1' THEN prm.p1_div
                        WHEN '3' THEN s.division_id
                        ELSE prm.p1_div
                    END
            )
          )
      AND (
            prm.p240_sstat IS NULL
            OR prm.p240_sstat = '1'
            OR (prm.p240_sstat = '2'  AND s.doc_stat IN (0, 1))
            OR (prm.p240_sstat = '3'  AND s.doc_stat = 2)
            OR (prm.p240_sstat = '4'  AND s.doc_stat IN (3, 5))
            OR (prm.p240_sstat = '5'  AND s.doc_stat = 4)
            OR (prm.p240_sstat = '10' AND s.created_div <> 90000)
          )
      AND (
            prm.p1_userid IS NULL
            OR prm.p1_userid NOT IN (9081)
            OR a.ins_type <> 3
          )
)

SELECT
    DATE_TRUNC('month', sluch_date)::DATE                                  AS report_month,
    branch_name,
    product_name,
    insurance_type,
    product_category,
    COUNT(DISTINCT sobitie_id)                                            AS total_events,
    COUNT(DISTINCT CASE WHEN paid_loss_amount > 0 THEN sobitie_id END)    AS paid_events,
    SUM(claimed_loss_amount)                                              AS total_claimed_loss_uzs,
    SUM(CASE WHEN paid_loss_amount > 0 THEN paid_loss_amount ELSE 0 END)  AS paid_amount_uzs,
    SUM(CASE WHEN unpaid_claimed_loss_amount > 0
             THEN unpaid_claimed_loss_amount ELSE 0 END)                  AS unpaid_claimed_loss_uzs,
    SUM(CASE WHEN paid_loss_amount > 0 THEN paid_loss_amount ELSE 0 END)
      + SUM(CASE WHEN unpaid_claimed_loss_amount > 0
                 THEN unpaid_claimed_loss_amount ELSE 0 END)              AS incurred_claims_amount_uzs
FROM claims_base
GROUP BY 1, 2, 3, 4, 5
