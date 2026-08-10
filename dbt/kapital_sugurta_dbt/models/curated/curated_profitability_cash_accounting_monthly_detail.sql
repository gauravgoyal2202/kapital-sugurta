{{ config(
    materialized = 'table',
    tags         = ['profitability', 'dashboard', 'cash_accounting'],
    post_hook    = [
        "CREATE INDEX IF NOT EXISTS idx_cpcamd_month   ON {{ this }} (month_start_date)",
        "CREATE INDEX IF NOT EXISTS idx_cpcamd_branch  ON {{ this }} (branch_name)",
        "CREATE INDEX IF NOT EXISTS idx_cpcamd_product ON {{ this }} (product_name)"
    ]
) }}

/*
  Cash / accounting profitability by branch and product (monthly).
  Same metric rules as curated_profitability_cash_accounting_monthly;
  grain adds branch_name + product dimensions for dashboard filters.
  Outgoing reinsurance is company-level; allocated to branch/product rows
  proportionally by premium share within each month.
*/

WITH month_spine AS (
    SELECT
        DATE_TRUNC('month', d)::DATE                                      AS month_start_date,
        EXTRACT(YEAR    FROM d)::INT                                      AS report_year,
        EXTRACT(MONTH   FROM d)::INT                                      AS report_month,
        EXTRACT(QUARTER FROM d)::INT                                      AS report_quarter,
        TO_CHAR(d, 'YYYY-MM')                                             AS period_label
    FROM generate_series(
        '2021-01-01'::DATE,
        DATE_TRUNC('month', CURRENT_DATE)::DATE,
        '1 month'::INTERVAL
    ) AS d
),

valid_polis_anketa AS (
    SELECT DISTINCT tb_anketa
    FROM {{ source('raw', 'ins_polis_oracle') }}
    WHERE tb_status IN (2, 9, 10)
),

claims_params AS (
    SELECT
        '3'::TEXT           AS p1_tb_direktor_filial,
        0::NUMERIC          AS p1_div,
        '1'::TEXT           AS p240_sstat,
        NULL::NUMERIC       AS p1_userid
),

payment_lines AS (
    SELECT
        DATE_TRUNC('month', bc.pym_date)::DATE                           AS month_start_date,
        COALESCE(div.sp_name1, 'Head Office')                             AS branch_name,
        COALESCE(div.sp_name2, 'Head Office')                             AS branch_name_uz,
        COALESCE(pd.product_name, 'Other')                                AS product_name,
        COALESCE(pd.product_name_uz, 'Other')                             AS product_name_uz,
        a.ins_type                                                          AS product_id,
        COALESCE(pd.insurance_type, 'Voluntary')                          AS insurance_type,
        COALESCE(pd.category, 'Unclassified')                             AS product_category,
        COALESCE(pd.category_uz, 'Unclassified')                          AS product_category_uz,

        CASE
            WHEN o.opl_val = 1 THEN COALESCE(o.oplata, 0)
            ELSE COALESCE(o.opl_summa, 0) * COALESCE(kurs.kurs_value, 0)
        END                                                             AS premium_uzs,

        COALESCE(pf.fifty_total, 0)                                     AS motivation_bonus_fifty_uzs

    FROM {{ source('raw', 'ins_oplata_oracle') }} o

    JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc
        ON bc.ins_id = o.bc_id
       AND bc.pym_date >= DATE '2021-01-01'

    JOIN valid_polis_anketa vp
        ON vp.tb_anketa = o.anketa_id

    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a
        ON a.ins_id = o.anketa_id

    LEFT JOIN {{ source('raw', 'sp_division_oracle') }} div
        ON div.sp_id = COALESCE(a.temp_div, a.ins_div)

    LEFT JOIN {{ ref('curated_product_dimension') }} pd
        ON pd.pturi_id = a.ins_type

    LEFT JOIN {{ source('raw', 'ins_kurs_oracle') }} k
        ON k.kurs_date::DATE = bc.pym_date::DATE

    CROSS JOIN LATERAL (
        SELECT {{ profitability_kurs_value('o.opl_val', 'k') }} AS kurs_value
    ) kurs

    LEFT JOIN {{ ref('curated_partner_payment_base') }} pf
        ON pf.oplata_id = o.ins_id

    WHERE (
            o.ins_type <> 3
            OR (o.ins_type = 3 AND bc.status = 2)
          )

    UNION ALL

    SELECT
        DATE_TRUNC('month', bc.pym_date)::DATE                           AS month_start_date,
        'Head Office'::TEXT                                               AS branch_name,
        'Head Office'::TEXT                                               AS branch_name_uz,
        'OSAGO OLD'::TEXT                                                 AS product_name,
        'OSAGO OLD'::TEXT                                                 AS product_name_uz,
        NULL::BIGINT                                                      AS product_id,
        'Compulsory'::TEXT                                                AS insurance_type,
        'OSAGO OLD'::TEXT                                                 AS product_category,
        'OSAGO OLD'::TEXT                                                 AS product_category_uz,

        COALESCE(o.tb_summa, 0)                                         AS premium_uzs,
        0::NUMERIC                                                      AS motivation_bonus_fifty_uzs

    FROM {{ source('raw', 'tb_anketa_oracle') }} t

    JOIN {{ source('raw', 'tb_polis_oracle') }} p
        ON p.tb_anketa = t.tb_id

    JOIN {{ source('raw', 'tb_oplata_oracle') }} o
        ON o.tb_anketa = t.tb_id

    JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc
        ON bc.ins_id = o.bc_id
       AND bc.status = 2
       AND bc.pym_date >= DATE '2021-01-01'

    WHERE o.tb_typepl = 1
      AND p.tb_status IN (2, 8)
),

/*
  Commission uses agent settlement month (ins_agent_akt.akt_date, active=2),
  not payment month — matches client profitability workbook rows 65–66.
*/
commission_lines AS (
    SELECT
        DATE_TRUNC('month', akt.akt_date)::DATE                           AS month_start_date,
        COALESCE(div.sp_name1, 'Head Office')                             AS branch_name,
        COALESCE(div.sp_name2, 'Head Office')                             AS branch_name_uz,
        COALESCE(pd.product_name, 'Other')                                AS product_name,
        COALESCE(pd.product_name_uz, 'Other')                             AS product_name_uz,
        a.ins_type                                                          AS product_id,
        COALESCE(pd.insurance_type, 'Voluntary')                          AS insurance_type,
        COALESCE(pd.category, 'Unclassified')                             AS product_category,
        COALESCE(pd.category_uz, 'Unclassified')                          AS product_category_uz,

        COALESCE(o.kommis_summa, 0) * COALESCE(kurs.kurs_value, 0)    AS agent_commission_uzs

    FROM {{ source('raw', 'ins_oplata_oracle') }} o

    JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc
        ON bc.ins_id = o.bc_id

    INNER JOIN {{ source('raw', 'ins_agent_akt_oracle') }} akt
        ON akt.ins_id = o.akt
       AND akt.active = 2
       AND akt.akt_date >= DATE '2021-01-01'

    JOIN valid_polis_anketa vp
        ON vp.tb_anketa = o.anketa_id

    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a
        ON a.ins_id = o.anketa_id

    LEFT JOIN {{ source('raw', 'sp_division_oracle') }} div
        ON div.sp_id = COALESCE(a.temp_div, a.ins_div)

    LEFT JOIN {{ ref('curated_product_dimension') }} pd
        ON pd.pturi_id = a.ins_type

    LEFT JOIN {{ source('raw', 'ins_kurs_oracle') }} k
        ON k.kurs_date::DATE = bc.pym_date::DATE

    CROSS JOIN LATERAL (
        SELECT {{ profitability_kurs_value('o.opl_val', 'k') }} AS kurs_value
    ) kurs

    WHERE (
            o.ins_type <> 3
            OR (o.ins_type = 3 AND bc.status = 2)
          )

    UNION ALL

    SELECT
        DATE_TRUNC('month', akt.akt_date)::DATE                           AS month_start_date,
        'Head Office'::TEXT                                               AS branch_name,
        'Head Office'::TEXT                                               AS branch_name_uz,
        'OSAGO OLD'::TEXT                                                 AS product_name,
        'OSAGO OLD'::TEXT                                                 AS product_name_uz,
        NULL::BIGINT                                                      AS product_id,
        'Compulsory'::TEXT                                                AS insurance_type,
        'OSAGO OLD'::TEXT                                                 AS product_category,
        'OSAGO OLD'::TEXT                                                 AS product_category_uz,

        (COALESCE(o.tb_summa, 0) * COALESCE(p.tb_komissia, 0) / 100.0) AS agent_commission_uzs

    FROM {{ source('raw', 'tb_anketa_oracle') }} t

    JOIN {{ source('raw', 'tb_polis_oracle') }} p
        ON p.tb_anketa = t.tb_id

    JOIN {{ source('raw', 'tb_oplata_oracle') }} o
        ON o.tb_anketa = t.tb_id

    JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc
        ON bc.ins_id = o.bc_id
       AND bc.status = 2

    INNER JOIN {{ source('raw', 'ins_agent_akt_oracle') }} akt
        ON akt.ins_id = p.akt
       AND akt.active = 2
       AND akt.akt_date >= DATE '2021-01-01'

    WHERE o.tb_typepl = 1
      AND p.tb_status IN (2, 8)
),

premium_fifty_monthly AS (
    SELECT
        month_start_date,
        branch_name,
        branch_name_uz,
        product_name,
        product_name_uz,
        product_id,
        insurance_type,
        product_category,
        product_category_uz,
        SUM(premium_uzs)                                                AS premium_uzs,
        SUM(motivation_bonus_fifty_uzs)                                 AS motivation_bonus_fifty_uzs
    FROM payment_lines
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

commission_monthly AS (
    SELECT
        month_start_date,
        branch_name,
        branch_name_uz,
        product_name,
        product_name_uz,
        product_id,
        insurance_type,
        product_category,
        product_category_uz,
        SUM(agent_commission_uzs)                                       AS agent_commission_uzs
    FROM commission_lines
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

claims_monthly AS (
    SELECT
        DATE_TRUNC('month', cb.decision_date)::DATE                       AS month_start_date,
        cb.branch_name,
        cb.branch_name_uz,
        cb.product_name,
        cb.product_name_uz,
        cb.product_id,
        cb.insurance_type,
        cb.product_category,
        cb.product_category_uz,
        SUM(cb.paid_loss_amount)                                          AS claims_volume_uzs
    FROM (
        SELECT
            s.decision_date::DATE                                         AS decision_date,
            COALESCE(div.sp_name1, 'Head Office')                         AS branch_name,
            COALESCE(div.sp_name2, 'Head Office')                         AS branch_name_uz,
            COALESCE(pd.product_name, 'Other')                            AS product_name,
            COALESCE(pd.product_name_uz, 'Other')                         AS product_name_uz,
            a.ins_type                                                      AS product_id,
            COALESCE(pd.insurance_type, 'Voluntary')                      AS insurance_type,
            COALESCE(pd.category, 'Unclassified')                         AS product_category,
            COALESCE(pd.category_uz, 'Unclassified')                      AS product_category_uz,
            CASE
                WHEN COALESCE(vipl.decision_summa, 0) > 0
                THEN COALESCE(vipl.decision_summa, 0)
                ELSE 0
            END                                                           AS paid_loss_amount
        FROM {{ source('raw', 'ins_sobitie_oracle') }} s
        INNER JOIN {{ source('raw', 'ins_loss_oracle') }} vipl
            ON vipl.sobitie_id = s.ins_id
        LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a
            ON s.anketa_id = a.ins_id
        LEFT JOIN {{ source('raw', 'sp_division_oracle') }} div
            ON div.sp_id = s.division_id
        LEFT JOIN {{ ref('curated_product_dimension') }} pd
            ON pd.pturi_id = a.ins_type
        CROSS JOIN claims_params prm
        WHERE COALESCE(s.active, 0) < 2
          AND s.decision_date IS NOT NULL
          AND s.decision_date >= DATE '2021-01-01'
          AND COALESCE(vipl.decision_summa, 0) > 0
          AND (
                (
                    prm.p1_tb_direktor_filial = '2'
                    AND FLOOR(s.division_id / 1000) * 1000
                        = FLOOR(prm.p1_div / 1000) * 1000
                )
                OR (
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
    ) cb
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

terminated_monthly AS (
    SELECT
        DATE_TRUNC('month', r.tb_dateras)::DATE                         AS month_start_date,
        COALESCE(div.sp_name1, 'Head Office')                             AS branch_name,
        COALESCE(div.sp_name2, 'Head Office')                             AS branch_name_uz,
        COALESCE(pd.product_name, 'Other')                                AS product_name,
        COALESCE(pd.product_name_uz, 'Other')                             AS product_name_uz,
        a.ins_type                                                          AS product_id,
        COALESCE(pd.insurance_type, 'Voluntary')                          AS insurance_type,
        COALESCE(pd.category, 'Unclassified')                             AS product_category,
        COALESCE(pd.category_uz, 'Unclassified')                          AS product_category_uz,
        SUM(COALESCE(r.vozvrat_sum, 0))                                 AS terminated_contracts_volume_uzs
    FROM {{ source('raw', 'ins_rastorg_oracle') }} r
    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a
        ON a.ins_id = r.tb_anketa
    LEFT JOIN {{ source('raw', 'sp_division_oracle') }} div
        ON div.sp_id = COALESCE(a.temp_div, a.ins_div)
    LEFT JOIN {{ ref('curated_product_dimension') }} pd
        ON pd.pturi_id = a.ins_type
    WHERE r.tb_dateras IS NOT NULL
      AND r.tb_dateras >= DATE '2021-01-01'
      AND r.tb_schet IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

dimension_keys AS (
    SELECT month_start_date, branch_name, branch_name_uz, product_name, product_name_uz, product_id, insurance_type, product_category, product_category_uz
    FROM premium_fifty_monthly
    UNION
    SELECT month_start_date, branch_name, branch_name_uz, product_name, product_name_uz, product_id, insurance_type, product_category, product_category_uz
    FROM commission_monthly
    UNION
    SELECT month_start_date, branch_name, branch_name_uz, product_name, product_name_uz, product_id, insurance_type, product_category, product_category_uz
    FROM claims_monthly
    UNION
    SELECT month_start_date, branch_name, branch_name_uz, product_name, product_name_uz, product_id, insurance_type, product_category, product_category_uz
    FROM terminated_monthly
),

reinsurance_excel AS (
    SELECT
        DATE_TRUNC('month', premium_accrual_date::DATE)::DATE              AS month_start_date,
        SUM(COALESCE(total_accrued_premium_uzs::NUMERIC, 0))              AS outgoing_reinsurance_premium_uzs
    FROM {{ source('raw', 'reinsurance_outgoing_portfolio') }}
    WHERE premium_accrual_date IS NOT NULL
    GROUP BY 1
),

reinsurance_oracle AS (
    SELECT
        DATE_TRUNC('month', r.slip_date)::DATE                            AS month_start_date,
        SUM(COALESCE(r.netto_accrual_premium, 0))                         AS outgoing_reinsurance_premium_uzs
    FROM {{ source('raw', 'ins_reinsurance_oracle') }} r
    WHERE r.direction = 1
      AND r.slip_date >= DATE '2026-01-01'
      AND COALESCE(r.netto_accrual_premium, 0) <> 0
    GROUP BY 1
),

reinsurance_outgoing AS (
    SELECT month_start_date, SUM(outgoing_reinsurance_premium_uzs)        AS outgoing_reinsurance_premium_uzs
    FROM (
        SELECT * FROM reinsurance_excel
        UNION ALL
        SELECT * FROM reinsurance_oracle
    ) u
    GROUP BY 1
),

detail_base AS (

SELECT
    ms.month_start_date,
    ms.report_year,
    ms.report_month,
    ms.report_quarter,
    ms.period_label,

    dk.branch_name,
    dk.branch_name_uz,
    dk.product_name,
    dk.product_name_uz,
    dk.product_id,
    dk.insurance_type,
    dk.product_category,
    dk.product_category_uz,

    COALESCE(pf.premium_uzs, 0)                                           AS premium_uzs,
    COALESCE(t.terminated_contracts_volume_uzs, 0)                      AS terminated_contracts_volume_uzs,
    COALESCE(cm.agent_commission_uzs, 0)                                AS agent_commission_uzs,
    COALESCE(c.claims_volume_uzs, 0)                                    AS claims_volume_uzs,
    COALESCE(pf.motivation_bonus_fifty_uzs, 0)                          AS motivation_bonus_fifty_uzs

FROM month_spine ms
INNER JOIN dimension_keys dk
    ON dk.month_start_date = ms.month_start_date
LEFT JOIN premium_fifty_monthly pf
    ON  pf.month_start_date = dk.month_start_date
    AND pf.branch_name = dk.branch_name
    AND pf.product_name = dk.product_name
    AND pf.product_id IS NOT DISTINCT FROM dk.product_id
    AND pf.insurance_type = dk.insurance_type
    AND pf.product_category = dk.product_category
    AND pf.product_category_uz = dk.product_category_uz
LEFT JOIN commission_monthly cm
    ON  cm.month_start_date = dk.month_start_date
    AND cm.branch_name = dk.branch_name
    AND cm.product_name = dk.product_name
    AND cm.product_id IS NOT DISTINCT FROM dk.product_id
    AND cm.insurance_type = dk.insurance_type
    AND cm.product_category = dk.product_category
    AND cm.product_category_uz = dk.product_category_uz
LEFT JOIN claims_monthly c
    ON  c.month_start_date = dk.month_start_date
    AND c.branch_name = dk.branch_name
    AND c.product_name = dk.product_name
    AND c.product_id IS NOT DISTINCT FROM dk.product_id
    AND c.insurance_type = dk.insurance_type
    AND c.product_category = dk.product_category
    AND c.product_category_uz = dk.product_category_uz
LEFT JOIN terminated_monthly t
    ON  t.month_start_date = dk.month_start_date
    AND t.branch_name = dk.branch_name
    AND t.product_name = dk.product_name
    AND t.product_id IS NOT DISTINCT FROM dk.product_id
    AND t.insurance_type = dk.insurance_type
    AND t.product_category = dk.product_category
    AND t.product_category_uz = dk.product_category_uz

)

SELECT
    d.month_start_date,
    d.report_year,
    d.report_month,
    d.report_quarter,
    d.period_label,

    d.branch_name,
    d.branch_name_uz,
    d.product_name,
    d.product_name_uz,
    d.product_id,
    d.insurance_type,
    d.product_category,
    d.product_category_uz,

    d.premium_uzs,
    d.terminated_contracts_volume_uzs,
    d.agent_commission_uzs,
    d.claims_volume_uzs,
    d.motivation_bonus_fifty_uzs,

    CASE
        WHEN SUM(d.premium_uzs) OVER (PARTITION BY d.month_start_date) = 0
        THEN ROUND(
            COALESCE(r.outgoing_reinsurance_premium_uzs, 0)
            / NULLIF(COUNT(*) OVER (PARTITION BY d.month_start_date), 0)::NUMERIC,
            2
        )
        ELSE ROUND(
            COALESCE(r.outgoing_reinsurance_premium_uzs, 0)
            * d.premium_uzs
            / NULLIF(SUM(d.premium_uzs) OVER (PARTITION BY d.month_start_date), 0)::NUMERIC,
            2
        )
    END                                                                 AS outgoing_reinsurance_premium_uzs

FROM detail_base d
LEFT JOIN reinsurance_outgoing r
    ON r.month_start_date = d.month_start_date

ORDER BY d.month_start_date, d.branch_name, d.product_name
