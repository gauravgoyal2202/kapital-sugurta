{{ config(
    materialized = 'table',
    tags         = ['profitability', 'dashboard', 'earned'],
    post_hook    = [
        "CREATE INDEX IF NOT EXISTS idx_cpemd_month   ON {{ this }} (report_month)",
        "CREATE INDEX IF NOT EXISTS idx_cpemd_branch  ON {{ this }} (branch_name)",
        "CREATE INDEX IF NOT EXISTS idx_cpemd_product ON {{ this }} (product_name)"
    ]
) }}

/*
  Earned premium / commission / bonus by branch and product (monthly).
  Same pro-rata logic as curated_profitability_earned_monthly_company;
  grain adds branch_name + product dimensions for dashboard filters.
*/

WITH month_spine AS (
    SELECT
        DATE_TRUNC('month', d)::DATE                                      AS period_start,
        (DATE_TRUNC('month', d) + INTERVAL '1 month')::DATE              AS period_end
    FROM generate_series(
        '2021-01-01'::DATE,
        DATE_TRUNC('month', CURRENT_DATE)::DATE,
        '1 month'::INTERVAL
    ) AS d
),

rastorg AS (
    SELECT DISTINCT ON (r.tb_anketa, r.tb_polis)
        r.tb_anketa                                                         AS anketa_id,
        r.tb_polis                                                          AS polis_id,
        r.tb_dateras::DATE                                                  AS tb_dateras
    FROM {{ source('raw', 'ins_rastorg_oracle') }} r
    WHERE r.tb_anketa  IS NOT NULL
      AND r.tb_dateras IS NOT NULL
      AND r.tb_schet   IS NOT NULL
      AND TRIM(r.tb_schet::TEXT) <> '0'
    ORDER BY r.tb_anketa, r.tb_polis, r.tb_dateras ASC, r.tb_schet DESC
),

earned_detailed AS (
    SELECT
        ms.period_start,
        ms.period_end,
        pb.anketa_id,
        pb.polis_id,
        pb.ins_datef,
        pb.ins_datet,
        pb.oplsum,
        pb.kom_sum,
        pb.fifty_zp,
        pb.fifty_dop,
        pb.fifty_director,
        COALESCE(div.sp_name1, 'Head Office')                             AS branch_name,
        COALESCE(div.sp_name2, 'Head Office')                             AS branch_name_uz,
        COALESCE(pd.product_name, 'Other')                                AS product_name,
        COALESCE(pd.product_name_uz, 'Other')                             AS product_name_uz,
        COALESCE(pd.insurance_type, 'Voluntary')                          AS insurance_type,
        COALESCE(pd.category, 'Unclassified')                             AS product_category,
        COALESCE(pd.category_uz, 'Unclassified')                          AS product_category_uz
    FROM {{ ref('curated_profitability_earned_payment_base') }} pb
    INNER JOIN month_spine ms
        ON pb.ins_datef < ms.period_end
       AND pb.ins_datet >= ms.period_start
    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a
        ON a.ins_id = pb.anketa_id
    LEFT JOIN {{ source('raw', 'sp_division_oracle') }} div
        ON div.sp_id = COALESCE(a.temp_div, a.ins_div)
    LEFT JOIN {{ ref('curated_product_dimension') }} pd
        ON pd.pturi_id = a.ins_type
),

earned_calc_dates AS (
    SELECT
        d.*,
        d.ins_datef::DATE                                                   AS policy_begin_date,
        d.ins_datet::DATE                                                   AS policy_end_date,
        CASE
            WHEN d.ins_datef IS NULL OR d.ins_datet IS NULL THEN NULL
            WHEN r.tb_dateras IS NOT NULL AND r.tb_dateras <= d.ins_datet::DATE + 1
            THEN r.tb_dateras
            ELSE d.ins_datet::DATE + 1
        END                                                                 AS effective_end_exclusive,
        COALESCE(d.fifty_zp, 0) + COALESCE(d.fifty_dop, 0)
          + COALESCE(d.fifty_director, 0)                                   AS staff_bonus
    FROM earned_detailed d
    LEFT JOIN rastorg r
        ON  r.anketa_id = d.anketa_id
        AND (r.polis_id = d.polis_id OR r.polis_id IS NULL)
),

earned_calc_base AS (
    SELECT
        c.*,
        CASE
            WHEN c.policy_begin_date IS NULL OR c.policy_end_date IS NULL
              OR c.policy_end_date < c.policy_begin_date THEN 0
            ELSE c.policy_end_date - c.policy_begin_date + 1
        END                                                                 AS original_policy_days,
        CASE
            WHEN c.policy_begin_date IS NULL OR c.effective_end_exclusive IS NULL
              OR c.effective_end_exclusive <= c.policy_begin_date THEN 0
            ELSE GREATEST(
                0,
                LEAST(c.effective_end_exclusive, c.period_end)
                - GREATEST(c.policy_begin_date, c.period_start)
            )
        END                                                                 AS earned_days
    FROM earned_calc_dates c
),

earned_calc_factor AS (
    SELECT
        cb.*,
        CASE
            WHEN cb.original_policy_days > 0
            THEN cb.earned_days::NUMERIC / cb.original_policy_days
            ELSE 0
        END                                                                 AS earned_factor
    FROM earned_calc_base cb
),

earned_calc_final AS (
    SELECT
        cf.period_start,
        cf.branch_name,
        cf.product_name,
        cf.insurance_type,
        cf.product_category,
        cf.earned_days,
        ROUND(COALESCE(cf.oplsum, 0)      * cf.earned_factor, 2)          AS earned_premium,
        ROUND(COALESCE(cf.staff_bonus, 0) * cf.earned_factor, 2)          AS earned_bonus,
        ROUND(COALESCE(cf.kom_sum, 0)     * cf.earned_factor, 2)          AS earned_commission
    FROM earned_calc_factor cf
    WHERE cf.earned_days > 0
)

SELECT
    period_start                                                          AS report_month,
    branch_name,
    product_name,
    insurance_type,
    product_category,
    SUM(COALESCE(earned_premium, 0))                                    AS earned_premium_uzs,
    SUM(COALESCE(earned_bonus, 0))                                        AS earned_bonus_uzs,
    SUM(COALESCE(earned_commission, 0))                                   AS earned_commission_uzs,
    SUM(earned_days)::NUMERIC / 365.0                                     AS policy_exposure
FROM earned_calc_final
GROUP BY
    period_start,
    branch_name,
    product_name,
    insurance_type,
    product_category
