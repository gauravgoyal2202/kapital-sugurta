{{ config(
    materialized = 'table',
    tags         = ['fm_plan', 'insurance', 'product', 'monthly']
) }}

/*
  FM Plan — Product Group Premiums (Curated)
  ----------------------------------------
  Sheet 'Страхование', company consolidated direct premium block (6 FM groups).
  Grain: month × FM product group.
*/

WITH raw_product AS (
    SELECT
        period_start_date::DATE                                             AS report_month,
        EXTRACT(YEAR FROM period_start_date)::INT                           AS report_year,
        product_group_code,
        product_group_label,
        value_mln_uzs
    FROM {{ source('raw', 'fm_plan_metrics') }}
    WHERE section_name = 'direct_premium_by_product'
      AND metric_code = 'gross_direct_premium'
      AND product_group_code IS NOT NULL
),

product_dim AS (
    SELECT *
    FROM (
        VALUES
            ('property',        'Voluntary',  'Добровольное', 'Property Insurance',        'Имущественное страхование', 'Имущественное страхование'),
            ('financial_risks', 'Voluntary',  'Добровольное', 'Financial Risks',           'Финансовые риски',          'Финансовые риски'),
            ('personal',        'Voluntary',  'Добровольное', 'Personal Insurance',        'Личное страхование',        'Личное страхование'),
            ('liability',       'Voluntary',  'Добровольное', 'Liability Insurance',       'Страхование ответственности','Страхование ответственности'),
            ('compulsory',      'Compulsory', 'Обязательное', 'Compulsory Insurance',      'Обязательное страхование',  'Обязательное страхование'),
            ('auto',            'Voluntary',  'Добровольное', 'Auto Insurance',            'Авто страхование',          'Авто страхование')
    ) AS t (
        product_group_code,
        insurance_type,
        insurance_type_ru,
        category,
        category_ru,
        product_name
    )
),

premiums AS (
    SELECT
        rp.report_month,
        rp.report_year,
        pd.insurance_type,
        pd.insurance_type_ru,
        CASE pd.insurance_type
            WHEN 'Compulsory' THEN 'Мажбурий'
            ELSE 'Ихтиёрий'
        END                                                                 AS insurance_type_uz_cyrl,
        CASE pd.insurance_type
            WHEN 'Compulsory' THEN 'Majburiy'
            ELSE 'Ixtiyoriy'
        END                                                                 AS insurance_type_uz_latn,
        pd.category,
        pd.category_ru,
        pd.category                                                                 AS category_uz,
        pd.category                                                                 AS category_uz_latn,
        pd.product_name,
        pd.product_name                                                             AS product_name_ru,
        pd.product_name                                                             AS product_name_uz,
        pd.product_name                                                             AS product_name_uz_latn,
        ROUND(COALESCE(rp.value_mln_uzs, 0) * 1000000, 2)                          AS premium_uzs
    FROM raw_product rp
    INNER JOIN product_dim pd
        ON pd.product_group_code = rp.product_group_code
),

company_claims AS (
    SELECT
        month_start_date                                                        AS report_month,
        paid_amount_uzs                                                         AS claims_uzs
    FROM {{ ref('curated_fm_plan_insurance_monthly') }}
)

SELECT
    p.report_month,
    p.report_year,
    p.insurance_type,
    p.insurance_type_ru,
    p.insurance_type_uz_cyrl,
    p.insurance_type_uz_latn,
    p.category,
    p.category_ru,
    p.category_uz,
    p.category_uz_latn,
    p.product_name,
    p.product_name_ru,
    p.product_name_uz,
    p.product_name_uz_latn,
    'Plan'::TEXT                                                                AS sales_channel,
    'Internal'::TEXT                                                            AS partner_type,
    'FM Plan'::TEXT                                                             AS partner_name,

    p.premium_uzs                                                               AS co_prem,
    0::NUMERIC                                                                  AS co_liab,
    0::NUMERIC                                                                  AS co_term,
    CASE
        WHEN SUM(p.premium_uzs) OVER (PARTITION BY p.report_month) = 0
        THEN 0
        ELSE ROUND(
            cc.claims_uzs
            * p.premium_uzs
            / NULLIF(SUM(p.premium_uzs) OVER (PARTITION BY p.report_month), 0),
            2
        )
    END                                                                         AS co_claims,

    'Plan'::TEXT                                                                AS scenario

FROM premiums p
LEFT JOIN company_claims cc
    ON cc.report_month = p.report_month

ORDER BY p.report_month, p.product_name
