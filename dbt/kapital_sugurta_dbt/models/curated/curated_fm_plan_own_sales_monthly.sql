{{ config(
    materialized = 'table',
    tags         = ['fm_plan', 'own_sales', 'monthly']
) }}

/*
  FM Plan — Own Sales / Digital Channel (Curated)
  ------------------------------------------------
  Sheet 'Коммерческие инициативы', initiative 'Развитие прямых цифровых каналов'.
  Grain: month × FM product, aligned with mart_own_sales.
*/

WITH raw_digital AS (
    SELECT
        period_start_date::DATE                                             AS report_month,
        EXTRACT(YEAR FROM period_start_date)::INT                           AS report_year,
        fm_product,
        grouping_name,
        line_item,
        value_mln_uzs
    FROM {{ source('raw', 'fm_plan_initiatives') }}
    WHERE section_name = 'commercial_initiatives'
      AND initiative_name = 'Развитие прямых цифровых каналов'
),

mapped AS (
    SELECT
        report_month,
        report_year,
        COALESCE(fm_product, 'Other')                                       AS product_name,
        CASE grouping_name
            WHEN 'Имущественое страхование'       THEN 'Property Insurance'
            WHEN 'Страхование авто'               THEN 'Auto Insurance'
            WHEN 'Обязательные виды страхования'  THEN 'Compulsory Insurance'
            WHEN 'Страхование ответственности'    THEN 'Liability Insurance'
            WHEN 'Личное страхование'             THEN 'Personal Insurance'
            WHEN 'Страхование финансовых рисков'  THEN 'Financial Risks'
            ELSE COALESCE(grouping_name, 'Other')
        END                                                                 AS product_category,
        CASE
            WHEN grouping_name = 'Обязательные виды страхования'
            THEN 'Compulsory'
            ELSE 'Voluntary'
        END                                                                 AS insurance_type,
        line_item,
        COALESCE(value_mln_uzs, 0) * 1000000                                AS value_uzs
    FROM raw_digital
),

aggregated AS (
    SELECT
        report_month,
        report_year,
        insurance_type,
        product_category,
        product_name,
        SUM(CASE WHEN line_item = 'Продажи инициатив' THEN value_uzs ELSE 0 END)
                                                                            AS premium_uzs,
        SUM(
            CASE
                WHEN line_item IN (
                    'Агентские расходы',
                    'Расходы на маркетинг',
                    'Расходы на найм',
                    'Консультационные расходы'
                )
                THEN value_uzs
                ELSE 0
            END
        )                                                                   AS commission_uzs,
        SUM(CASE WHEN line_item = 'Расходы на Возмещения' THEN value_uzs ELSE 0 END)
                                                                            AS claims_uzs,
        SUM(CASE WHEN line_item = 'Расходы на мотивацию' THEN value_uzs ELSE 0 END)
                                                                            AS motivation_uzs,
        SUM(
            CASE
                WHEN line_item = 'Расходы регресса' THEN value_uzs
                WHEN line_item = 'Доходы от регресса' THEN -value_uzs
                ELSE 0
            END
        )                                                                   AS terminated_uzs
    FROM mapped
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    report_month,
    report_year,
    EXTRACT(QUARTER FROM report_month)::INT                                 AS report_quarter,
    'Website - Internal - API'::TEXT                                        AS channels,
    insurance_type,
    product_category,
    product_name,
    ROUND(premium_uzs::NUMERIC, 2)                                          AS premium_uzs,
    ROUND(commission_uzs::NUMERIC, 2)                                       AS commission_uzs,
    ROUND(claims_uzs::NUMERIC, 2)                                           AS claims_uzs,
    ROUND(motivation_uzs::NUMERIC, 2)                                       AS motivation_uzs,
    ROUND(terminated_uzs::NUMERIC, 2)                                       AS terminated_uzs,
    0::NUMERIC                                                              AS strah_summa_uzs,
    'Plan'::TEXT                                                            AS scenario
FROM aggregated
WHERE premium_uzs <> 0
   OR commission_uzs <> 0
   OR claims_uzs <> 0
   OR motivation_uzs <> 0
   OR terminated_uzs <> 0

ORDER BY report_month, product_name
