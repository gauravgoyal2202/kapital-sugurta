{{ config(
    materialized = 'table',
    tags         = ['fm_plan', 'commercial', 'monthly']
) }}

/*
  FM Plan — Commercial Initiatives (Curated)
  ------------------------------------------
  Sheet 'Коммерческие инициативы': initiative × product × line item × month.
  Aggregated to the grain of mart_commercial_development_priority_areas_base.

  Excludes the digital-channel initiative (routed to curated_fm_plan_own_sales_monthly).
  Units: mln UZS → UZS.
*/

WITH raw_initiatives AS (
    SELECT
        period_start_date::DATE                                             AS report_month,
        EXTRACT(YEAR FROM period_start_date)::INT                           AS report_year,
        initiative_name,
        fm_product,
        grouping_name,
        line_item,
        value_mln_uzs
    FROM {{ source('raw', 'fm_plan_initiatives') }}
    WHERE section_name = 'commercial_initiatives'
      AND initiative_name IS DISTINCT FROM 'Развитие прямых цифровых каналов'
),

mapped AS (
    SELECT
        report_month,
        report_year,
        initiative_name,
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
            WHEN initiative_name = 'Лидерство в Авто страховании'
            THEN 'Motor'
            WHEN initiative_name IN (
                'Лидерство в Банкинге',
                'Развитие партнерства с Анорбанк',
                'Развитие Партнерств'
            )
            THEN 'Banking'
            WHEN grouping_name = 'Страхование авто'
            THEN 'Motor'
            WHEN grouping_name = 'Имущественое страхование'
            THEN 'Property'
            ELSE 'Other'
        END                                                                 AS priority_area,
        CASE
            WHEN grouping_name = 'Обязательные виды страхования'
            THEN 'Compulsory'
            ELSE 'Voluntary'
        END                                                                 AS insurance_type,
        line_item,
        COALESCE(value_mln_uzs, 0) * 1000000                                AS value_uzs
    FROM raw_initiatives
),

aggregated AS (
    SELECT
        report_month,
        report_year,
        priority_area,
        insurance_type,
        product_category,
        product_name,
        SUM(CASE WHEN line_item = 'Продажи инициатив' THEN value_uzs ELSE 0 END)
                                                                            AS co_premium,
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
        )                                                                   AS co_expenses,
        SUM(CASE WHEN line_item = 'Расходы на Возмещения' THEN value_uzs ELSE 0 END)
                                                                            AS co_claims,
        SUM(CASE WHEN line_item = 'Расходы на мотивацию' THEN value_uzs ELSE 0 END)
                                                                            AS co_fifty,
        SUM(
            CASE
                WHEN line_item = 'Расходы регресса' THEN value_uzs
                WHEN line_item = 'Доходы от регресса' THEN -value_uzs
                ELSE 0
            END
        )                                                                   AS co_ras
    FROM mapped
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
    report_month,
    report_year,
    priority_area,
    insurance_type,
    CASE insurance_type
        WHEN 'Compulsory' THEN 'Обязательное'
        ELSE 'Добровольное'
    END                                                                     AS insurance_type_ru,
    CASE insurance_type
        WHEN 'Compulsory' THEN 'Мажбурий'
        ELSE 'Ихтиёрий'
    END                                                                     AS insurance_type_uz_cyrl,
    CASE insurance_type
        WHEN 'Compulsory' THEN 'Majburiy'
        ELSE 'Ixtiyoriy'
    END                                                                     AS insurance_type_uz_latn,
    product_category,
    product_name,
    'FM Plan'::TEXT                                                         AS channels,
    ROUND(co_premium::NUMERIC, 3)                                          AS co_premium,
    ROUND(co_claims::NUMERIC, 3)                                            AS co_claims,
    ROUND(co_expenses::NUMERIC, 3)                                           AS co_expenses,
    ROUND(co_fifty::NUMERIC, 3)                                             AS co_fifty,
    ROUND(co_ras::NUMERIC, 3)                                               AS co_ras,
    CASE
        WHEN co_premium > 0
        THEN ROUND(
            ((co_premium - co_expenses - co_claims - co_fifty) / co_premium * 100)::NUMERIC,
            2
        )
        ELSE 0
    END                                                                     AS profitability_pct,
    'Plan'::TEXT                                                            AS scenario
FROM aggregated
WHERE co_premium <> 0
   OR co_claims <> 0
   OR co_expenses <> 0
   OR co_fifty <> 0
   OR co_ras <> 0

ORDER BY report_month, priority_area, product_name
