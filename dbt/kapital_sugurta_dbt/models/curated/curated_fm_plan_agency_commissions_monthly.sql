{{ config(
    materialized = 'table',
    tags         = ['fm_plan', 'agency', 'commissions', 'monthly']
) }}

/*
  FM Plan — Agency Commissions (Curated)
  --------------------------------------
  Sheet 'Коммерческие инициативы', line item 'Агентские расходы'.
  Entity split from FM column 'Лицо':
    Юр лицо → legal entity
    Физ лицо → individual

  Coverage: Jan 2026 – Dec 2028 (initiative sheet monthly columns).
*/

WITH agent_commissions AS (
    SELECT
        period_start_date::DATE                                             AS report_month,
        entity_type_label,
        COALESCE(value_mln_uzs, 0) * 1000000                                AS commission_amount_uzs
    FROM {{ source('raw', 'fm_plan_initiatives') }}
    WHERE section_name = 'commercial_initiatives'
      AND line_item = 'Агентские расходы'
),

aggregated AS (
    SELECT
        report_month,
        SUM(commission_amount_uzs)                                            AS total_commission_volume_uzs,
        SUM(
            CASE
                WHEN entity_type_label ILIKE '%Юр%'
                THEN commission_amount_uzs
                ELSE 0
            END
        )                                                                   AS legal_entity_commission_volume_uzs,
        SUM(
            CASE
                WHEN entity_type_label ILIKE '%Физ%'
                THEN commission_amount_uzs
                ELSE 0
            END
        )                                                                   AS individual_commission_volume_uzs
    FROM agent_commissions
    GROUP BY 1
)

SELECT
    report_month,
    ROUND(total_commission_volume_uzs::NUMERIC, 2)                            AS total_commission_volume_uzs,
    ROUND(legal_entity_commission_volume_uzs::NUMERIC, 2)                     AS legal_entity_commission_volume_uzs,
    ROUND(individual_commission_volume_uzs::NUMERIC, 2)                       AS individual_commission_volume_uzs,
    'Plan'::TEXT                                                              AS scenario
FROM aggregated
WHERE total_commission_volume_uzs <> 0
ORDER BY report_month
