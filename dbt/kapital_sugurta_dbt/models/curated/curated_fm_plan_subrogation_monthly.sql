{{ config(
    materialized = 'table',
    tags         = ['fm_plan', 'subrogation', 'monthly']
) }}

/*
  FM Plan — Subrogation / Regress Recoveries (Curated)
  ----------------------------------------------------
  Sheet 'Коммерческие инициативы', line item 'Доходы от регресса'.
  Aggregated company-wide by month.

  FM does not split exact vs undefined recoveries — plan rows set
  exact = total and undefined = 0.
  Coverage: Jan 2026 – Dec 2028 (initiative sheet monthly columns).
*/

WITH regress_income AS (
    SELECT
        period_start_date::DATE                                             AS report_month,
        SUM(COALESCE(value_mln_uzs, 0)) * 1000000                           AS total_subrogation_volume_uzs
    FROM {{ source('raw', 'fm_plan_initiatives') }}
    WHERE section_name = 'commercial_initiatives'
      AND line_item = 'Доходы от регресса'
    GROUP BY 1
)

SELECT
    report_month,
    ROUND(total_subrogation_volume_uzs::NUMERIC, 2)                           AS total_subrogation_volume_uzs,
    ROUND(total_subrogation_volume_uzs::NUMERIC, 2)                           AS exact_subrogation_volume_uzs,
    0::NUMERIC                                                                AS undefined_subrogation_volume_uzs,
    'Plan'::TEXT                                                              AS scenario
FROM regress_income
WHERE total_subrogation_volume_uzs <> 0
ORDER BY report_month
