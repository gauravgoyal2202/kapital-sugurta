{{ config(materialized='table') }}

/*
  Dashboard 15 — Policy Renewal Indicators
  ------------------------------------------------
  Grain: one expired physical-person policy (fizyur = 0).
  Matches Oracle KONTRAGENT_REPORT / client validation sheet.

  Power BI (required):
    Renewal Rate % =
      DIVIDE(
        SUM(total_renewed_policies),
        SUM(total_expiring_policies)
      ) * 100

    Do NOT use DISTINCTCOUNT(client_id) — that is ~9.97% vs sheet ~9.25%.
    Do NOT format a 0.09 ratio as Percent (shows 0.09% instead of 9%).
    renewal_rate_pct is already a monthly percent; AVERAGE is OK only
    with no product/channel filter.
*/

SELECT
    report_month,
    EXTRACT(YEAR FROM report_month)::INT AS report_year,
    EXTRACT(QUARTER FROM report_month)::INT AS report_quarter,
    insurance_type,
    CASE insurance_type
        WHEN 'Compulsory' THEN 'Обязательное'
        WHEN 'Mandatory'  THEN 'Обязательное'
        ELSE 'Добровольное'
    END AS insurance_type_ru,
    CASE insurance_type
        WHEN 'Compulsory' THEN 'Мажбурий'
        WHEN 'Mandatory'  THEN 'Мажбурий'
        ELSE 'Ихтиёрий'
    END AS insurance_type_uz_cyrl,
    CASE insurance_type
        WHEN 'Compulsory' THEN 'Majburiy'
        WHEN 'Mandatory'  THEN 'Majburiy'
        ELSE 'Ixtiyoriy'
    END AS insurance_type_uz_latn,
    product_category,
    product_name,
    client_id,
    1 AS expired_clients,
    is_renewed AS renewed_clients,
    1 AS total_expiring_policies,
    is_renewed AS total_renewed_policies,
    ROUND(
        100.0 * SUM(is_renewed) OVER (PARTITION BY report_month)
        / NULLIF(COUNT(*) OVER (PARTITION BY report_month), 0)
    , 2) AS renewal_rate_pct,
    'Actual'::TEXT AS scenario
FROM {{ ref('curated_policy_renewals') }}
WHERE fizyur = 0
  AND client_id IS NOT NULL
ORDER BY report_month DESC, client_id
