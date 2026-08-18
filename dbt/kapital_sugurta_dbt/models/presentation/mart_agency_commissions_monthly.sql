{{ config(
    materialized = 'table',
    tags         = ['agency', 'commissions', 'monthly', 'fm_plan']
) }}

/*
  Agency Commissions — Monthly (Mart)
  -----------------------------------
  Actual: curated_agency_commissions (Oracle agent acts).
  Plan:   FM commercial initiatives — 'Агентские расходы' (Jan 2026 – Dec 2028).
*/

WITH aggregated_commissions AS (
    SELECT
        DATE_TRUNC('month', commission_date)::DATE                             AS report_month,
        SUM(commission_amount_uzs)                                            AS total_commission_volume_uzs,
        SUM(
            CASE
                WHEN entity_type_flag IN (1, '1')
                THEN commission_amount_uzs
                ELSE 0
            END
        )                                                                     AS legal_entity_commission_volume_uzs,
        SUM(
            CASE
                WHEN entity_type_flag IN (2, '2')
                THEN commission_amount_uzs
                ELSE 0
            END
        )                                                                     AS individual_commission_volume_uzs
    FROM {{ ref('curated_agency_commissions') }}
    WHERE commission_date IS NOT NULL
    GROUP BY 1
),

combined AS (
    SELECT
        report_month,
        'Actual'::TEXT                                                        AS scenario,
        COALESCE(total_commission_volume_uzs, 0)                              AS total_commission_volume_uzs,
        COALESCE(legal_entity_commission_volume_uzs, 0)                       AS legal_entity_commission_volume_uzs,
        COALESCE(individual_commission_volume_uzs, 0)                         AS individual_commission_volume_uzs
    FROM aggregated_commissions

    UNION ALL

    SELECT
        report_month,
        scenario,
        total_commission_volume_uzs,
        legal_entity_commission_volume_uzs,
        individual_commission_volume_uzs
    FROM {{ ref('curated_fm_plan_agency_commissions_monthly') }}
),

period_flags AS (
    SELECT
        *,
        CASE
            WHEN report_month = MAX(report_month) OVER (
                PARTITION BY scenario, EXTRACT(YEAR FROM report_month), EXTRACT(QUARTER FROM report_month)
            ) THEN 1 ELSE 0
        END AS is_qtr_end,
        CASE
            WHEN report_month = MAX(report_month) OVER (
                PARTITION BY scenario, EXTRACT(YEAR FROM report_month)
            ) THEN 1 ELSE 0
        END AS is_year_end
    FROM combined
)

SELECT
    report_month,
    scenario,

    total_commission_volume_uzs,
    legal_entity_commission_volume_uzs,
    individual_commission_volume_uzs,

    CASE WHEN is_qtr_end = 1 THEN total_commission_volume_uzs ELSE 0 END
        AS total_commission_volume_uzs_qtr,
    CASE WHEN is_qtr_end = 1 THEN legal_entity_commission_volume_uzs ELSE 0 END
        AS legal_entity_commission_volume_uzs_qtr,
    CASE WHEN is_qtr_end = 1 THEN individual_commission_volume_uzs ELSE 0 END
        AS individual_commission_volume_uzs_qtr,

    CASE WHEN is_year_end = 1 THEN total_commission_volume_uzs ELSE 0 END
        AS total_commission_volume_uzs_year,
    CASE WHEN is_year_end = 1 THEN legal_entity_commission_volume_uzs ELSE 0 END
        AS legal_entity_commission_volume_uzs_year,
    CASE WHEN is_year_end = 1 THEN individual_commission_volume_uzs ELSE 0 END
        AS individual_commission_volume_uzs_year,

    CASE
        WHEN total_commission_volume_uzs > 0
        THEN (legal_entity_commission_volume_uzs / total_commission_volume_uzs) * 100
        ELSE 0
    END AS legal_entity_commissions_pct,

    CASE
        WHEN total_commission_volume_uzs > 0
        THEN (individual_commission_volume_uzs / total_commission_volume_uzs) * 100
        ELSE 0
    END AS individual_commission_volume_pct

FROM period_flags
ORDER BY report_month DESC, scenario ASC
