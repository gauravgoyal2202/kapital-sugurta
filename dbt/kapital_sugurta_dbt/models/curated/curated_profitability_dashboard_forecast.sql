{{ config(
    materialized = 'table',
    tags         = ['profitability', 'dashboard', 'forecast']
) }}

/*
  Profitability Dashboard — Forecast (Curated)
  --------------------------------------------
  Year-wise portfolio forecast: one row per year from 2021 through the current
  calendar year (extends automatically; no next-year preview row).

  Client rules (Mirabbos, 2026):
    • Granularity: year-wise
    • Avg insurance premium: average of monthly (earned / exposure) over the
      previous six months; prefer forecast-year H1 when available
    • Avg ZNU per case: linear regression y = intercept + slope × x
      Year-wise table: yearly avg ZNU (claimed / events) for past 2 years,
      forecast at x = 3 (Excel TREND / FORECAST.LINEAR equivalent)
    • Claim frequency: average of monthly claim_frequency (previous 24 months)
    • Payment-to-ZNU share: SUM(paid) / SUM(claimed) over previous 24 months
    • All inputs use a sliding lookback ending before each forecast year starts
*/

WITH monthly AS (
    SELECT
        month_start_date,
        report_year,
        report_month,
        earned_premium_uzs,
        policy_exposure,
        total_events,
        claim_frequency,
        paid_amount_uzs,
        total_claimed_loss_uzs,
        CASE
            WHEN policy_exposure > 0
            THEN earned_premium_uzs / policy_exposure
            ELSE NULL
        END                                                                 AS avg_premium_uzs
    FROM {{ ref('curated_profitability_dashboard_monthly') }}
),

forecast_years AS (
    SELECT generate_series(
        2021,
        EXTRACT(YEAR FROM CURRENT_DATE)::INT
    )::INT                                                                  AS forecast_year
),

lookback AS (
    SELECT
        fy.forecast_year,
        MAKE_DATE(fy.forecast_year, 1, 1)::DATE                              AS forecast_start,
        (MAKE_DATE(fy.forecast_year, 1, 1) - INTERVAL '24 months')::DATE   AS rolling_24m_start,
        (MAKE_DATE(fy.forecast_year, 1, 1) - INTERVAL '6 months')::DATE    AS rolling_6m_start
    FROM forecast_years fy
),

rolling_24m AS (
    SELECT
        lb.forecast_year,
        m.claim_frequency,
        m.paid_amount_uzs,
        m.total_claimed_loss_uzs
    FROM monthly m
    INNER JOIN lookback lb
        ON m.month_start_date >= lb.rolling_24m_start
       AND m.month_start_date < lb.forecast_start
),

forecast_h1_avg_premium AS (
    SELECT
        lb.forecast_year,
        AVG(m.avg_premium_uzs)                                              AS avg_insurance_premium_uzs
    FROM monthly m
    INNER JOIN lookback lb
        ON m.report_year = lb.forecast_year
       AND m.report_month <= 6
    WHERE m.avg_premium_uzs > 0
    GROUP BY lb.forecast_year
),

prior_6m_avg_premium AS (
    SELECT
        lb.forecast_year,
        AVG(m.avg_premium_uzs)                                              AS avg_insurance_premium_uzs
    FROM monthly m
    INNER JOIN lookback lb
        ON m.month_start_date >= lb.rolling_6m_start
       AND m.month_start_date < lb.forecast_start
    WHERE m.avg_premium_uzs > 0
    GROUP BY lb.forecast_year
),

avg_insurance_premium AS (
    SELECT
        fy.forecast_year,
        COALESCE(h.avg_insurance_premium_uzs, p6.avg_insurance_premium_uzs) AS avg_insurance_premium_uzs
    FROM forecast_years fy
    LEFT JOIN forecast_h1_avg_premium h
        ON h.forecast_year = fy.forecast_year
    LEFT JOIN prior_6m_avg_premium p6
        ON p6.forecast_year = fy.forecast_year
),

payment_to_znu AS (
    SELECT
        forecast_year,
        CASE
            WHEN SUM(total_claimed_loss_uzs) > 0
            THEN SUM(paid_amount_uzs) / SUM(total_claimed_loss_uzs)
            ELSE 0::NUMERIC
        END                                                                 AS payment_to_znu_share
    FROM rolling_24m
    GROUP BY forecast_year
),

claim_frequency_input AS (
    SELECT
        forecast_year,
        AVG(claim_frequency) FILTER (WHERE claim_frequency > 0)           AS claim_frequency
    FROM rolling_24m
    GROUP BY forecast_year
),

yearly_znu AS (
    SELECT
        lb.forecast_year,
        m.report_year,
        ROW_NUMBER() OVER (
            PARTITION BY lb.forecast_year
            ORDER BY m.report_year
        )                                                                   AS x_index,
        SUM(m.total_claimed_loss_uzs) / NULLIF(SUM(m.total_events), 0)  AS avg_znu_per_case_uzs
    FROM lookback lb
    INNER JOIN monthly m
        ON m.report_year IN (lb.forecast_year - 2, lb.forecast_year - 1)
    WHERE m.total_events > 0
    GROUP BY lb.forecast_year, m.report_year
),

znu_regression AS (
    SELECT
        forecast_year,
        COUNT(*)::NUMERIC                                                   AS n,
        SUM(x_index)::NUMERIC                                               AS sum_x,
        SUM(avg_znu_per_case_uzs)::NUMERIC                                  AS sum_y,
        SUM(x_index * avg_znu_per_case_uzs)::NUMERIC                        AS sum_xy,
        SUM(x_index * x_index)::NUMERIC                                     AS sum_x2
    FROM yearly_znu
    GROUP BY forecast_year
),

znu_forecast AS (
    SELECT
        r.forecast_year,
        CASE
            WHEN r.n >= 2
                 AND (r.n * r.sum_x2 - r.sum_x * r.sum_x) <> 0
            THEN (
                (r.sum_y - (
                    (r.n * r.sum_xy - r.sum_x * r.sum_y)
                    / (r.n * r.sum_x2 - r.sum_x * r.sum_x)
                ) * r.sum_x) / r.n
            ) + (
                (r.n * r.sum_xy - r.sum_x * r.sum_y)
                / (r.n * r.sum_x2 - r.sum_x * r.sum_x)
            ) * (r.n + 1)
            WHEN r.n = 1
            THEN (
                SELECT yz.avg_znu_per_case_uzs
                FROM yearly_znu yz
                WHERE yz.forecast_year = r.forecast_year
                LIMIT 1
            )
            ELSE NULL::NUMERIC
        END                                                                 AS forecast_avg_znu_per_case_uzs
    FROM znu_regression r
),

inputs AS (
    SELECT
        fy.forecast_year,
        ap.avg_insurance_premium_uzs,
        zf.forecast_avg_znu_per_case_uzs,
        COALESCE(cf.claim_frequency, 0)                                     AS claim_frequency,
        COALESCE(pz.payment_to_znu_share, 0)                                AS payment_to_znu_share
    FROM forecast_years fy
    LEFT JOIN avg_insurance_premium ap
        ON ap.forecast_year = fy.forecast_year
    LEFT JOIN znu_forecast zf
        ON zf.forecast_year = fy.forecast_year
    LEFT JOIN payment_to_znu pz
        ON pz.forecast_year = fy.forecast_year
    LEFT JOIN claim_frequency_input cf
        ON cf.forecast_year = fy.forecast_year
)

SELECT
    forecast_year,
    ROUND(COALESCE(avg_insurance_premium_uzs, 0)::NUMERIC, 2)               AS avg_insurance_premium_uzs,
    ROUND(COALESCE(forecast_avg_znu_per_case_uzs, 0)::NUMERIC, 2)         AS forecast_avg_znu_per_case_uzs,
    ROUND(claim_frequency::NUMERIC, 4)                                      AS claim_frequency,
    ROUND(payment_to_znu_share::NUMERIC, 4)                                 AS payment_to_znu_share,
    ROUND(
        (
            COALESCE(forecast_avg_znu_per_case_uzs, 0)
            * payment_to_znu_share
        )::NUMERIC, 2
    )                                                                       AS forecast_avg_payout_uzs,
    ROUND(
        (
            CASE
                WHEN COALESCE(avg_insurance_premium_uzs, 0) > 0
                THEN (
                    claim_frequency
                    * COALESCE(forecast_avg_znu_per_case_uzs, 0)
                    * payment_to_znu_share
                ) / avg_insurance_premium_uzs
                ELSE 0::NUMERIC
            END
        )::NUMERIC, 4
    )                                                                       AS forecast_loss_per_policy,
    'client_logic_applied'::TEXT                                            AS validation_status

FROM inputs

ORDER BY forecast_year
