{{ config(
    materialized = 'table',
    tags         = ['profitability', 'dashboard', 'reinsurance'],
    post_hook    = [
        "CREATE INDEX IF NOT EXISTS idx_cprm_month ON {{ this }} (report_month)"
    ]
) }}

/*
  Client earned outgoing reinsurance premium (monthly)
  ----------------------------------------------------
  Source: docs/queries/postgres_earned_premium_claims_reinsurance.sql (query 2)

  Company-wide metric (not ROAD24-specific). Join to quarterly General Portfolio only.
*/

WITH month_spine AS (
    SELECT
        DATE_TRUNC('month', d)::DATE                                          AS month_start,
        (DATE_TRUNC('month', d) + INTERVAL '1 month')::DATE                   AS month_end
    FROM generate_series(
        '2021-01-01'::DATE,
        DATE_TRUNC('month', CURRENT_DATE)::DATE,
        '1 month'::INTERVAL
    ) AS d
),

contract_dates AS (
    SELECT
        c.reinsurance_id,
        MIN(c.start_date)                                                     AS contract_start_date,
        MAX(c.end_date)                                                       AS contract_end_date
    FROM {{ source('raw', 'ins_reins_contract_oracle') }} c
    GROUP BY c.reinsurance_id
),

reinsurance_base AS (
    SELECT
        r.id                                                                  AS reinsurance_id,
        COALESCE(r.reinsurance_start_date, cd.contract_start_date)::DATE    AS reinsurance_begin_date,
        COALESCE(r.reinsurance_end_date, cd.contract_end_date)::DATE        AS reinsurance_end_date,
        COALESCE(r.netto_accrual_premium, 0)                                AS ceded_reinsurance_premium
    FROM {{ source('raw', 'ins_reinsurance_oracle') }} r
    LEFT JOIN contract_dates cd ON cd.reinsurance_id = r.id
    WHERE r.direction = 1
      AND COALESCE(r.netto_accrual_premium, 0) <> 0
      AND COALESCE(r.reinsurance_start_date, cd.contract_start_date) IS NOT NULL
      AND COALESCE(r.reinsurance_end_date, cd.contract_end_date) IS NOT NULL
),

calc_base AS (
    SELECT
        m.month_start,
        rb.ceded_reinsurance_premium,
        CASE
            WHEN rb.reinsurance_end_date < rb.reinsurance_begin_date THEN 0
            ELSE rb.reinsurance_end_date - rb.reinsurance_begin_date + 1
        END                                                                 AS total_reinsurance_days,
        GREATEST(
            0,
            LEAST(rb.reinsurance_end_date + 1, m.month_end)
            - GREATEST(rb.reinsurance_begin_date, m.month_start)
        )                                                                   AS earned_days
    FROM month_spine m
    JOIN reinsurance_base rb
        ON rb.reinsurance_begin_date < m.month_end
       AND rb.reinsurance_end_date   >= m.month_start
),

calc_factor AS (
    SELECT
        cb.*,
        CASE
            WHEN cb.total_reinsurance_days > 0
            THEN cb.earned_days::NUMERIC / cb.total_reinsurance_days
            ELSE 0
        END                                                                 AS earned_factor
    FROM calc_base cb
),

calc_final AS (
    SELECT
        cf.month_start,
        ROUND(
            COALESCE(cf.ceded_reinsurance_premium, 0) * cf.earned_factor,
            2
        )                                                                   AS ceded_earned_reinsurance_premium
    FROM calc_factor cf
    WHERE cf.earned_days > 0
)

SELECT
    month_start                                                           AS report_month,
    SUM(COALESCE(ceded_earned_reinsurance_premium, 0))                    AS ceded_earned_reinsurance_premium_uzs

FROM calc_final
GROUP BY month_start
