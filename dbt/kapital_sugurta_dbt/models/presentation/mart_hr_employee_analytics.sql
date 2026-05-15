{{ config(materialized='table') }}

/*
  Dashboard 19 — HR Analytics (Employee Turnover & NPS)
  -----------------------------------------------------
  Updated Logic: Simplified monthly aggregate as per client request,
  with Branch/Department filters and Employee NPS integration.
*/

WITH months AS (
    SELECT
        DATE_TRUNC('month', d)::DATE AS month_start,
        (
            DATE_TRUNC('month', d)
            + INTERVAL '1 month'
            - INTERVAL '1 day'
        )::DATE AS month_end
    FROM generate_series(
        '2025-01-01'::DATE,
        '2026-12-01'::DATE,
        '1 month'::INTERVAL
    ) d
),

dept_branch_spine AS (
    SELECT DISTINCT
        COALESCE(dept.name1, 'Unknown') AS department_name,
        COALESCE(div.sp_name1, 'Head Office') AS branch_name
    FROM {{ source('raw', 'ins_employee_oracle') }} e
    LEFT JOIN {{ source('raw', 'ins_department_oracle') }} dept ON e.department_id = dept.ins_id
    LEFT JOIN {{ source('raw', 'sp_division_oracle') }} div ON e.division_id = div.sp_id
),

monthly_actions AS (
    SELECT
        DATE_TRUNC('month', e.post_date)::DATE AS report_month,
        COALESCE(dept.name1, 'Unknown') AS department_name,
        COALESCE(div.sp_name1, 'Head Office') AS branch_name,
        COUNT(CASE WHEN e.action = 0 THEN 1 END) AS hired_count,
        COUNT(CASE WHEN e.action = 2 THEN 1 END) AS fired_count,
        COUNT(CASE WHEN e.action = 1 THEN 1 END) AS reassigned_count
    FROM {{ source('raw', 'ins_employee_oracle') }} e
    LEFT JOIN {{ source('raw', 'ins_department_oracle') }} dept ON e.department_id = dept.ins_id
    LEFT JOIN {{ source('raw', 'sp_division_oracle') }} div ON e.division_id = div.sp_id
    WHERE e.post_date >= DATE '2025-01-01'
      AND e.post_date < DATE '2027-01-01'
    GROUP BY 1, 2, 3
),

headcount_start AS (
    SELECT
        m.month_start,
        COALESCE(dept.name1, 'Unknown') AS department_name,
        COALESCE(div.sp_name1, 'Head Office') AS branch_name,
        COUNT(*) AS headcount_start
    FROM months m
    JOIN (
        SELECT
            m2.month_start,
            e.pers_id,
            e.action,
            e.department_id,
            e.division_id,
            ROW_NUMBER() OVER (
                PARTITION BY m2.month_start, e.pers_id
                ORDER BY e.post_date DESC, e.ins_id DESC
            ) AS rn
        FROM months m2
        JOIN {{ source('raw', 'ins_employee_oracle') }} e
            ON e.post_date < m2.month_start
    ) x
        ON x.month_start = m.month_start
       AND x.rn = 1
       AND x.action IN (0, 1)
    LEFT JOIN {{ source('raw', 'ins_department_oracle') }} dept ON x.department_id = dept.ins_id
    LEFT JOIN {{ source('raw', 'sp_division_oracle') }} div ON x.division_id = div.sp_id
    GROUP BY 1, 2, 3
),

headcount_end AS (
    SELECT
        m.month_start,
        COALESCE(dept.name1, 'Unknown') AS department_name,
        COALESCE(div.sp_name1, 'Head Office') AS branch_name,
        COUNT(*) AS headcount_end
    FROM months m
    JOIN (
        SELECT
            m2.month_start,
            e.pers_id,
            e.action,
            e.department_id,
            e.division_id,
            ROW_NUMBER() OVER (
                PARTITION BY m2.month_start, e.pers_id
                ORDER BY e.post_date DESC, e.ins_id DESC
            ) AS rn
        FROM months m2
        JOIN {{ source('raw', 'ins_employee_oracle') }} e
            ON e.post_date <= m2.month_end
    ) x
        ON x.month_start = m.month_start
       AND x.rn = 1
       AND x.action IN (0, 1)
    LEFT JOIN {{ source('raw', 'ins_department_oracle') }} dept ON x.department_id = dept.ins_id
    LEFT JOIN {{ source('raw', 'sp_division_oracle') }} div ON x.division_id = div.sp_id
    GROUP BY 1, 2, 3
),

nps_data AS (
    SELECT
        DATE_TRUNC('month', response_timestamp)::DATE as report_month,
        COUNT(*) as total_responses,
        SUM(CASE WHEN nps_category = 'Promoter' THEN 1 ELSE 0 END) as promoters,
        SUM(CASE WHEN nps_category = 'Detractor' THEN 1 ELSE 0 END) as detractors
    FROM {{ ref('curated_nps_survey') }}
    WHERE survey_type = 'Employee NPS'
    GROUP BY 1
)

SELECT
    m.month_start::DATE AS report_month,
    ds.branch_name,
    ds.department_name,
    COALESCE(ma.hired_count, 0) AS hired_count,
    COALESCE(ma.fired_count, 0) AS fired_count,
    COALESCE(ma.reassigned_count, 0) AS reassigned_count,
    COALESCE(hs.headcount_start, 0) AS headcount_start,
    COALESCE(he.headcount_end, 0) AS headcount_end,
    ROUND(
        (
            COALESCE(hs.headcount_start, 0)
            + COALESCE(he.headcount_end, 0)
        )::NUMERIC / 2,
        2
    ) AS average_headcount,

    ROUND(
        (
            COALESCE(ma.fired_count, 0)::NUMERIC
            /
            NULLIF(
                (
                    COALESCE(hs.headcount_start, 0)
                    + COALESCE(he.headcount_end, 0)
                )::NUMERIC / 2,
                0
            )
        ) * 100,
        2
    ) AS employee_turnover_rate_pct,
    'Actual' AS scenario,
    
    -- Employee NPS Metric
    COALESCE(n.total_responses, 0) as nps_total_responses,
    CASE 
        WHEN n.total_responses > 0 
        THEN ROUND(((n.promoters - n.detractors)::NUMERIC / n.total_responses) * 100, 1)
        ELSE 0 
    END as nps_index

FROM months m
CROSS JOIN dept_branch_spine ds
LEFT JOIN monthly_actions ma
    ON ma.report_month = m.month_start AND ma.branch_name = ds.branch_name AND ma.department_name = ds.department_name
LEFT JOIN headcount_start hs
    ON hs.month_start = m.month_start AND hs.branch_name = ds.branch_name AND hs.department_name = ds.department_name
LEFT JOIN headcount_end he
    ON he.month_start = m.month_start AND he.branch_name = ds.branch_name AND he.department_name = ds.department_name
LEFT JOIN nps_data n 
    ON m.month_start = n.report_month

WHERE hs.headcount_start > 0 OR he.headcount_end > 0 OR ma.hired_count > 0 OR ma.fired_count > 0
ORDER BY m.month_start DESC, ds.branch_name
