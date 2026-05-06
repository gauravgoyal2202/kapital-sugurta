{{ config(materialized='table') }}

/*
  Dashboard 19 — HR Analytics (eNPS and Turnover)
  -----------------------------------------------
  Top Chart: Employee NPS (eNPS)
  Bottom Chart: Employee Turnover Rate
  
  eNPS = % Promoters - % Detractors (Employee Survey)
  Turnover % = Exits / ((Headcount Start + Headcount End) / 2)
*/

WITH months AS (
    SELECT DATE_TRUNC('month', d)::DATE as report_month
    FROM generate_series('2024-01-01'::DATE, '2026-12-31'::DATE, '1 month'::interval) d
),

enps_data AS (
    SELECT
        report_month,
        survey_type,
        COUNT(*) as total_responses,
        SUM(CASE WHEN nps_category = 'Promoter' THEN 1 ELSE 0 END) as promoters,
        SUM(CASE WHEN nps_category = 'Detractor' THEN 1 ELSE 0 END) as detractors
    FROM {{ ref('curated_nps_survey') }}
    WHERE survey_type = 'Employee NPS'
    GROUP BY 1, 2
),

employee_base AS (
    SELECT
        e.ins_id,
        e.post_date::DATE as hire_date,
        e.fired,
        e.fired_date::DATE as termination_date,
        COALESCE(d.sp_name1, 'Head Office') as branch_name,
        'Internal'::VARCHAR as department_name -- Placeholder until department mapping is confirmed
    FROM {{ source('raw', 'ins_employee_oracle') }} e
    LEFT JOIN {{ source('raw', 'sp_division_oracle') }} d ON e.division_id = d.sp_id
),

monthly_turnover AS (
    SELECT
        m.report_month,
        eb.branch_name,
        eb.department_name,
        
        -- Exits in this month
        COUNT(DISTINCT CASE 
            WHEN eb.fired = 1 
             AND eb.termination_date >= m.report_month 
             AND eb.termination_date < (m.report_month + INTERVAL '1 month')::DATE 
            THEN eb.ins_id 
        END) as exits,
        
        -- Headcount at Start of Month
        COUNT(DISTINCT CASE 
            WHEN eb.hire_date < m.report_month 
             AND (eb.fired = 0 OR eb.termination_date >= m.report_month) 
            THEN eb.ins_id 
        END) as headcount_start,
        
        -- Headcount at End of Month
        COUNT(DISTINCT CASE 
            WHEN eb.hire_date < (m.report_month + INTERVAL '1 month')::DATE 
             AND (eb.fired = 0 OR eb.termination_date >= (m.report_month + INTERVAL '1 month')::DATE) 
            THEN eb.ins_id 
        END) as headcount_end
        
    FROM months m
    CROSS JOIN employee_base eb
    GROUP BY 1, 2, 3
)

SELECT
    t.report_month,
    EXTRACT(YEAR FROM t.report_month)::INT as report_year,
    EXTRACT(QUARTER FROM t.report_month)::INT as report_quarter,
    t.branch_name,
    t.department_name,
    'Actual'::VARCHAR as scenario,
    
    -- Turnover Metrics
    t.exits,
    t.headcount_start,
    t.headcount_end,
    ROUND((t.headcount_start + t.headcount_end)::NUMERIC / 2, 1) as avg_headcount,
    
    CASE 
        WHEN (t.headcount_start + t.headcount_end) > 0 
        THEN ROUND((t.exits::NUMERIC / NULLIF((t.headcount_start + t.headcount_end)::NUMERIC / 2, 0)) * 100, 2)
        ELSE 0 
    END as turnover_rate_pct,
    
    -- eNPS Metrics (Aggregated at month level, branches/depts not available in current survey)
    COALESCE(e.total_responses, 0) as enps_total_responses,
    CASE 
        WHEN e.total_responses > 0 
        THEN ROUND(((e.promoters - e.detractors)::NUMERIC / e.total_responses) * 100, 1)
        ELSE 0 
    END as enps_index

FROM monthly_turnover t
LEFT JOIN enps_data e ON t.report_month = e.report_month
ORDER BY t.report_month DESC, t.branch_name
