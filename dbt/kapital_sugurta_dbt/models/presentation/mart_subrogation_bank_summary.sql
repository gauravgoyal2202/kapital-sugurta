{{ config(materialized='table') }}

/*
  Dashboard 8 — Subrogation and Banks Summary
  -----------------------------------------------------
  Rolling Year-over-Year Bank metrics.
  For every month, it provides the current value and the value from the same month
  in the prior year. This allows Power BI to slice by any Year/Month and see the
  correct comparison.

  MODIFIED:
    - Fetches directly from curated models to bypass buggy upstream joins.
    - Standardizes bank partner names using the partner_mapping seed.
    - Derives customer_segment (Physical/Juridical) from fizyur field in each curated model.
    - Aggregates metrics at the (bank_partner_name, product_category, customer_segment, report_month) level.
*/

WITH partner_map AS (
    SELECT * FROM {{ ref('curated_bank_partner_mapping') }}
),

standardized_partners AS (
    SELECT
        m.anketa_id,
        COALESCE(pm.standardized_partner_name, m.bank_partner_name) AS bank_partner_name,
        m.product_category
    FROM partner_map m
    LEFT JOIN (
        SELECT DISTINCT raw_partner_name, standardized_partner_name
        FROM {{ ref('partner_mapping') }}
    ) pm ON pm.raw_partner_name = m.bank_partner_name
),

-- 1. PREMIUMS
--    fizyur: 0 = Physical, else = Juridical (from curated_insurance_premium)
premiums_monthly AS (
    SELECT
        DATE_TRUNC('month', p.payment_date)::DATE                      AS report_month,
        COALESCE(sp.bank_partner_name, 'Direct/No Partner')            AS bank_partner_name,
        COALESCE(sp.product_category, 'Other')                         AS product_category,
        CASE WHEN COALESCE(p.fizyur, 0) = 0 THEN 'Physical' ELSE 'Juridical' END AS customer_segment,
        SUM(p.premium_amount)                                          AS premium
    FROM {{ ref('curated_insurance_premium') }} p
    LEFT JOIN standardized_partners sp ON sp.anketa_id = p.anketa_id
    WHERE p.payment_date IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

-- 2. COMMISSIONS
--    entity_type_flag = fizyur: 0 = Physical, else = Juridical (from curated_agency_commissions)
commissions_monthly AS (
    SELECT
        DATE_TRUNC('month', c.commission_date)::DATE                   AS report_month,
        COALESCE(sp.bank_partner_name, 'Direct/No Partner')            AS bank_partner_name,
        COALESCE(sp.product_category, 'Other')                         AS product_category,
        CASE WHEN COALESCE(c.entity_type_flag, 0) = 0 THEN 'Physical' ELSE 'Juridical' END AS customer_segment,
        SUM(c.commission_amount_uzs)                                   AS commission
    FROM {{ ref('curated_agency_commissions') }} c
    LEFT JOIN standardized_partners sp ON sp.anketa_id = c.anketa_id
    WHERE c.commission_date IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

-- 3. CLAIMS
--    fizyur not in curated_insurance_claims — join ins_anketa_oracle via anketa_id
claims_monthly AS (
    SELECT
        DATE_TRUNC('month', cl.payment_date)::DATE                     AS report_month,
        COALESCE(sp.bank_partner_name, 'Direct/No Partner')            AS bank_partner_name,
        COALESCE(sp.product_category, 'Other')                         AS product_category,
        CASE WHEN COALESCE(a.fizyur, 0) = 0 THEN 'Physical' ELSE 'Juridical' END AS customer_segment,
        SUM(cl.claim_amount)                                           AS claims
    FROM {{ ref('curated_insurance_claims') }} cl
    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a ON a.ins_id = cl.anketa_id
    LEFT JOIN standardized_partners sp ON sp.anketa_id = cl.anketa_id
    WHERE cl.payment_date IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

-- 4. TERMINATED CONTRACTS
--    fizyur not in curated_terminated_contracts — join ins_anketa_oracle via anketa_id
terminated_monthly AS (
    SELECT
        DATE_TRUNC('month', t.termination_date)::DATE                  AS report_month,
        COALESCE(sp.bank_partner_name, 'Direct/No Partner')            AS bank_partner_name,
        COALESCE(sp.product_category, 'Other')                         AS product_category,
        CASE WHEN COALESCE(a.fizyur, 0) = 0 THEN 'Physical' ELSE 'Juridical' END AS customer_segment,
        SUM(t.terminated_amount)                                       AS terminated
    FROM {{ ref('curated_terminated_contracts') }} t
    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a ON a.ins_id = t.anketa_id
    LEFT JOIN standardized_partners sp ON sp.anketa_id = t.anketa_id
    WHERE t.termination_date IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

-- 5. SUBROGATION RECOVERIES
--    fizyur directly in curated_subrogation_recoveries.
--    Uses total_recovery_amount to match mart_subrogation_indicators_monthly.
--    Uses bank_partner_name already resolved inside the curated model.
recoveries_monthly AS (
    SELECT
        DATE_TRUNC('month', r.recovery_payment_date)::DATE             AS report_month,
        COALESCE(sp.bank_partner_name, pm_fallback.standardized_partner_name, r.bank_partner_name, 'Direct/No Partner') AS bank_partner_name,
        COALESCE(r.product_category, 'Other')                          AS product_category,
        CASE WHEN COALESCE(r.fizyur, 0) = 0 THEN 'Physical' ELSE 'Juridical' END AS customer_segment,
        SUM(r.total_recovery_amount)                                   AS recovery,
        AVG(r.recovery_processing_time_days)                           AS avg_processing_time
    FROM {{ ref('curated_subrogation_recoveries') }} r
    LEFT JOIN standardized_partners sp ON sp.anketa_id = r.anketa_id
    LEFT JOIN (
        SELECT DISTINCT raw_partner_name, standardized_partner_name
        FROM {{ ref('partner_mapping') }}
    ) pm_fallback ON pm_fallback.raw_partner_name = r.bank_partner_name
    WHERE r.recovery_payment_date IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

-- UNIFIED SPINE
spine AS (
    SELECT report_month, bank_partner_name, product_category, customer_segment FROM premiums_monthly
    UNION
    SELECT report_month, bank_partner_name, product_category, customer_segment FROM commissions_monthly
    UNION
    SELECT report_month, bank_partner_name, product_category, customer_segment FROM claims_monthly
    UNION
    SELECT report_month, bank_partner_name, product_category, customer_segment FROM terminated_monthly
    UNION
    SELECT report_month, bank_partner_name, product_category, customer_segment FROM recoveries_monthly
),

base_data AS (
    SELECT
        s.bank_partner_name,
        s.product_category,
        s.customer_segment,
        s.report_month,
        EXTRACT(YEAR  FROM s.report_month) AS year_num,
        EXTRACT(MONTH FROM s.report_month) AS month_num,
        COALESCE(p.premium, 0)         AS premium,
        COALESCE(c.commission, 0)      AS commission,
        COALESCE(cl.claims, 0)         AS claims,
        COALESCE(t.terminated, 0)      AS terminated,
        COALESCE(r.recovery, 0)        AS recovery,
        COALESCE(r.avg_processing_time, 0) AS avg_processing_time
    FROM spine s
    LEFT JOIN premiums_monthly p
        ON  p.report_month     = s.report_month
        AND p.bank_partner_name = s.bank_partner_name
        AND p.product_category  = s.product_category
        AND p.customer_segment  = s.customer_segment
    LEFT JOIN commissions_monthly c
        ON  c.report_month     = s.report_month
        AND c.bank_partner_name = s.bank_partner_name
        AND c.product_category  = s.product_category
        AND c.customer_segment  = s.customer_segment
    LEFT JOIN claims_monthly cl
        ON  cl.report_month    = s.report_month
        AND cl.bank_partner_name = s.bank_partner_name
        AND cl.product_category  = s.product_category
        AND cl.customer_segment  = s.customer_segment
    LEFT JOIN terminated_monthly t
        ON  t.report_month     = s.report_month
        AND t.bank_partner_name = s.bank_partner_name
        AND t.product_category  = s.product_category
        AND t.customer_segment  = s.customer_segment
    LEFT JOIN recoveries_monthly r
        ON  r.report_month     = s.report_month
        AND r.bank_partner_name = s.bank_partner_name
        AND r.product_category  = s.product_category
        AND r.customer_segment  = s.customer_segment
)

SELECT
    COALESCE(curr.bank_partner_name, prev.bank_partner_name)           AS bank_name,
    COALESCE(curr.product_category, prev.product_category)             AS product_category,
    CASE COALESCE(curr.product_category, prev.product_category)
        WHEN 'Motor'    THEN 'Авто'
        WHEN 'Banking'  THEN 'Банкинг'
        WHEN 'Property' THEN 'Имущество'
        WHEN 'Other'    THEN 'Прочее'
        ELSE COALESCE(curr.product_category, prev.product_category)
    END AS product_category_ru,
    CASE COALESCE(curr.product_category, prev.product_category)
        WHEN 'Motor'    THEN 'Авто'
        WHEN 'Banking'  THEN 'Банк'
        WHEN 'Property' THEN 'Мулк'
        WHEN 'Other'    THEN 'Бошқа'
        ELSE COALESCE(curr.product_category, prev.product_category)
    END AS product_category_uz_cyrl,
    CASE COALESCE(curr.product_category, prev.product_category)
        WHEN 'Motor'    THEN 'Avto'
        WHEN 'Banking'  THEN 'Bank'
        WHEN 'Property' THEN 'Mulk'
        WHEN 'Other'    THEN 'Boshqa'
        ELSE COALESCE(curr.product_category, prev.product_category)
    END AS product_category_uz_latn,
    COALESCE(curr.customer_segment, prev.customer_segment)             AS customer_segment,
    CASE COALESCE(curr.customer_segment, prev.customer_segment)
        WHEN 'Physical'  THEN 'Физическое лицо'
        WHEN 'Juridical' THEN 'Юридическое лицо'
        ELSE COALESCE(curr.customer_segment, prev.customer_segment)
    END AS customer_segment_ru,
    CASE COALESCE(curr.customer_segment, prev.customer_segment)
        WHEN 'Physical'  THEN 'Жисмоний'
        WHEN 'Juridical' THEN 'Юридик'
        ELSE COALESCE(curr.customer_segment, prev.customer_segment)
    END AS customer_segment_uz_cyrl,
    CASE COALESCE(curr.customer_segment, prev.customer_segment)
        WHEN 'Physical'  THEN 'Jismoniy'
        WHEN 'Juridical' THEN 'Yuridik'
        ELSE COALESCE(curr.customer_segment, prev.customer_segment)
    END AS customer_segment_uz_latn,
    COALESCE(curr.report_month, (prev.report_month + INTERVAL '1 year')::DATE) AS report_month,
    EXTRACT(YEAR FROM COALESCE(curr.report_month, (prev.report_month + INTERVAL '1 year')::DATE))::INT AS report_year,
    (EXTRACT(YEAR FROM COALESCE(curr.report_month, (prev.report_month + INTERVAL '1 year')::DATE)) - 1)::INT AS prev_year,

    -- PREMIUMS (CY & PY)
    ROUND(COALESCE(curr.premium, 0)::NUMERIC, 3)                       AS insurance_premium_volume_curr_year_uzs,
    ROUND(COALESCE(prev.premium, 0)::NUMERIC, 3)                       AS insurance_premium_volume_prev_year_uzs,

    -- AGENCY COMMISSIONS (CY & PY)
    ROUND(COALESCE(curr.commission, 0)::NUMERIC, 3)                    AS agency_commission_volume_curr_year_uzs,
    ROUND(COALESCE(prev.commission, 0)::NUMERIC, 3)                    AS agency_commission_volume_prev_year_uzs,

    -- CLAIMS (CY & PY)
    ROUND(COALESCE(curr.claims, 0)::NUMERIC, 3)                        AS insurance_claims_volume_curr_year_uzs,
    ROUND(COALESCE(prev.claims, 0)::NUMERIC, 3)                        AS insurance_claims_volume_prev_year_uzs,

    -- TERMINATED CONTRACTS (CY & PY)
    ROUND(COALESCE(curr.terminated, 0)::NUMERIC, 3)                    AS terminated_contracts_volume_curr_year_uzs,
    ROUND(COALESCE(prev.terminated, 0)::NUMERIC, 3)                    AS terminated_contracts_volume_prev_year_uzs,

    -- SUBROGATION RECOVERY (CY & PY)
    ROUND(COALESCE(curr.recovery, 0)::NUMERIC, 3)                      AS subrogation_recovery_curr_year_uzs,
    ROUND(COALESCE(prev.recovery, 0)::NUMERIC, 3)                      AS subrogation_recovery_prev_year_uzs,

    -- NET PROFIT FORMULA (CY):
    --   Premium - Agency Commissions - Claims - Terminated + Subrogation Recovery
    ROUND(
        (
            COALESCE(curr.premium, 0)
          - COALESCE(curr.commission, 0)
          - COALESCE(curr.claims, 0)
          - COALESCE(curr.terminated, 0)
          + COALESCE(curr.recovery, 0)
        )::NUMERIC, 3
    ) AS net_profit_formula_curr_year_uzs,

    -- NET PROFIT FORMULA (PY)
    ROUND(
        (
            COALESCE(prev.premium, 0)
          - COALESCE(prev.commission, 0)
          - COALESCE(prev.claims, 0)
          - COALESCE(prev.terminated, 0)
          + COALESCE(prev.recovery, 0)
        )::NUMERIC, 3
    ) AS net_profit_formula_prev_year_uzs,

    -- AVG RECOVERY PROCESSING TIME
    ROUND(COALESCE(curr.avg_processing_time, 0)::NUMERIC, 0)           AS avg_recovery_processing_time_from_debt_occurrence_days

FROM base_data curr

-- Prior year self-join (same month, one year back)
FULL OUTER JOIN base_data prev
    ON  prev.bank_partner_name = curr.bank_partner_name
    AND prev.product_category  = curr.product_category
    AND prev.customer_segment  = curr.customer_segment
    AND prev.report_month      = (curr.report_month - INTERVAL '1 year')::DATE

ORDER BY report_month DESC, insurance_premium_volume_curr_year_uzs DESC
