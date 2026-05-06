{{ config(materialized='table') }}

/*
  Dashboard 9 — Bottom Section: "Показатели агентских вознаграждений"
  mart_bank_partner_performance_monthly
  --------------------------------------------------------------------
  Grain: (report_month, bank_partner_name, product_category)

  This mart supports two tabs in Dashboard 9:
  1. Bank-Partner (group by bank_partner_name)
  2. Product Category (group by product_category)

  METRICS:
    - insurance_premium_volume_uzs
    - agency_commission_volume_uzs
    - insurance_claims_volume_uzs
    - other_payments_volume_uzs
    - recovery_from_bank_uzs
    - avg_recovery_processing_time_days
*/

WITH partner_map AS (
    SELECT * FROM {{ ref('curated_bank_partner_mapping') }}
),

-- 1. PREMIUMS
premium_agg AS (
    SELECT
        DATE_TRUNC('month', p.payment_date)::DATE AS report_month,
        pm.bank_partner_name,
        pm.product_category,
        CASE WHEN p.fizyur = 0 THEN 'Physical' ELSE 'Juridical' END AS customer_segment,
        SUM(p.premium_amount) AS insurance_premium_volume_uzs
    FROM {{ ref('curated_insurance_premium') }} p
    LEFT JOIN partner_map pm ON pm.anketa_id = p.anketa_id
    WHERE p.payment_date IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

-- 2. COMMISSIONS
commission_agg AS (
    SELECT
        DATE_TRUNC('month', cm.commission_date)::DATE AS report_month,
        pm.bank_partner_name,
        pm.product_category,
        CASE WHEN cm.entity_type_flag = 0 THEN 'Physical' ELSE 'Juridical' END AS customer_segment,
        SUM(cm.commission_amount_uzs) AS agency_commission_volume_uzs
    FROM {{ ref('curated_agency_commissions') }} cm
    -- Re-joining on partner map to ensure consistent bank partner names across all metrics
    -- (The commission model has its own logic, but we align here for the mart)
    LEFT JOIN partner_map pm ON pm.anketa_id = cm.anketa_id
    WHERE cm.commission_date IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

-- 3. CLAIMS & OTHER PAYMENTS
claims_agg AS (
    SELECT
        DATE_TRUNC('month', v.date_viplata)::DATE AS report_month,
        pm.bank_partner_name,
        pm.product_category,
        CASE WHEN a.fizyur = 0 THEN 'Physical' ELSE 'Juridical' END AS customer_segment,
        SUM(CASE WHEN COALESCE(v.val_type, 1) = 1 THEN COALESCE(v.viplate, 0) ELSE COALESCE(v.viplate, 0) * COALESCE(v.val_kurs, 1) END) AS insurance_claims_volume_uzs,
        SUM(CASE WHEN COALESCE(v.val_type, 1) = 1 THEN COALESCE(v.usluga_sum, 0) ELSE COALESCE(v.usluga_sum, 0) * COALESCE(v.val_kurs, 1) END) AS other_payments_volume_uzs
    FROM {{ source('raw', 'ins_viplati_oracle') }} v
    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a ON a.ins_id = v.anketa_id
    LEFT JOIN partner_map pm ON pm.anketa_id = v.anketa_id
    WHERE v.date_viplata IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

-- 4. RECOVERIES (SUBROGATION)
recovery_agg AS (
    SELECT
        DATE_TRUNC('month', s.recovery_payment_date)::DATE AS report_month,
        pm.bank_partner_name,
        pm.product_category,
        CASE WHEN s.fizyur = 0 THEN 'Physical' ELSE 'Juridical' END AS customer_segment,
        SUM(s.exact_recovery_amount) AS recovery_from_bank_uzs,
        AVG(s.recovery_processing_time_days) AS avg_recovery_processing_time_days
    FROM {{ ref('curated_subrogation_recoveries') }} s
    -- Subrogation already has bank_partner_name, but we align via anketa_id if possible.
    -- However, curated_subrogation_recoveries is aggregated. 
    -- Let's use the bank_partner_name directly since it was already derived from Beneficiary in its curated layer.
    LEFT JOIN partner_map pm ON pm.anketa_id = s.anketa_id
    WHERE s.recovery_payment_date IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

-- UNIFIED SPINE
spine AS (
    SELECT report_month, bank_partner_name, product_category, customer_segment FROM premium_agg
    UNION
    SELECT report_month, bank_partner_name, product_category, customer_segment FROM commission_agg
    UNION
    SELECT report_month, bank_partner_name, product_category, customer_segment FROM claims_agg
    UNION
    SELECT report_month, bank_partner_name, product_category, customer_segment FROM recovery_agg
)

SELECT
    s.report_month,
    s.bank_partner_name,
    s.product_category,
    s.customer_segment,
    COALESCE(p.insurance_premium_volume_uzs, 0) AS insurance_premium_volume_uzs,
    COALESCE(cm.agency_commission_volume_uzs, 0) AS agency_commission_volume_uzs,
    COALESCE(cl.insurance_claims_volume_uzs, 0) AS insurance_claims_volume_uzs,
    (COALESCE(p.insurance_premium_volume_uzs, 0) - COALESCE(cm.agency_commission_volume_uzs, 0) - COALESCE(cl.insurance_claims_volume_uzs, 0)) AS insurance_profit_volume_uzs,
    COALESCE(cl.other_payments_volume_uzs, 0) AS other_payments_volume_uzs,
    COALESCE(r.recovery_from_bank_uzs, 0) AS recovery_from_bank_uzs,
    COALESCE(r.avg_recovery_processing_time_days, 0) AS avg_recovery_processing_time_days
    
FROM spine s
LEFT JOIN premium_agg p 
    ON s.report_month = p.report_month AND s.bank_partner_name = p.bank_partner_name AND s.product_category = p.product_category AND s.customer_segment = p.customer_segment
LEFT JOIN commission_agg cm 
    ON s.report_month = cm.report_month AND s.bank_partner_name = cm.bank_partner_name AND s.product_category = cm.product_category AND s.customer_segment = cm.customer_segment
LEFT JOIN claims_agg cl 
    ON s.report_month = cl.report_month AND s.bank_partner_name = cl.bank_partner_name AND s.product_category = cl.product_category AND s.customer_segment = cl.customer_segment
LEFT JOIN recovery_agg r 
    ON s.report_month = r.report_month AND s.bank_partner_name = r.bank_partner_name AND s.product_category = r.product_category AND s.customer_segment = r.customer_segment

ORDER BY s.report_month DESC, s.bank_partner_name ASC
