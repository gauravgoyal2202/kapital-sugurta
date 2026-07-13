{{config(
    materialized = 'table',
    post_hook    = [
      "CREATE INDEX IF NOT EXISTS idx_mbppm_month    ON {{ this }} (report_month)",
      "CREATE INDEX IF NOT EXISTS idx_mbppm_partner  ON {{ this }} (bank_partner_name)"
    ]
)}}

/*
  Dashboard 9 — Bottom Section: "Показатели агентских вознаграждений"
  mart_bank_partner_performance_monthly
  --------------------------------------------------------------------
  Grain: (report_month, bank_partner_name, insurance_type, product_category, product_name, sales_channel, agent_type, agent)

  FIXES APPLIED:
    1. Commission calc: changed INNER JOIN -> LEFT JOIN to channel_dims and used commission_date
       instead of payment_date to prevent OSAGO commission rows being silently dropped.
    2. Bank name standardization: partner_mapping seed applied to curated_bank_partner_mapping
       so branch names (e.g. "Чиланзарский филиал Ориент Финанс банка") are folded into
       standard bank names (e.g. "ОРИЕНТ ФИНАНС ОПЕРУ ЧАКБ БАНК").
*/

WITH raw_partner_map AS (
    SELECT * FROM {{ ref('curated_bank_partner_mapping') }}
),

-- Deduplicated partner_mapping seed (some raw names appear twice)
std_name_map AS (
    SELECT DISTINCT raw_partner_name, standardized_partner_name
    FROM {{ ref('partner_mapping') }}
),

-- Standardized partner map: applies seed normalization to raw bank names
partner_map AS (
    SELECT
        m.anketa_id,
        COALESCE(s.standardized_partner_name, m.bank_partner_name) AS bank_partner_name,
        m.product_category
    FROM raw_partner_map m
    LEFT JOIN std_name_map s ON s.raw_partner_name = m.bank_partner_name
),

-- 1. CENTRALIZED SALES CHANNEL & DIMENSIONS
channel_dims AS (
    SELECT
        payment_date,
        anketa_id,
        insurance_type,
        product_category,
        product_name,
        sales_channel,
        agent_type,
        agent_name
    FROM {{ ref('curated_sales_channels') }}
    WHERE payment_date IS NOT NULL
),

-- 1b. DEDUPLICATED DIMENSIONS FOR COMMISSION JOIN
deduped_channel_dims AS (
    SELECT DISTINCT ON (anketa_id)
        anketa_id,
        insurance_type,
        product_category,
        product_name,
        sales_channel,
        agent_type,
        agent_name
    FROM {{ ref('curated_sales_channels') }}
    ORDER BY anketa_id, payment_date DESC
),

-- 2. PREMIUMS
premium_agg AS (
    SELECT
        DATE_TRUNC('month', sc.payment_date)::DATE AS report_month,
        pm.bank_partner_name,
        CASE WHEN p.fizyur = 0 THEN 'Physical' ELSE 'Juridical' END AS customer_segment,
        sc.insurance_type,
        sc.product_category,
        sc.product_name,
        sc.sales_channel,
        sc.agent_type,
        sc.agent_name,
        SUM(p.premium_amount) AS insurance_premium_volume_uzs
    FROM {{ ref('curated_insurance_premium') }} p
    JOIN channel_dims sc ON sc.anketa_id = p.anketa_id AND sc.payment_date = p.payment_date
    LEFT JOIN partner_map pm ON pm.anketa_id = p.anketa_id
    WHERE p.payment_date IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

-- 3. COMMISSIONS
--    FIX: Use commission_date (not payment_date) and LEFT JOIN to deduped_channel_dims
--    so OSAGO commissions (from tb_oplata_oracle) are NOT dropped and not duplicated.
--    curated_agency_commissions already carries product_category and entity_type_flag.
commission_agg AS (
    SELECT
        DATE_TRUNC('month', cm.commission_date)::DATE AS report_month,
        pm.bank_partner_name,
        CASE WHEN cm.entity_type_flag = 0 THEN 'Physical' ELSE 'Juridical' END AS customer_segment,
        COALESCE(sc.insurance_type,    'Other')          AS insurance_type,
        COALESCE(sc.product_category,  cm.product_category, 'Other') AS product_category,
        COALESCE(sc.product_name,      'Other')          AS product_name,
        COALESCE(sc.sales_channel,     'Other')          AS sales_channel,
        COALESCE(sc.agent_type,        'Other')          AS agent_type,
        COALESCE(sc.agent_name,        'Other')          AS agent_name,
        SUM(cm.commission_amount_uzs) AS agency_commission_volume_uzs
    FROM {{ ref('curated_agency_commissions') }} cm
    LEFT JOIN deduped_channel_dims sc ON sc.anketa_id = cm.anketa_id
    LEFT JOIN partner_map pm  ON pm.anketa_id  = cm.anketa_id
    WHERE cm.commission_date IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

-- 4. CLAIMS
claims_agg AS (
    SELECT
        DATE_TRUNC('month', v.date_viplata)::DATE AS report_month,
        pm.bank_partner_name,
        CASE WHEN a.fizyur = 0 THEN 'Physical' ELSE 'Juridical' END AS customer_segment,
        CASE WHEN pt.mandatory = 1 THEN 'Mandatory' ELSE 'Voluntary' END as insurance_type,
        COALESCE(vert.name3, 'Other') as product_category,
        COALESCE(pt.polis_name, 'Other') as product_name,
        'N/A' as sales_channel,
        'N/A' as agent_type,
        'N/A' as agent_name,
        SUM(CASE WHEN COALESCE(v.val_type, 1) = 1 THEN COALESCE(v.viplate, 0) ELSE COALESCE(v.viplate, 0) * COALESCE(v.val_kurs, 1) END) AS insurance_claims_volume_uzs,
        SUM(CASE WHEN COALESCE(v.val_type, 1) = 1 THEN COALESCE(v.usluga_sum, 0) ELSE COALESCE(v.usluga_sum, 0) * COALESCE(v.val_kurs, 1) END) AS other_payments_volume_uzs
    FROM {{ source('raw', 'ins_viplati_oracle') }} v
    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a ON a.ins_id = v.anketa_id
    LEFT JOIN {{ source('raw', 'ins_polis_oracle') }} pol ON pol.tb_anketa = v.anketa_id
    LEFT JOIN {{ source('raw', 'ins_pturi_oracle') }} pt ON pt.ins_id = pol.pturi_id
    LEFT JOIN {{ source('raw', 'ins_vertical_oracle') }} vert ON vert.ins_id = pt.vertical
    LEFT JOIN partner_map pm ON pm.anketa_id = v.anketa_id
    WHERE v.date_viplata IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

-- 5. RECOVERIES
recovery_agg AS (
    SELECT
        DATE_TRUNC('month', s.recovery_payment_date)::DATE AS report_month,
        pm.bank_partner_name,
        CASE WHEN s.fizyur = 0 THEN 'Physical' ELSE 'Juridical' END AS customer_segment,
        'Voluntary' as insurance_type,
        'Motor' as product_category,
        'Auto Subrogation' as product_name,
        'N/A' as sales_channel,
        'N/A' as agent_type,
        'N/A' as agent_name,
        SUM(s.exact_recovery_amount) AS recovery_from_bank_uzs,
        AVG(s.recovery_processing_time_days) AS avg_recovery_processing_time_days
    FROM {{ ref('curated_subrogation_recoveries') }} s
    LEFT JOIN partner_map pm ON pm.anketa_id = s.anketa_id
    WHERE s.recovery_payment_date IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

-- UNIFIED SPINE
spine AS (
    SELECT report_month, bank_partner_name, customer_segment, insurance_type, product_category, product_name, sales_channel, agent_type, agent_name FROM premium_agg
    UNION
    SELECT report_month, bank_partner_name, customer_segment, insurance_type, product_category, product_name, sales_channel, agent_type, agent_name FROM commission_agg
    UNION
    SELECT report_month, bank_partner_name, customer_segment, insurance_type, product_category, product_name, sales_channel, agent_type, agent_name FROM claims_agg
    UNION
    SELECT report_month, bank_partner_name, customer_segment, insurance_type, product_category, product_name, sales_channel, agent_type, agent_name FROM recovery_agg
)

SELECT
    s.report_month,
    s.bank_partner_name,
    s.customer_segment,
    s.insurance_type,
    s.product_category,
    s.product_name,
    s.sales_channel,
    s.agent_type,
    s.agent_name as agent,
    'Actual' as scenario,

    COALESCE(p.insurance_premium_volume_uzs, 0)   AS insurance_premium_volume_uzs,
    COALESCE(cm.agency_commission_volume_uzs, 0)   AS agency_commission_volume_uzs,
    COALESCE(cl.insurance_claims_volume_uzs, 0)    AS insurance_claims_volume_uzs,
    (COALESCE(p.insurance_premium_volume_uzs, 0) - COALESCE(cm.agency_commission_volume_uzs, 0) - COALESCE(cl.insurance_claims_volume_uzs, 0)) AS insurance_profit_volume_uzs,
    COALESCE(cl.other_payments_volume_uzs, 0)      AS other_payments_volume_uzs,
    COALESCE(r.recovery_from_bank_uzs, 0)          AS recovery_from_bank_uzs,
    COALESCE(r.avg_recovery_processing_time_days, 0) AS avg_recovery_processing_time_days

FROM spine s
LEFT JOIN premium_agg p
    ON s.report_month = p.report_month AND s.bank_partner_name = p.bank_partner_name AND s.customer_segment = p.customer_segment
    AND s.insurance_type = p.insurance_type AND s.product_category = p.product_category AND s.product_name = p.product_name
    AND s.sales_channel = p.sales_channel AND s.agent_type = p.agent_type AND s.agent_name = p.agent_name
LEFT JOIN commission_agg cm
    ON s.report_month = cm.report_month AND s.bank_partner_name = cm.bank_partner_name AND s.customer_segment = cm.customer_segment
    AND s.insurance_type = cm.insurance_type AND s.product_category = cm.product_category AND s.product_name = cm.product_name
    AND s.sales_channel = cm.sales_channel AND s.agent_type = cm.agent_type AND s.agent_name = cm.agent_name
LEFT JOIN claims_agg cl
    ON s.report_month = cl.report_month AND s.bank_partner_name = cl.bank_partner_name AND s.customer_segment = cl.customer_segment
    AND s.insurance_type = cl.insurance_type AND s.product_category = cl.product_category AND s.product_name = cl.product_name
    AND s.sales_channel = cl.sales_channel AND s.agent_type = cl.agent_type AND s.agent_name = cl.agent_name
LEFT JOIN recovery_agg r
    ON s.report_month = r.report_month AND s.bank_partner_name = r.bank_partner_name AND s.customer_segment = r.customer_segment
    AND s.insurance_type = r.insurance_type AND s.product_category = r.product_category AND s.product_name = r.product_name
    AND s.sales_channel = r.sales_channel AND s.agent_type = r.agent_type AND s.agent_name = r.agent_name

WHERE s.report_month IS NOT NULL
ORDER BY s.report_month DESC, s.bank_partner_name ASC
