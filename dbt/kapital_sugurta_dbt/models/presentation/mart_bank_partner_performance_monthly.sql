{{ config(materialized='table') }}

WITH premium_by_bank AS (
    SELECT
        DATE_TRUNC('month', p.payment_date)::DATE AS report_month,
        -- Dynamically extracting partner names securely via native join instead of fragile PLPGSQL context functions
        COALESCE(NULLIF(TRIM(k.tb_orgname), ''), NULLIF(TRIM(k.tb_name), ''), 'Direct/No Partner') AS bank_partner_name,
        SUM(p.premium_amount) AS insurance_premium_volume_uzs
        
    FROM {{ ref('curated_insurance_premium') }} p
    LEFT JOIN {{ source('raw', 'ins_anketa_oracle') }} a ON a.ins_id = p.anketa_id
    LEFT JOIN {{ source('raw', 'ins_kontragent_oracle') }} k ON a.owner = k.tb_id
    GROUP BY 1, 2
),

claims_by_bank AS (
    SELECT
        DATE_TRUNC('month', c.payout_date)::DATE AS report_month,
        -- Relying on existing curated claims logic for partner associations 
        COALESCE(c.client_name, 'Direct/No Partner') AS bank_partner_name,
        SUM(c.payout_total) AS insurance_claims_volume_uzs
    FROM {{ ref('curated_claims_portfolio') }} c
    GROUP BY 1, 2
),

subrogation_by_bank AS (
    SELECT
        DATE_TRUNC('month', s.recovery_payment_date)::DATE AS report_month,
        
        -- Defaulting to a safe aggregate until client confirms exact bank joining pathway for undetermined regs
        'General Recovery' AS bank_partner_name, 
        
        -- Exporting only exact sums as per dashboard subrogation column definitions
        SUM(s.exact_recovery_amount) AS recovery_from_bank_uzs
    FROM {{ ref('curated_subrogation_recoveries') }} s
    GROUP BY 1, 2
),

commissions_by_bank AS (
    SELECT
        DATE_TRUNC('month', cm.commission_date)::DATE AS report_month,
        COALESCE(cm.bank_partner_name, 'Direct/No Partner') AS bank_partner_name,
        SUM(cm.commission_amount_uzs) AS agency_commission_volume_uzs
    FROM {{ ref('curated_agency_commissions') }} cm
    GROUP BY 1, 2
),

time_bank_spine AS (
    SELECT report_month, bank_partner_name FROM premium_by_bank
    UNION
    SELECT report_month, bank_partner_name FROM claims_by_bank
    UNION
    SELECT report_month, bank_partner_name FROM subrogation_by_bank
    UNION
    SELECT report_month, bank_partner_name FROM commissions_by_bank
)

SELECT
    s.report_month,
    s.bank_partner_name,
    COALESCE(p.insurance_premium_volume_uzs, 0) AS insurance_premium_volume_uzs,
    COALESCE(cm.agency_commission_volume_uzs, 0) AS agency_commission_volume_uzs,
    COALESCE(c.insurance_claims_volume_uzs, 0) AS insurance_claims_volume_uzs,
    
    -- Explicit NULL placeholder pending translation/Oracle definition from client on "Other Payouts"
    NULL::NUMERIC AS insurance_profit_volume_uzs, 
    
    COALESCE(sb.recovery_from_bank_uzs, 0) AS recovery_from_bank_uzs,
    
    -- Explicit NULL placeholder pending client confirmation of debt occurrence dates
    NULL::NUMERIC AS avg_recovery_processing_time_days
    
FROM time_bank_spine s
LEFT JOIN premium_by_bank p 
    ON s.report_month = p.report_month AND s.bank_partner_name = p.bank_partner_name
LEFT JOIN claims_by_bank c 
    ON s.report_month = c.report_month AND s.bank_partner_name = c.bank_partner_name
LEFT JOIN subrogation_by_bank sb 
    ON s.report_month = sb.report_month AND s.bank_partner_name = sb.bank_partner_name
LEFT JOIN commissions_by_bank cm 
    ON s.report_month = cm.report_month AND s.bank_partner_name = cm.bank_partner_name
ORDER BY s.report_month DESC, s.bank_partner_name ASC
