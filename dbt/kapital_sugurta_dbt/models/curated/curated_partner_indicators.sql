{{ config(materialized='view') }}

/*
  Partner Performance & Profitability (Curated)
  --------------------------------------------
  Optimized Version: Pre-aggregates claims and reinsurance
  to avoid correlated subqueries in the final projection.
*/

WITH partner_whitelist AS (
    SELECT 
        tb_id as partner_id,
        TRIM(tb_surname) as partner_surname,
        TRIM(tb_name) as partner_name,
        is_partner_api,
        is_marketplace,
        CASE 
            WHEN (UPPER(tb_surname) LIKE '%BANK%' OR UPPER(tb_name) LIKE '%BANK%') THEN 'Banks'
            WHEN is_marketplace = 1 THEN 'Marketplaces'
            ELSE 'API Partners'
        END AS partner_category
    FROM {{ source('raw', 'tb_users_oracle') }}
    WHERE tb_id IN (82793,64788,40791,21741,20845,20829,20827,20732,20731,20729,20704,20574,20546,20471,20282,20326,20240,20325,19626,20323,19998,19887,19768,20322,19459,20318,19417,20317,19366,20504,20538,20553,20554,20562,20645,20728,20771,20822,20828,20904,21410,21409,21740,21739,21774,40795,72788,82792,82794,82795)
),

premiums_commissions AS (
    SELECT
        o.user_id as partner_id,
        o.anketa_id,
        o.opl_data::DATE as report_date,
        (CASE WHEN o.opl_val = 1 THEN o.oplata ELSE o.opl_summa * COALESCE(o.val_kurs, 1) END)::NUMERIC AS premium_amount,
        (CASE WHEN o.opl_val = 1 THEN o.kommis_summa ELSE o.kommis_summa * COALESCE(o.val_kurs, 1) END)::NUMERIC AS commission_amount
    FROM {{ source('raw', 'ins_oplata_oracle') }} o
    WHERE o.opl_type <> '6'
      AND o.bc_id IS NOT NULL
      AND o.user_id IN (SELECT partner_id FROM partner_whitelist)
),

-- Pre-aggregated Claims
claims_agg AS (
    SELECT
        v.anketa_id,
        SUM(COALESCE(v.viplate, 0) * COALESCE(v.val_kurs, 1))::NUMERIC as claims_amount
    FROM {{ source('raw', 'ins_viplati_oracle') }} v
    WHERE v.date_viplata IS NOT NULL
    GROUP BY 1
),

-- Pre-aggregated Reinsurance
reinsurance_agg AS (
    SELECT
        pol.tb_anketa as anketa_id,
        SUM(COALESCE(rp.total_premiums_ceded_uzs, 0)) as reinsurance_amount
    FROM {{ ref('curated_reinsurance_outgoing_portfolio') }} rp
    JOIN {{ source('raw', 'ins_polis_oracle') }} pol ON pol.tb_number::TEXT = rp.insurance_contract_number
    GROUP BY 1
),

enriched_premiums AS (
    SELECT
        p.partner_id,
        p.partner_surname || ' ' || p.partner_name as partner_full_name,
        p.partner_category,
        pc.report_date,
        pc.anketa_id,
        CASE WHEN pt.mandatory = 1 THEN 'Mandatory' ELSE 'Voluntary' END as insurance_type,
        COALESCE(v_cat.name3, 'Other') as product_category,
        COALESCE(pt.polis_name, 'Other') as product_name,
        pc.premium_amount,
        pc.commission_amount
    FROM premiums_commissions pc
    JOIN partner_whitelist p ON p.partner_id = pc.partner_id
    LEFT JOIN {{ source('raw', 'ins_polis_oracle') }} pol ON pol.tb_anketa = pc.anketa_id
    LEFT JOIN {{ source('raw', 'ins_pturi_oracle') }} pt ON pt.ins_id = pol.pturi_id
    LEFT JOIN {{ source('raw', 'ins_vertical_oracle') }} v_cat ON v_cat.ins_id = pt.vertical
)

SELECT
    ep.*,
    COALESCE(c.claims_amount, 0) as claims_amount,
    COALESCE(r.reinsurance_amount, 0) as reinsurance_amount
FROM enriched_premiums ep
LEFT JOIN claims_agg c ON c.anketa_id = ep.anketa_id
LEFT JOIN reinsurance_agg r ON r.anketa_id = ep.anketa_id
