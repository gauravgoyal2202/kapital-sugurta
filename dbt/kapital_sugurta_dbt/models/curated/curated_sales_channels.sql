{{ config(materialized='view') }}

/*
  Sales Channel Mapping & Performance (Curated)
  --------------------------------------------
  Optimized Version: Direct joins for product filters.
*/

WITH payment_base AS (
    SELECT 
        o.bc_id, 
        o.user_id, 
        o.akt as agent_akt_id,
        o.anketa_id,
        (CASE WHEN o.opl_val = 1 THEN COALESCE(o.oplata, 0) ELSE COALESCE(o.oplata, 0) * COALESCE(o.val_kurs, 1) END)::NUMERIC AS premium_amount,
        (CASE WHEN o.opl_val = 1 THEN COALESCE(o.kommis_summa, 0) ELSE COALESCE(o.kommis_summa, 0) * COALESCE(o.val_kurs, 1) END)::NUMERIC AS commission_amount
    FROM {{ source('raw', 'ins_oplata_oracle') }} o
),

enriched AS (
    SELECT
        pb.*,
        bc.pym_date::DATE as payment_date,
        u.tb_name, u.tb_surname, u.is_partner_api, akt.akt_type,
        
        -- Filter Dimensions
        CASE WHEN pt.mandatory = 1 THEN 'Mandatory' ELSE 'Voluntary' END as insurance_type,
        COALESCE(v.name3, 'Прочее') as product_category,
        COALESCE(pt.polis_name, 'Other') as product_name,
        CASE WHEN pb.user_id IN (19202, 19588, 20322, 40791) THEN 'Yes' ELSE 'No' END AS is_anor_bank,
        TRIM(COALESCE(u.tb_surname, '')) || ' ' || TRIM(COALESCE(u.tb_name, '')) as agent_name,
        CASE 
            WHEN akt.akt_type = 0 THEN 'Individual Agent'
            WHEN akt.akt_type = 1 THEN 'Bank'
            WHEN akt.akt_type = 2 THEN 'Legal Entity Agent'
            ELSE 'Branch/Own'
        END as agent_type,

        -- SALES CHANNEL CLASSIFICATION
        CASE
            WHEN u.tb_surname = 'KSC.UZ' AND u.tb_name = 'WEB' THEN 'Website'
            WHEN u.is_partner_api = 1 OR pb.user_id IN (19482, 19588, 19738, 19887, 20174, 20282, 20463, 20522, 20574, 20704, 20728, 20729, 20731, 20732, 20822, 20827, 20828, 20829, 20845, 20877, 21367, 21405, 21409, 21410, 21739, 21740, 21741, 21751, 21774, 35788, 20322, 40791, 40795, 44788, 64788, 67790, 8280) THEN 'API Partner'
            WHEN akt.akt_type = 2 OR u.is_marketplace = 1 THEN 'Marketplace'
            WHEN akt.akt_type = 1 THEN 'Banks'
            WHEN akt.akt_type = 0 THEN 'Agent Network'
            ELSE 'Branch Network'
        END AS sales_channel

    FROM payment_base pb
    LEFT JOIN {{ source('raw', 'ins_bank_client_oracle') }} bc ON bc.ins_id = pb.bc_id
    LEFT JOIN {{ source('raw', 'tb_users_oracle') }} u ON u.tb_id = pb.user_id
    LEFT JOIN {{ source('raw', 'ins_agent_akt_oracle') }} akt ON akt.ins_id = pb.agent_akt_id
    -- Optimized Join Logic
    LEFT JOIN {{ source('raw', 'ins_polis_oracle') }} p ON p.tb_anketa = pb.anketa_id
    LEFT JOIN {{ source('raw', 'ins_pturi_oracle') }} pt ON pt.ins_id = p.pturi_id
    LEFT JOIN {{ source('raw', 'ins_vertical_oracle') }} v ON v.ins_id = pt.vertical
    WHERE bc.pym_date IS NOT NULL
)

SELECT
    payment_date,
    anketa_id, -- Added for joining
    sales_channel,
    CASE
        WHEN sales_channel IN ('Banks', 'API Partner', 'Marketplace') THEN 'Partner Channels'
        WHEN sales_channel = 'Agent Network' THEN 'Agency Network'
        ELSE 'Own Channels'
    END AS channel_group,
    insurance_type,
    product_category,
    product_name,
    is_anor_bank,
    agent_name,
    agent_type,
    premium_amount,
    commission_amount
FROM enriched
