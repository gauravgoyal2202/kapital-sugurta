{{ config(materialized='view') }}

/*
  Channel & Partner Dimension (Curated)
  --------------------------------------
  Maps each insurance anketa_id to its Sales Channel, Partner Type, and Partner Name.
  Used by mart_insurance_volume_indicators for Dashboard 7 bottom section filters.

  Filter columns produced:
    sales_channel  Website / Agent Network / Banks / Marketplace / API Partner / In-House
    partner_type   API / Internal
    partner_name   Specific entity name (for Banks, Marketplaces, Agent Networks)

  Source logic (per client spec):
    ins_oplata_oracle.user_id  → tb_users_oracle  (is_partner_api, is_marketplace, name)
    ins_oplata_oracle.akt      → ins_agent_akt_oracle (akt_type: 0=Agent, 1=Bank, 2=Marketplace)
    Sales Channel derivation mirrors curated_sales_channels.sql classification.

  Deduplication:
    One row per anketa_id — when multiple payments exist for the same anketa,
    the first payment record determines the channel assignment.
*/

WITH payment_map AS (
    SELECT
        o.anketa_id,
        o.user_id,
        o.akt                   AS agent_akt_id,
        u.tb_name,
        u.tb_surname,
        COALESCE(u.is_partner_api,  0)  AS is_partner_api,
        COALESCE(u.is_marketplace,  0)  AS is_marketplace,
        akt.akt_type
    FROM {{ source('raw', 'ins_oplata_oracle') }} o
    LEFT JOIN {{ source('raw', 'tb_users_oracle') }}       u   ON u.tb_id    = o.user_id
    LEFT JOIN {{ source('raw', 'ins_agent_akt_oracle') }}  akt ON akt.ins_id = o.akt
    WHERE o.anketa_id IS NOT NULL
),

classified AS (
    SELECT
        anketa_id,

        -- ── SALES CHANNEL ────────────────────────────────────────────────
        CASE
            WHEN tb_surname = 'KSC.UZ' AND tb_name = 'WEB'
                THEN 'Website'
            WHEN is_partner_api = 1
              OR user_id IN (
                    19482,19588,19738,19887,20174,20282,20463,20522,20574,20704,
                    20728,20729,20731,20732,20822,20827,20828,20829,20845,20877,
                    21367,21405,21409,21410,21739,21740,21741,21751,21774,
                    35788,20322,40791,40795,44788,64788,67790,8280
                 )
                THEN 'API Partner'
            WHEN akt_type = 2 OR is_marketplace = 1
                THEN 'Marketplace'
            WHEN akt_type = 1
                THEN 'Banks'
            WHEN akt_type = 0
                THEN 'Agent Network'
            ELSE 'In-House'
        END AS sales_channel,

        -- ── PARTNER TYPE ─────────────────────────────────────────────────
        CASE
            WHEN is_partner_api = 1 THEN 'API'
            ELSE 'Internal'
        END AS partner_type,

        -- ── PARTNER NAME ─────────────────────────────────────────────────
        -- Populated for partner channels; NULL for website / in-house
        CASE
            WHEN akt_type IN (0, 1, 2)
              OR is_marketplace = 1
              OR is_partner_api = 1
            THEN NULLIF(TRIM(COALESCE(tb_surname, '') || ' ' || COALESCE(tb_name, '')), '')
            ELSE NULL
        END AS partner_name

    FROM payment_map
),

-- One row per anketa_id (first payment wins for channel assignment)
deduped AS (
    SELECT DISTINCT ON (anketa_id)
        anketa_id,
        sales_channel,
        partner_type,
        partner_name
    FROM classified
    ORDER BY anketa_id
)

SELECT * FROM deduped
