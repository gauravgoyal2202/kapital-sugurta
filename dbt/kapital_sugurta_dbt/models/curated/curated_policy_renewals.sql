{{ config(
    materialized='table',
    indexes=[
        {'columns': ['report_month']},
        {'columns': ['client_id']}
    ]
) }}

/*
  Curated Policy Renewals
  ------------------------------------
  Aligned 100% with Oracle Logic:
  - Materializes the self-join of raw.kontragent_report for high performance.
  - Groups expired and renewed clients at the rekvezid level.
*/

WITH base AS (
    SELECT * FROM {{ ref('curated_policy_renewals_base') }}
),

renewed_flags AS (
    SELECT DISTINCT ON (i.anketa_id)
        i.anketa_id AS old_anketa_id,
        i.date_end AS expiration_date,
        DATE_TRUNC('month', i.date_end)::DATE AS report_month,
        i.rekvezid AS client_id,
        i.ins_type AS product_type_id,
        i.fizyur,
        
        -- Mark as renewed if another policy exists for this client after expiration_date
        CASE
            WHEN n.rekvezid IS NOT NULL
            AND (
                COALESCE(NULLIF(n.policy_sery::VARCHAR, ''), 'x') <> COALESCE(NULLIF(i.policy_sery::VARCHAR, ''), 'x')
                OR COALESCE(NULLIF(n.policy_number::VARCHAR, ''), 'x') <> COALESCE(NULLIF(i.policy_number::VARCHAR, ''), 'x')
            )
            AND n.date_control > i.date_end
            THEN 1
            ELSE 0
        END AS is_renewed
        
    FROM base i
    LEFT JOIN base n
        ON n.rekvezid = i.rekvezid
        AND n.fizyur = i.fizyur
        AND n.date_control > i.date_end
    ORDER BY i.anketa_id, is_renewed DESC -- Ensure we get is_renewed = 1 if any duplicate exists
)

SELECT 
    r.*,
    -- Product dimensions
    CASE WHEN pt.mandatory = 1 THEN 'Mandatory' ELSE 'Voluntary' END AS insurance_type,
    COALESCE(v_cat.name3, 'Other') AS product_category,
    COALESCE(pt.polis_name, 'Other') AS product_name
FROM renewed_flags r
LEFT JOIN {{ source('raw', 'ins_pturi_oracle') }} pt ON pt.ins_id = r.product_type_id
LEFT JOIN {{ source('raw', 'ins_vertical_oracle') }} v_cat ON v_cat.ins_id = pt.vertical
