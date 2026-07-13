{{ config(
    materialized = 'table',
    post_hook    = [
        "CREATE INDEX IF NOT EXISTS idx_stg_klass_polis ON {{ this }} (polis_id)"
    ]
) }}

/*
  stg_klass
  ---------
  Pre-materialises the raw.f_ins_pturiklass(pid) PL/pgSQL function as a tiny
  set-based lookup table (≈320 rows — one per distinct polis_id in ins_link_oracle).

  Replaces ~3.3 million per-row subquery invocations previously embedded inside:
    - curated_commercial_development_priority_areas
    - mart_commercial_market_share_yearly

  Logic mirrors the function body exactly:
    FOR each polis_id in ins_link_oracle:
        klass = STRING_AGG(DISTINCT klass_id ORDER BY klass_id)
                FROM ins_link_klass_oracle
                WHERE link_id IN (SELECT ins_id FROM ins_link_oracle WHERE polis_id = pid)
*/

SELECT
    lo.polis_id,
    STRING_AGG(DISTINCT lk.klass_id::TEXT, ', ' ORDER BY lk.klass_id::TEXT) AS klass
FROM {{ source('raw', 'ins_link_oracle') }} lo
JOIN {{ source('raw', 'ins_link_klass_oracle') }} lk
    ON lk.link_id = lo.ins_id
GROUP BY lo.polis_id
