{{
  config(
    materialized = 'table',
    post_hook    = [
      "CREATE INDEX IF NOT EXISTS idx_stg_valid_polis_anketa ON {{ this }} (tb_anketa)"
    ]
  )
}}

/*
  stg_valid_policies
  -------------------
  Pre-materialises the set of anketa_ids whose associated policy is
  in a VALID status (2 = active, 9 = extended, 10 = renewed).

  Replaces the expensive EXISTS subquery that every heavy curated model
  currently repeats inline:

      AND EXISTS (
          SELECT 1
          FROM raw.ins_polis_oracle p
          WHERE p.tb_status IN (2, 9, 10)
            AND p.tb_anketa = o.anketa_id
      )

  Usage in downstream models:
      JOIN ref('stg_valid_policies') vp
          ON vp.tb_anketa = o.anketa_id

  By materialising this once as a table (with an index), every model
  that joins it will do a fast index-lookup instead of a repeated
  full scan of ins_polis_oracle.
*/

SELECT DISTINCT
    tb_anketa
FROM {{ source('raw', 'ins_polis_oracle') }}
WHERE tb_status IN (2, 9, 10)
  AND tb_anketa IS NOT NULL
