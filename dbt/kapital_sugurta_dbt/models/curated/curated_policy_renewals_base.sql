{{ config(
    materialized='table',
    indexes=[
        {'columns': ['rekvezid']},
        {'columns': ['fizyur']},
        {'columns': ['date_end']},
        {'columns': ['date_control']}
    ]
) }}

/*
  Base table for policy renewals, materializing raw.kontragent_report
  for extremely fast self-joins and dashboard filtering.
*/

SELECT
    ins_type,
    kod_num,
    name,
    anketa_id,
    policy_sery,
    policy_number,
    date_control,
    date_begin,
    date_end,
    tb_id,
    fizyur,
    pnfl,
    inn,
    rekvezid,
    userid
FROM {{ source('raw', 'kontragent_report') }}
