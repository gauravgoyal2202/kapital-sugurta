{{
  config(
    materialized = 'table',
    post_hook    = [
      "CREATE INDEX IF NOT EXISTS idx_stg_exchange_rates_date_curr ON {{ this }} (kurs_date, currency_id)"
    ]
  )
}}

/*
  stg_exchange_rates
  -------------------
  Unpivots the wide ins_kurs_oracle table into a normalised
  (date, currency_id, rate) lookup.

  Replaces the row-by-row UDF call:
      raw.F_INS_GETKURS(o.opl_val, bc.pym_date)

  Usage in downstream models:
      LEFT JOIN ref('stg_exchange_rates') kurs
          ON  kurs.kurs_date   = bc.pym_date::date
          AND kurs.currency_id = o.opl_val

      -- Then use: COALESCE(kurs.rate, 1)  (1 = UZS, no conversion needed)

  Currency IDs mirror the original UDF CASE statement exactly:
      1  = UZS  (rate always 1)
      2  = USD
      3  = EUR
      4  = RUB
      5  = AED
      6  = AUD
      7  = CAD
      8  = CHF
      9  = CNY
      10 = DKK
      11 = EGP
      12 = GBP
      13 = ISK
      14 = JPY
      15 = KRW
      16 = KWD
      17 = LBP
      18 = MYR
      19 = NOK
      20 = PLN
      21 = SEK
      22 = SGD
      23 = TRY
      24 = UAH
      25 = INR  (not in source table — will be NULL, same as UDF)
      26 = KZT
*/

WITH raw_kurs AS (
    SELECT
        kurs_date::date AS kurs_date,
        kurs_usd,
        kurs_eur,
        kurs_rub,
        kurs_aed,
        kurs_aud,
        kurs_cad,
        kurs_chf,
        kurs_cny,
        kurs_dkk,
        kurs_egp,
        kurs_gbp,
        kurs_isk,
        kurs_jpy,
        kurs_krw,
        kurs_kwd,
        kurs_lbp,
        kurs_myr,
        kurs_nok,
        kurs_pln,
        kurs_sek,
        kurs_sgd,
        kurs_try,
        kurs_uah,
        kurs_kzt
    FROM {{ source('raw', 'ins_kurs_oracle') }}
),

unpivoted AS (

    -- 1  UZS — always 1:1, no conversion
    SELECT kurs_date,  1::numeric AS currency_id,  1::numeric          AS rate FROM raw_kurs
    UNION ALL
    -- 2  USD
    SELECT kurs_date,  2,  kurs_usd  FROM raw_kurs
    UNION ALL
    -- 3  EUR
    SELECT kurs_date,  3,  kurs_eur  FROM raw_kurs
    UNION ALL
    -- 4  RUB
    SELECT kurs_date,  4,  kurs_rub  FROM raw_kurs
    UNION ALL
    -- 5  AED
    SELECT kurs_date,  5,  kurs_aed  FROM raw_kurs
    UNION ALL
    -- 6  AUD
    SELECT kurs_date,  6,  kurs_aud  FROM raw_kurs
    UNION ALL
    -- 7  CAD
    SELECT kurs_date,  7,  kurs_cad  FROM raw_kurs
    UNION ALL
    -- 8  CHF
    SELECT kurs_date,  8,  kurs_chf  FROM raw_kurs
    UNION ALL
    -- 9  CNY
    SELECT kurs_date,  9,  kurs_cny  FROM raw_kurs
    UNION ALL
    -- 10 DKK
    SELECT kurs_date, 10,  kurs_dkk  FROM raw_kurs
    UNION ALL
    -- 11 EGP
    SELECT kurs_date, 11,  kurs_egp  FROM raw_kurs
    UNION ALL
    -- 12 GBP
    SELECT kurs_date, 12,  kurs_gbp  FROM raw_kurs
    UNION ALL
    -- 13 ISK
    SELECT kurs_date, 13,  kurs_isk  FROM raw_kurs
    UNION ALL
    -- 14 JPY
    SELECT kurs_date, 14,  kurs_jpy  FROM raw_kurs
    UNION ALL
    -- 15 KRW
    SELECT kurs_date, 15,  kurs_krw  FROM raw_kurs
    UNION ALL
    -- 16 KWD
    SELECT kurs_date, 16,  kurs_kwd  FROM raw_kurs
    UNION ALL
    -- 17 LBP
    SELECT kurs_date, 17,  kurs_lbp  FROM raw_kurs
    UNION ALL
    -- 18 MYR
    SELECT kurs_date, 18,  kurs_myr  FROM raw_kurs
    UNION ALL
    -- 19 NOK
    SELECT kurs_date, 19,  kurs_nok  FROM raw_kurs
    UNION ALL
    -- 20 PLN
    SELECT kurs_date, 20,  kurs_pln  FROM raw_kurs
    UNION ALL
    -- 21 SEK
    SELECT kurs_date, 21,  kurs_sek  FROM raw_kurs
    UNION ALL
    -- 22 SGD
    SELECT kurs_date, 22,  kurs_sgd  FROM raw_kurs
    UNION ALL
    -- 23 TRY
    SELECT kurs_date, 23,  kurs_try  FROM raw_kurs
    UNION ALL
    -- 24 UAH
    SELECT kurs_date, 24,  kurs_uah  FROM raw_kurs
    UNION ALL
    -- 25 INR — not in source table; NULL (identical to original UDF behaviour)
    SELECT kurs_date, 25,  NULL::numeric FROM raw_kurs
    UNION ALL
    -- 26 KZT
    SELECT kurs_date, 26,  kurs_kzt  FROM raw_kurs

)

SELECT
    kurs_date,
    currency_id,
    rate
FROM unpivoted
WHERE kurs_date IS NOT NULL
