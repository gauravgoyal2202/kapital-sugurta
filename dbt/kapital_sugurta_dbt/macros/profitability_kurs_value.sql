{% macro profitability_kurs_value(opl_val_expr, k_alias='k') %}
CASE {{ opl_val_expr }}
    WHEN  1 THEN 1
    WHEN  2 THEN {{ k_alias }}.kurs_usd
    WHEN  3 THEN {{ k_alias }}.kurs_eur
    WHEN  4 THEN {{ k_alias }}.kurs_rub
    WHEN  5 THEN {{ k_alias }}.kurs_aed
    WHEN  6 THEN {{ k_alias }}.kurs_aud
    WHEN  7 THEN {{ k_alias }}.kurs_cad
    WHEN  8 THEN {{ k_alias }}.kurs_chf
    WHEN  9 THEN {{ k_alias }}.kurs_cny
    WHEN 10 THEN {{ k_alias }}.kurs_dkk
    WHEN 11 THEN {{ k_alias }}.kurs_egp
    WHEN 12 THEN {{ k_alias }}.kurs_gbp
    WHEN 13 THEN {{ k_alias }}.kurs_isk
    WHEN 14 THEN {{ k_alias }}.kurs_jpy
    WHEN 15 THEN {{ k_alias }}.kurs_krw
    WHEN 16 THEN {{ k_alias }}.kurs_kwd
    WHEN 17 THEN {{ k_alias }}.kurs_lbp
    WHEN 18 THEN {{ k_alias }}.kurs_myr
    WHEN 19 THEN {{ k_alias }}.kurs_nok
    WHEN 20 THEN {{ k_alias }}.kurs_pln
    WHEN 21 THEN {{ k_alias }}.kurs_sek
    WHEN 22 THEN {{ k_alias }}.kurs_sgd
    WHEN 23 THEN {{ k_alias }}.kurs_try
    WHEN 24 THEN {{ k_alias }}.kurs_uah
    WHEN 26 THEN {{ k_alias }}.kurs_kzt
    ELSE NULL
END
{% endmacro %}
