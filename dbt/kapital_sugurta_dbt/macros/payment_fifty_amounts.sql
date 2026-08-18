{% macro payment_fifty_amounts_ctes(source_relation='payment_context') %}
fifty_zp_resolved AS (
    SELECT
        pc.*,
        CASE
            WHEN pc.is_beneficiary = 1 AND pc.bank_head_id IS NOT NULL
            THEN {{ get_dopstimul_sql('pc.bank_head_id', 'pc.ins_type', 'pc.opl_data', 'pc.ins_div', '1') }}
        END                                                                 AS bank_zp_fifty_id
    FROM {{ source_relation }} pc
),

fifty_zp_rule AS (
    SELECT
        fz.*,
        COALESCE(fz.bank_zp_fifty_id, fz.pturi_fifty_id)                    AS resolved_zp_fifty_id,
        CASE
            WHEN f_base.kod = 30 THEN
                CASE
                    WHEN fz.owner_rezident = 2 THEN 33
                    WHEN FLOOR(fz.ins_div / 1000) IN (39, 90, 10, 34)
                      OR fz.ins_div = 11000 THEN
                        CASE fz.driver_limit
                            WHEN 0 THEN 31.2
                            WHEN 1 THEN 31.1
                            ELSE 0
                        END
                    WHEN COALESCE(fz.use_territory, 0) NOT IN (1) THEN 32
                    ELSE
                        CASE fz.driver_limit
                            WHEN 0 THEN 31.2
                            WHEN 1 THEN 31.1
                            ELSE 0
                        END
                END
        END                                                                 AS osago_kod
    FROM fifty_zp_resolved fz
    LEFT JOIN {{ source('raw', 'ins_fifty_oracle') }} f_base
        ON f_base.id = COALESCE(fz.bank_zp_fifty_id, fz.pturi_fifty_id)
),

fifty_zp_amounts AS (
    SELECT
        fz.*,
        COALESCE(f_osago.id, f_rule.id)                                    AS zp_fifty_id,
        COALESCE(f_osago.percent_or_sum, f_rule.percent_or_sum)             AS zp_percent_or_sum,
        COALESCE(f_osago.percent, f_rule.percent)                           AS zp_percent,
        COALESCE(f_osago.sum, f_rule.sum)                                   AS zp_sum,
        COALESCE(f_osago.for_director, f_rule.for_director)                 AS zp_for_director
    FROM fifty_zp_rule fz
    LEFT JOIN {{ source('raw', 'ins_fifty_oracle') }} f_rule
        ON f_rule.id = fz.resolved_zp_fifty_id
    LEFT JOIN {{ source('raw', 'ins_fifty_oracle') }} f_osago
        ON f_osago.kod = fz.osago_kod
       AND fz.osago_kod IS NOT NULL
       AND fz.osago_kod <> 0
),

fifty_dop_amounts AS (
    SELECT
        za.*,
        CASE
            WHEN pt_chk.ins_id IS NULL
              OR f_pt.id IS NULL
              OR bk_bank.tb_id IS NULL
              OR bk_bank.tb_isbank <> 1
            THEN 0::NUMERIC
            ELSE
                CASE
                    WHEN dop_bank.fifty_id = 0 THEN 0::NUMERIC
                    WHEN dop_bank.fifty_id IS NOT NULL THEN
                        CASE
                            WHEN fd.percent_or_sum = 0
                            THEN ROUND(fd.percent * za.fifty_base_summa / 100, 2)
                            ELSE fd.sum::NUMERIC
                        END
                    WHEN COALESCE(za.pturi_dop_stimul, 0) <> 0 THEN
                        CASE
                            WHEN fd_pt.percent_or_sum = 0
                            THEN ROUND(fd_pt.percent * za.fifty_base_summa / 100, 2)
                            ELSE fd_pt.sum::NUMERIC
                        END
                    ELSE 0::NUMERIC
                END
        END                                                                 AS fifty_dop
    FROM fifty_zp_amounts za
    LEFT JOIN {{ source('raw', 'ins_pturi_oracle') }} pt_chk
        ON pt_chk.ins_id = za.ins_type
    LEFT JOIN {{ source('raw', 'ins_fifty_oracle') }} f_pt
        ON f_pt.id = pt_chk.fifty_id
    LEFT JOIN {{ source('raw', 'ins_kontragent_oracle') }} bk_bank
        ON za.beneficiary = bk_bank.tb_id
       AND bk_bank.tb_isbank = 1
    LEFT JOIN LATERAL (
        SELECT {{ get_dopstimul_sql('bk_bank.head_id', 'za.ins_type', 'za.opl_data', 'za.ins_div', '0') }} AS fifty_id
    ) dop_bank ON bk_bank.head_id IS NOT NULL
    LEFT JOIN {{ source('raw', 'ins_fifty_oracle') }} fd
        ON fd.id = dop_bank.fifty_id
       AND dop_bank.fifty_id IS NOT NULL
       AND dop_bank.fifty_id <> 0
    LEFT JOIN {{ source('raw', 'ins_fifty_oracle') }} fd_pt
        ON fd_pt.id = za.pturi_dop_stimul
       AND dop_bank.fifty_id IS NULL
       AND COALESCE(za.pturi_dop_stimul, 0) <> 0
)
{% endmacro %}

{% macro payment_fifty_select_columns() %}
    CASE
        WHEN zp_fifty_id IS NULL THEN 0::NUMERIC
        WHEN zp_percent_or_sum = 0
        THEN ROUND(zp_percent * fifty_base_summa / 100, 2)
        ELSE zp_sum::NUMERIC
    END                                                                 AS fifty_zp,
    fifty_dop,
    CASE
        WHEN zp_fifty_id IS NULL THEN 0::NUMERIC
        ELSE ROUND(COALESCE(zp_for_director, 0) * fifty_base_summa / 100, 2)
    END                                                                 AS fifty_director
{% endmacro %}
