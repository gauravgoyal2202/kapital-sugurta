{% macro get_dopstimul_sql(bank_expr, pturi_expr, start_date_expr, division_expr, type_expr) %}
COALESCE(
    (
        SELECT bp.fifty_id
        FROM {{ source('raw', 'ins_bank_pturi_oracle') }} bp
        WHERE bp.bank_id = {{ bank_expr }}
          AND (bp.pturi_id = {{ pturi_expr }} OR bp.pturi_id = 0)
          AND bp.type = {{ type_expr }}
          AND bp.fifty_id IS NOT NULL
          AND bp.division_id = {{ division_expr }}
          AND bp.start_date BETWEEN {{ start_date_expr }}::DATE
              AND (
                    DATE_TRUNC('month', {{ start_date_expr }}::DATE)
                    + INTERVAL '1 month'
                    - INTERVAL '1 day'
                  )::DATE
        ORDER BY bp.start_date DESC, bp.fifty_id DESC
        LIMIT 1
    ),
    (
        SELECT bp.fifty_id
        FROM {{ source('raw', 'ins_bank_pturi_oracle') }} bp
        WHERE bp.bank_id = {{ bank_expr }}
          AND (bp.pturi_id = {{ pturi_expr }} OR bp.pturi_id = 0)
          AND bp.type = {{ type_expr }}
          AND bp.fifty_id IS NOT NULL
          AND {{ start_date_expr }}::DATE >= bp.start_date
        ORDER BY bp.start_date DESC, bp.fifty_id DESC
        LIMIT 1
    )
)
{% endmacro %}
