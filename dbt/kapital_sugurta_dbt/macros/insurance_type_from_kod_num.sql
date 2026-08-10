{% macro insurance_type_en(kod_num_expr) %}
CASE
    WHEN {{ kod_num_expr }} ~ '^[0-9]+$'
     AND {{ kod_num_expr }}::INT IN (3, 108, 123, 210, 211, 218, 401, 402, 418)
    THEN 'Compulsory'
    ELSE 'Voluntary'
END
{% endmacro %}

{% macro insurance_type_ru_from_kod_num(kod_num_expr) %}
CASE
    WHEN {{ kod_num_expr }} ~ '^[0-9]+$'
     AND {{ kod_num_expr }}::INT IN (3, 108, 123, 210, 211, 218, 401, 402, 418)
    THEN 'Обязательное'
    ELSE 'Добровольное'
END
{% endmacro %}

{% macro insurance_type_uz_cyrl_from_kod_num(kod_num_expr) %}
CASE
    WHEN {{ kod_num_expr }} ~ '^[0-9]+$'
     AND {{ kod_num_expr }}::INT IN (3, 108, 123, 210, 211, 218, 401, 402, 418)
    THEN 'Мажбурий'
    ELSE 'Ихтиёрий'
END
{% endmacro %}

{% macro insurance_type_uz_latn_from_kod_num(kod_num_expr) %}
CASE
    WHEN {{ kod_num_expr }} ~ '^[0-9]+$'
     AND {{ kod_num_expr }}::INT IN (3, 108, 123, 210, 211, 218, 401, 402, 418)
    THEN 'Majburiy'
    ELSE 'Ixtiyoriy'
END
{% endmacro %}
