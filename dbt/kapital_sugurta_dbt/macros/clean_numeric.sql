{% macro clean_numeric(column_name) %}
    NULLIF(REPLACE(SUBSTRING(REPLACE({{ column_name }}, ' ', ''), '(-?[0-9]+(?:[.,][0-9]+)?)'), ',', '.'), '')::NUMERIC
{% endmacro %}
