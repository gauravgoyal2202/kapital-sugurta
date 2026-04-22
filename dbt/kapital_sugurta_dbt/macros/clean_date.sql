{% macro clean_date(column_name) %}
    public.try_cast_date({{ column_name }}::text)
{% endmacro %}
