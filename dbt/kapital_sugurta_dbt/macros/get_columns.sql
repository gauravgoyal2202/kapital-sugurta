{% macro get_my_columns() %}
  {% set query %}
    SELECT report_year, COUNT(*) as rows FROM presentation.mart_claim_settlement_indicators GROUP BY report_year ORDER BY report_year
  {% endset %}
  
  {% set results = run_query(query) %}
  
  {% if execute %}
    {% for row in results.rows %}
      {% do log("YEAR " ~ row[0] ~ ": " ~ row[1] ~ " rows", info=true) %}
    {% endfor %}
  {% endif %}
{% endmacro %}
