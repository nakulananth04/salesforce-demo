{% macro safe_any_date_field_cast(source_relation) %}
  {% set columns = adapter.get_columns_in_relation(source_relation) %}
  {% set matched_column = none %}

  {% for col in columns %}
    {% if col.name.upper().endswith('_DATE') %}
      {% set matched_column = col.name %}
      {% break %}
    {% endif %}
  {% endfor %}

  {% if matched_column %}
    TO_TIMESTAMP_NTZ({{ matched_column }}::BIGINT / 1e9) AS {{ matched_column | lower }}_ts
  {% else %}
    NULL AS generic_date_ts
  {% endif %}
{% endmacro %}
