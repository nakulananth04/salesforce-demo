{{ config(materialized='incremental', unique_key='template_id') }}

WITH source_data AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['template_id']) }} AS template_sk,
    template_id,
    template_name,
    subject,
    html_content,
    created_date
  FROM {{ source('marketing_source', 'email_template') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01') FROM {{ this }}
    )
  {% endif %}
)

SELECT *, CURRENT_TIMESTAMP() AS last_modified_timestamp
FROM source_data
