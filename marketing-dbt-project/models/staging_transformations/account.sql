{{ config(materialized='incremental', unique_key='account_id') }}

WITH source_data AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['account_id']) }} AS account_sk,
    account_id,
    account_name,
    industry,
    website,
    annual_revenue,
    created_date
  FROM {{ source('marketing_source', 'account') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01') FROM {{ this }}
    )
  {% endif %}
)

SELECT *, CURRENT_TIMESTAMP() AS last_modified_timestamp
FROM source_data