{{ config(materialized='incremental', unique_key='lead_id') }}

WITH source_data AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['lead_id']) }} AS lead_sk,
    lead_id,
    first_name,
    last_name,
    company,
    email,
    phone,
    status,
    converted_contact_id,
    created_date
  FROM {{ source('marketing_source', 'lead') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01') FROM {{ this }}
    )
  {% endif %}
)

SELECT *, CURRENT_TIMESTAMP() AS last_modified_timestamp
FROM source_data
