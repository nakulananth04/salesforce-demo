{{
  config(
    materialized = 'incremental',
    unique_key = 'contact_id'
  )
}}

WITH source_data AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['contact_id']) }} AS contact_sk,
    CONTACT_ID,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    PHONE,
    created_date
  FROM {{ source('marketing_source', 'contact') }}

  {% if is_incremental() %}
    WHERE LAST_MODIFIED_TIMESTAMP > (
      SELECT COALESCE(MAX(LAST_MODIFIED_TIMESTAMP), '1900-01-01'::timestamp) FROM {{ this }}
    )
  {% endif %}
)

SELECT
  *,
  CURRENT_TIMESTAMP() AS LAST_MODIFIED_TIMESTAMP
FROM source_data
