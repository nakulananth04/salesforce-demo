{{
  config(
    materialized = 'incremental',
    unique_key = 'campaign_id'
  )
}}

WITH source_data AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['campaign_id']) }} AS campaign_sk,
    CAMPAIGN_ID,
    CAMPAIGN_NAME,
    START_DATE,
    END_DATE,
    STATUS,
    BUDGET
  FROM {{ source('marketing_source', 'campaign') }}

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
