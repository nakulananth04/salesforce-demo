{{
  config(
    materialized = 'incremental',
    unique_key = 'send_id'
  )
}}

WITH source_data AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['send_id']) }} AS send_sk,
    SEND_ID,
    CAMPAIGN_ID,
    EMAIL_TEMPLATE_ID,
    SEND_DATE,
    SUBJECT_LINE,
    TOTAL_SENT
  FROM {{ source('marketing_source', 'email_send') }}

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
