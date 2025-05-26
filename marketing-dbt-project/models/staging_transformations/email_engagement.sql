{{
  config(
    materialized='incremental',
    unique_key='engagement_id'
  )
}}

WITH source_data AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['engagement_id']) }} AS engagement_sk,
    e.engagement_id,
    e.send_id,
    e.contact_id,
    c.contact_sk,
    e.engagement_type,
    e.engagement_timestamp,
    e.link_url
  FROM {{ source('marketing_source', 'email_engagement') }} AS e
  LEFT JOIN {{ ref('contact') }} c ON e.contact_id = c.contact_id
  
  {% if is_incremental() %}
    WHERE e.last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp) FROM {{ this }}
    )
  {% endif %}
)

SELECT
  *,
  CURRENT_TIMESTAMP() AS last_modified_timestamp
FROM source_data
