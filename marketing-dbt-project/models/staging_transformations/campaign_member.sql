{{ config(materialized='incremental', unique_key='member_id') }}

WITH source_data AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['member_id']) }} AS member_sk,
    member_id,
    campaign_id,
    contact_id,
    lead_id,
    status,
    first_responded_date,
    created_date
  FROM {{ source('marketing_source', 'campaign_member') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01') FROM {{ this }}
    )
  {% endif %}
)

SELECT *, CURRENT_TIMESTAMP() AS last_modified_timestamp
FROM source_data
