{{ config(materialized='incremental', unique_key='event_id') }}

WITH source_data AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['event_id']) }} AS event_sk,
    event_id,
    subject,
    start_date_time,
    end_date_time,
    type,
    related_campaign_id,
    related_contact_id,
    related_lead_id,
    created_date
  FROM {{ source('marketing_source', 'event') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01') FROM {{ this }}
    )
  {% endif %}
)

SELECT *, CURRENT_TIMESTAMP() AS last_modified_timestamp
FROM source_data
