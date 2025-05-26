{{ config(materialized='incremental', unique_key='opportunity_id') }}

WITH source_data AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['opportunity_id']) }} AS opportunity_sk,
    opportunity_id,
    opportunity_name,
    account_id,
    stage_name,
    amount,
    close_date,
    campaign_id,
    created_date
  FROM {{ source('marketing_source', 'opportunity') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01') FROM {{ this }}
    )
  {% endif %}
)

SELECT *, CURRENT_TIMESTAMP() AS last_modified_timestamp
FROM source_data
