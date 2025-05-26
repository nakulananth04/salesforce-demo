{{
  config(
    materialized='incremental',
    unique_key='campaign_sk'
  )
}}

WITH source_data AS (

  SELECT 
    CAMPAIGN_ID,
    CAMPAIGN_NAME,
    START_DATE,
    END_DATE,
    STATUS,
    BUDGET
  FROM {{ ref('campaign') }}

  {% if is_incremental() %}
    WHERE LAST_MODIFIED_TIMESTAMP > (
      SELECT COALESCE(MAX(LAST_MODIFIED_TIMESTAMP), '1900-01-01'::timestamp_ntz)
      FROM {{ this }}
    )
  {% endif %}

)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['CAMPAIGN_ID']) }} as CAMPAIGN_SK,
  *,
CURRENT_TIMESTAMP() AS last_modified_timestamp

FROM source_data
