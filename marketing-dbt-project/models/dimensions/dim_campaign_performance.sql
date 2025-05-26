{{
  config(
    schema='marketing',
    materialized='incremental',
    unique_key='campaign_sk'
  )
}}

WITH filtered_campaign AS (
  SELECT * FROM {{ ref('campaign') }}
  
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp) FROM {{ this }}
    )
  {% endif %}
),

filtered_email_send AS (
  SELECT * FROM {{ ref('email_send') }}
  
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp) FROM {{ this }}
    )
  {% endif %}
),

filtered_email_engagement AS (
  SELECT * FROM {{ ref('email_engagement') }}
  
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp) FROM {{ this }}
    )
  {% endif %}
),

source_data AS (
  SELECT 
    c.campaign_sk,
    c.campaign_id,
    c.campaign_name,
    c.status,
    c.start_date,
    c.end_date,
    COUNT(DISTINCT e.contact_sk) AS unique_recipients,
    COUNT(DISTINCT CASE WHEN e.engagement_type = 'Open' THEN e.engagement_sk END) AS total_opens,
    COUNT(DISTINCT CASE WHEN e.engagement_type = 'Click' THEN e.engagement_sk END) AS total_clicks
  FROM {{ ref('campaign') }} c
  LEFT JOIN {{ ref('email_send') }} s ON c.campaign_id = s.campaign_id
  LEFT JOIN {{ ref('email_engagement') }} e ON s.send_id = e.send_id
  GROUP BY c.campaign_sk, c.campaign_id, c.campaign_name, c.status, c.start_date, c.end_date
)

SELECT
  *,
  CURRENT_TIMESTAMP() AS last_modified_timestamp
FROM source_data
