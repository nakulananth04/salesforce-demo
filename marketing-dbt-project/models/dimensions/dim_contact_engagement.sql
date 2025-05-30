{{ 
  config(
    materialized='incremental',
    unique_key='contact_sk'
  ) 
}}

WITH filtered_contact AS (
    SELECT * FROM {{ ref('contact') }}
    {% if is_incremental() %}
      WHERE last_modified_timestamp > (
          SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz)
          FROM {{ this }}
      )
    {% endif %}
),

filtered_email_engagement AS (
    SELECT * FROM {{ ref('email_engagement') }}
    {% if is_incremental() %}
      WHERE last_modified_timestamp > (
          SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz)
          FROM {{ this }}
      )
    {% endif %}
),

engagement_summary AS (
    SELECT 
        ct.contact_sk,
        ct.contact_id,
        ct.first_name,
        ct.last_name,
        ct.email,
        COUNT(DISTINCT e.engagement_id) AS total_engagements,
        COUNT(DISTINCT CASE WHEN e.engagement_type = 'Open' THEN e.engagement_id END) AS email_opens,
        COUNT(DISTINCT CASE WHEN e.engagement_type = 'Click' THEN e.engagement_id END) AS link_clicks,
        MAX(e.engagement_timestamp) AS last_engagement_date
    FROM 
        filtered_contact ct
    LEFT JOIN 
        filtered_email_engagement e ON ct.contact_id = e.contact_id
    GROUP BY 
        ct.contact_sk, ct.contact_id, ct.first_name, ct.last_name, ct.email
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS last_modified_timestamp
FROM engagement_summary
