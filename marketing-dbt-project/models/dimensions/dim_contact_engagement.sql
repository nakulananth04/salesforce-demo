{{ 
  config(
    materialized='incremental',
    unique_key='contact_sk'
  ) 
}}

WITH engagement_summary AS (
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
        {{ ref('contact') }} ct
    LEFT JOIN 
        {{ ref('email_engagement') }} e ON ct.contact_id = e.contact_id
    GROUP BY 
        ct.contact_sk,ct.contact_id, ct.first_name, ct.last_name, ct.email
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS last_modified_timestamp
FROM engagement_summary

{% if is_incremental() %}
WHERE last_modified_timestamp > (
    SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01 00:00:00'::timestamp_ntz)
    FROM {{ this }}
)
{% endif %}
