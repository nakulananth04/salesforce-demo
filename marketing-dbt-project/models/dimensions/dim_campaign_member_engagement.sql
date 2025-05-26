{{
  config(
    materialized = 'incremental',
    unique_key = 'member_sk'
  )
}}

WITH filtered_campaign_member AS (
  SELECT * FROM {{ ref('campaign_member') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01') FROM {{ this }})
  {% endif %}
),
filtered_email_engagement AS (
  SELECT * FROM {{ ref('email_engagement') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01') FROM {{ this }})
  {% endif %}
),
filtered_event AS (
  SELECT * FROM {{ ref('event') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01') FROM {{ this }})
  {% endif %}
),
filtered_campaign AS (
  SELECT * FROM {{ ref('campaign') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01') FROM {{ this }})
  {% endif %}
),
source_data AS (
  SELECT
    cm.member_sk,
    cm.member_id,
    cm.campaign_id,
    c.campaign_name,
    cm.contact_id,
    cm.lead_id,
    cm.status AS member_status,
    cm.first_responded_date,
    COUNT(DISTINCT ee.engagement_id) AS total_engagements,
    COUNT(DISTINCT CASE WHEN ee.engagement_type = 'Open' THEN ee.engagement_id END) AS email_opens,
    COUNT(DISTINCT CASE WHEN e.event_id IS NOT NULL THEN e.event_id END) AS event_attendances,
    MAX(COALESCE(ee.engagement_timestamp, e.start_date_time)) AS last_engagement_date
  FROM filtered_campaign_member cm
  LEFT JOIN filtered_campaign c ON cm.campaign_id = c.campaign_id
  LEFT JOIN filtered_email_engagement ee ON cm.contact_id = ee.contact_id OR cm.lead_id = ee.contact_id
  LEFT JOIN filtered_event e ON (cm.contact_id = e.related_contact_id OR cm.lead_id = e.related_lead_id)
                                 AND cm.campaign_id = e.related_campaign_id
  GROUP BY cm.member_sk, cm.member_id, cm.campaign_id, c.campaign_name, cm.contact_id, cm.lead_id, cm.status, cm.first_responded_date
)

SELECT *, CURRENT_TIMESTAMP() AS last_modified_timestamp FROM source_data