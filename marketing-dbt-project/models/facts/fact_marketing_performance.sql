{{ config(
    materialized='incremental',
    unique_key='FACT_SK',
    on_schema_change='sync_all_columns',
    alias='FACT_MARKETING_PERFORMANCE',
    transient=false
) }}

WITH filtered_email_engagement AS (
  SELECT * FROM {{ ref('email_engagement') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz)
      FROM {{ this }}
    )
  {% endif %}
),

filtered_email_send AS (
  SELECT * FROM {{ ref('email_send') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz)
      FROM {{ this }}
    )
  {% endif %}
),

filtered_campaign AS (
  SELECT * FROM {{ ref('campaign') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz)
      FROM {{ this }}
    )
  {% endif %}
),

filtered_contact AS (
  SELECT * FROM {{ ref('contact') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz)
      FROM {{ this }}
    )
  {% endif %}
),

filtered_lead AS (
  SELECT * FROM {{ ref('lead') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz)
      FROM {{ this }}
    )
  {% endif %}
),

filtered_opportunity AS (
  SELECT * FROM {{ ref('opportunity') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz)
      FROM {{ this }}
    )
  {% endif %}
),

filtered_event AS (
  SELECT * FROM {{ ref('event') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz)
      FROM {{ this }}
    )
  {% endif %}
),

filtered_campaign_member AS (
  SELECT * FROM {{ ref('campaign_member') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz)
      FROM {{ this }}
    )
  {% endif %}
)

SELECT
    MD5(CONCAT(
        COALESCE(ee.ENGAGEMENT_SK, ''),
        COALESCE(e.EVENT_SK, ''),
        COALESCE(o.OPPORTUNITY_SK, '')
    )) AS FACT_SK,

    -- Engagement Info
    ee.ENGAGEMENT_SK,
    ee.ENGAGEMENT_ID,
    ee.ENGAGEMENT_TYPE,
    ee.ENGAGEMENT_TIMESTAMP,
    ee.LINK_URL,
    ee.ENGAGEMENT_TYPE = 'Open' AS IS_OPEN,
    ee.ENGAGEMENT_TYPE = 'Click' AS IS_CLICK,

    -- Send Info
    es.SEND_SK,
    es.SEND_ID,
    es.EMAIL_TEMPLATE_ID,
    es.SEND_DATE,
    es.SUBJECT_LINE,

    -- Campaign Info
    c.CAMPAIGN_SK,
    c.CAMPAIGN_ID,
    c.CAMPAIGN_NAME,

    -- Contact Info
    ct.CONTACT_SK,
    ct.CONTACT_ID,
    ct.FIRST_NAME AS CONTACT_FIRST_NAME,
    ct.LAST_NAME AS CONTACT_LAST_NAME,

    -- Lead Info
    l.LEAD_SK,
    l.LEAD_ID,
    l.FIRST_NAME AS LEAD_FIRST_NAME,
    l.LAST_NAME AS LEAD_LAST_NAME,

    -- Opportunity Info
    o.OPPORTUNITY_SK,
    o.OPPORTUNITY_ID,
    o.OPPORTUNITY_NAME,

    -- Event Info
    e.EVENT_SK,
    e.EVENT_ID,
    e.TYPE AS EVENT_TYPE,

    -- Campaign Member Info
    cm.MEMBER_SK,
    cm.MEMBER_ID,

    -- Time Dimensions
    DATE(COALESCE(ee.ENGAGEMENT_TIMESTAMP, e.START_DATE_TIME)) AS ENGAGEMENT_DATE,
    HOUR(COALESCE(ee.ENGAGEMENT_TIMESTAMP, e.START_DATE_TIME)) AS ENGAGEMENT_HOUR,
    DATEDIFF('day', c.START_DATE, DATE(COALESCE(ee.ENGAGEMENT_TIMESTAMP, e.START_DATE_TIME))) AS DAYS_SINCE_CAMPAIGN_START,

    CURRENT_TIMESTAMP() AS LAST_MODIFIED_TIMESTAMP

FROM filtered_email_engagement ee
LEFT JOIN filtered_email_send es ON ee.SEND_ID = es.SEND_ID
LEFT JOIN filtered_campaign c ON es.CAMPAIGN_ID = c.CAMPAIGN_ID
LEFT JOIN filtered_contact ct ON ee.CONTACT_ID = ct.CONTACT_ID
LEFT JOIN filtered_lead l ON ct.CONTACT_ID = l.CONVERTED_CONTACT_ID
LEFT JOIN filtered_opportunity o ON c.CAMPAIGN_ID = o.CAMPAIGN_ID
    AND (
        o.ACCOUNT_ID = ct.CONTACT_ID 
        OR (l.LEAD_ID IS NOT NULL AND o.ACCOUNT_ID = l.LEAD_ID)
    )
LEFT JOIN filtered_event e ON (
    ee.CONTACT_ID = e.RELATED_CONTACT_ID 
    OR l.LEAD_ID = e.RELATED_LEAD_ID
) AND c.CAMPAIGN_ID = e.RELATED_CAMPAIGN_ID
LEFT JOIN filtered_campaign_member cm ON (
    ee.CONTACT_ID = cm.CONTACT_ID 
    OR l.LEAD_ID = cm.LEAD_ID
) AND c.CAMPAIGN_ID = cm.CAMPAIGN_ID
