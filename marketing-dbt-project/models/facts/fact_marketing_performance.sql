{{ config(
    materialized='incremental',
    unique_key='FACT_SK',
    on_schema_change='sync_all_columns',
    alias='FACT_MARKETING_PERFORMANCE',
    transient=false
) }}

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

FROM {{ ref('email_engagement') }} ee
LEFT JOIN {{ ref('email_send') }} es ON ee.SEND_ID = es.SEND_ID
LEFT JOIN {{ ref('campaign') }} c ON es.CAMPAIGN_ID = c.CAMPAIGN_ID
LEFT JOIN {{ ref('contact') }} ct ON ee.CONTACT_ID = ct.CONTACT_ID
LEFT JOIN {{ ref('lead') }} l ON ct.CONTACT_ID = l.CONVERTED_CONTACT_ID
LEFT JOIN {{ ref('opportunity') }} o ON c.CAMPAIGN_ID = o.CAMPAIGN_ID
    AND (
        o.ACCOUNT_ID = ct.CONTACT_ID 
        OR (l.LEAD_ID IS NOT NULL AND o.ACCOUNT_ID = l.LEAD_ID)
    )
LEFT JOIN {{ ref('event') }} e ON (
    ee.CONTACT_ID = e.RELATED_CONTACT_ID 
    OR l.LEAD_ID = e.RELATED_LEAD_ID
) AND c.CAMPAIGN_ID = e.RELATED_CAMPAIGN_ID
LEFT JOIN {{ ref('campaign_member') }} cm ON (
    ee.CONTACT_ID = cm.CONTACT_ID 
    OR l.LEAD_ID = cm.LEAD_ID
) AND c.CAMPAIGN_ID = cm.CAMPAIGN_ID

{% if is_incremental() %}
WHERE CURRENT_TIMESTAMP() > (
    SELECT MAX(LAST_MODIFIED_TIMESTAMP) FROM {{ this }}
)
{% endif %}
