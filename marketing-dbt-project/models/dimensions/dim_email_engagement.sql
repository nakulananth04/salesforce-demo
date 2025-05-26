{{ 
  config(
    materialized='incremental',
    unique_key='engagement_sk'
  ) 
}}

WITH engagement_data AS (

  SELECT 
    {{ dbt_utils.generate_surrogate_key(['ENGAGEMENT_ID']) }} as ENGAGEMENT_SK,
    ee.ENGAGEMENT_ID,
    ee.ENGAGEMENT_TYPE,
    ee.CONTACT_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    es.SEND_ID,
    es.SUBJECT_LINE,
    et.TEMPLATE_NAME,
    cam.CAMPAIGN_NAME,
    TO_TIMESTAMP(ee.ENGAGEMENT_TIMESTAMP) AS ENGAGEMENT_TIMESTAMP

  FROM {{ ref('email_engagement') }} ee

  JOIN {{ ref('email_send') }} es 
    ON ee.SEND_ID = es.SEND_ID

  JOIN {{ ref('email_template') }} et 
    ON es.EMAIL_TEMPLATE_ID = et.TEMPLATE_ID

  LEFT JOIN {{ ref('campaign') }} cam 
    ON es.CAMPAIGN_ID = cam.CAMPAIGN_ID

  LEFT JOIN {{ ref('contact') }} c 
    ON ee.CONTACT_ID = c.CONTACT_ID

  {% if is_incremental() %}
    WHERE TO_TIMESTAMP(ee.ENGAGEMENT_TIMESTAMP) > (
      SELECT COALESCE(MAX(ENGAGEMENT_TIMESTAMP), '1900-01-01'::timestamp_ntz)
      FROM {{ this }}
    )
  {% endif %}

)

SELECT 
  *,
  CURRENT_TIMESTAMP() AS last_modified_timestamp
FROM engagement_data
