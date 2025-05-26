{{
  config(
    schema='marketing',
    materialized='incremental',
    unique_key='campaign_date_sk'
  )
}}

WITH base AS (

  SELECT
    dd.DATE_KEY,
    dc.CAMPAIGN_SK,
    dc.CAMPAIGN_ID,
    dc.CAMPAIGN_NAME,
    dc.STATUS,

    COUNT(DISTINCT dcm.CONTACT_ID) AS unique_contacts_engaged,
    COUNT(DISTINCT dcm.MEMBER_ID) AS total_members,
    SUM(dcm.TOTAL_ENGAGEMENTS) AS total_engagements,
    SUM(dcm.EMAIL_OPENS) AS total_opens,

    COUNT(DISTINCT CASE 
      WHEN dee.ENGAGEMENT_TYPE = 'Click' THEN dee.ENGAGEMENT_ID 
    END) AS link_clicks,

    COUNT(DISTINCT dop.OPPORTUNITY_ID) AS opportunities_generated,
    SUM(dop.AMOUNT) AS opportunity_amount

  FROM {{ ref('dim_campaign') }} dc
  JOIN {{ ref('dim_date') }} dd
    ON dd.DATE_KEY BETWEEN dc.START_DATE AND COALESCE(dc.END_DATE, CURRENT_DATE())

  LEFT JOIN {{ ref('dim_campaign_member_engagement') }} dcm 
    ON dc.CAMPAIGN_ID = dcm.CAMPAIGN_ID

  LEFT JOIN {{ ref('dim_email_engagement') }} dee 
    ON dcm.CONTACT_ID = dee.CONTACT_ID

  LEFT JOIN {{ ref('dim_opportunity_performance') }} dop 
    ON dc.CAMPAIGN_ID = dop.CAMPAIGN_ID

  GROUP BY
    dd.DATE_KEY, dc.CAMPAIGN_SK, dc.CAMPAIGN_ID, dc.CAMPAIGN_NAME, dc.STATUS
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['campaign_sk', 'date_key']) }} AS campaign_date_sk,
  *,
  CURRENT_TIMESTAMP() AS last_modified_timestamp
FROM base

{% if is_incremental() %}
-- Optional: only process campaigns updated recently
-- WHERE last_modified_timestamp > (
--   SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01') FROM {{ this }}
-- )
{% endif %}
