{{
  config(
    schema='marketing',
    materialized='incremental',
    unique_key='campaign_date_sk',
    on_schema_change='ignore'
  )
}}

WITH filtered_campaign AS (
  SELECT * FROM {{ ref('dim_campaign') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz)
      FROM {{ this }}
    )
  {% endif %}
),

filtered_campaign_member_engagement AS (
  SELECT * FROM {{ ref('dim_campaign_member_engagement') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz)
      FROM {{ this }}
    )
  {% endif %}
),

filtered_email_engagement AS (
  SELECT * FROM {{ ref('dim_email_engagement') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz)
      FROM {{ this }}
    )
  {% endif %}
),

filtered_opportunity_performance AS (
  SELECT * FROM {{ ref('dim_opportunity_performance') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz)
      FROM {{ this }}
    )
  {% endif %}
),

base AS (

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

  FROM filtered_campaign dc
  JOIN {{ ref('dim_date') }} dd
    ON dd.DATE_KEY BETWEEN dc.START_DATE AND COALESCE(dc.END_DATE, CURRENT_DATE())

  LEFT JOIN filtered_campaign_member_engagement dcm 
    ON dc.CAMPAIGN_ID = dcm.CAMPAIGN_ID

  LEFT JOIN filtered_email_engagement dee 
    ON dcm.CONTACT_ID = dee.CONTACT_ID

  LEFT JOIN filtered_opportunity_performance dop 
    ON dc.CAMPAIGN_ID = dop.CAMPAIGN_ID

  GROUP BY
    dd.DATE_KEY, dc.CAMPAIGN_SK, dc.CAMPAIGN_ID, dc.CAMPAIGN_NAME, dc.STATUS
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['campaign_sk', 'date_key']) }} AS campaign_date_sk,
  *,
  CURRENT_TIMESTAMP() AS last_modified_timestamp
FROM base
