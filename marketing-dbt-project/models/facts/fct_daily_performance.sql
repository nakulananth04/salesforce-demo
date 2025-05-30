{{
  config(
    schema='marketing',
    materialized='incremental',
    unique_key='daily_performance_sk'
  )
}}

WITH filtered_campaign AS (
  SELECT * FROM {{ ref('dim_campaign') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz) FROM {{ this }}
    )
  {% endif %}
),

filtered_campaign_performance AS (
  SELECT * FROM {{ ref('dim_campaign_performance') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz) FROM {{ this }}
    )
  {% endif %}
),

filtered_campaign_member_engagement AS (
  SELECT * FROM {{ ref('dim_campaign_member_engagement') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz) FROM {{ this }}
    )
  {% endif %}
),

filtered_contact_engagement AS (
  SELECT * FROM {{ ref('dim_contact_engagement') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz) FROM {{ this }}
    )
  {% endif %}
),

filtered_lead_conversion AS (
  SELECT * FROM {{ ref('dim_lead_conversion') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz) FROM {{ this }}
    )
  {% endif %}
),

filtered_opportunity_performance AS (
  SELECT * FROM {{ ref('dim_opportunity_performance') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz) FROM {{ this }}
    )
  {% endif %}
),

base_joins AS (
    SELECT
        dc.CAMPAIGN_SK,
        dcm.MEMBER_SK,
        dce.CONTACT_SK,
        dlc.LEAD_SK,
        dop.OPPORTUNITY_SK,
        dd.DATE_KEY,
        dc.BUDGET AS CAMPAIGN_BUDGET,
        dp.UNIQUE_RECIPIENTS,
        dp.TOTAL_OPENS,
        dp.TOTAL_CLICKS,
        dcm.TOTAL_ENGAGEMENTS,
        dcm.EMAIL_OPENS,
        dcm.EVENT_ATTENDANCES,
        dlc.IS_CONVERTED,
        dlc.DAYS_TO_CONVERT,
        dop.AMOUNT AS OPPORTUNITY_AMOUNT,
        dop.DAYS_OPEN,
        dop.IS_WON,
        CASE WHEN dp.UNIQUE_RECIPIENTS > 0 THEN dp.TOTAL_OPENS / dp.UNIQUE_RECIPIENTS ELSE NULL END AS OPEN_RATE,
        CASE WHEN dp.TOTAL_OPENS > 0 THEN dp.TOTAL_CLICKS / dp.TOTAL_OPENS ELSE NULL END AS CLICK_THROUGH_RATE,
        CURRENT_TIMESTAMP() AS SOURCE_MODIFIED_TIMESTAMP
    FROM filtered_campaign dc
    LEFT JOIN filtered_campaign_performance dp ON dc.CAMPAIGN_ID = dp.CAMPAIGN_ID
    LEFT JOIN filtered_campaign_member_engagement dcm ON dc.CAMPAIGN_ID = dcm.CAMPAIGN_ID
    LEFT JOIN filtered_contact_engagement dce ON dcm.CONTACT_ID = dce.CONTACT_ID
    LEFT JOIN filtered_lead_conversion dlc ON dce.CONTACT_ID = dlc.CONVERTED_CONTACT_ID
    LEFT JOIN filtered_opportunity_performance dop ON dc.CAMPAIGN_ID = dop.CAMPAIGN_ID
    LEFT JOIN {{ ref('dim_date') }} dd ON dc.START_DATE <= dd.DATE_KEY AND COALESCE(dc.END_DATE, CURRENT_DATE()) >= dd.DATE_KEY
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'campaign_sk',
        'member_sk',
        'contact_sk',
        'lead_sk',
        'opportunity_sk',
        'date_key'
    ]) }} AS daily_performance_sk,
    campaign_sk,
    member_sk,
    contact_sk,
    lead_sk,
    opportunity_sk,
    date_key,
    CAMPAIGN_BUDGET,
    UNIQUE_RECIPIENTS,
    TOTAL_OPENS,
    TOTAL_CLICKS,
    TOTAL_ENGAGEMENTS,
    EMAIL_OPENS,
    EVENT_ATTENDANCES,
    IS_CONVERTED,
    DAYS_TO_CONVERT,
    OPPORTUNITY_AMOUNT,
    DAYS_OPEN,
    IS_WON,
    OPEN_RATE,
    CLICK_THROUGH_RATE,
    CURRENT_TIMESTAMP() AS last_modified_timestamp
FROM base_joins
