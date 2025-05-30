{{
  config(
    schema='marketing',
    materialized='incremental',
    unique_key='event_outcome_sk'
  )
}}

WITH filtered_event_outcome AS (
  SELECT * FROM {{ ref('dim_event_outcome') }}
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

base AS (

  SELECT
    eo.event_id,
    eo.subject,
    eo.outcome_status,
    eo.days_to_opportunity,
    eo.opportunity_id,

    COUNT(DISTINCT ee.engagement_id) AS total_engagements_before_event,
    COUNT(DISTINCT CASE 
      WHEN ee.engagement_type = 'Click' THEN ee.engagement_id 
    END) AS pre_event_clicks,

    MIN(DATEDIFF(
      'hour',
      ee.engagement_timestamp,
      DATEADD('day', eo.days_to_opportunity, CURRENT_DATE())
    )) AS hours_before_opportunity

  FROM filtered_event_outcome eo

  LEFT JOIN filtered_email_engagement ee 
    ON eo.event_id = ee.campaign_name
    AND ee.engagement_timestamp < DATEADD('day', eo.days_to_opportunity * -1, CURRENT_DATE())

  GROUP BY
    eo.event_id, eo.subject, eo.outcome_status, eo.days_to_opportunity, eo.opportunity_id

)

SELECT
  {{ dbt_utils.generate_surrogate_key(['event_id']) }} AS event_outcome_sk,
  *,
  CURRENT_TIMESTAMP() AS last_modified_timestamp
FROM base
