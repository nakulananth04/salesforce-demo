{{
  config(
    schema='marketing',
    materialized='incremental',
    unique_key='event_outcome_sk'
  )
}}

WITH filtered_event AS (
  SELECT * FROM {{ ref('event') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp)
      FROM {{ this }}
    )
  {% endif %}
),

filtered_opportunity AS (
  SELECT * FROM {{ ref('opportunity') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp)
      FROM {{ this }}
    )
  {% endif %}
),

event_outcome_data AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['e.event_id', 'o.opportunity_id']) }} AS event_outcome_sk,
    
    e.EVENT_ID,
    e.SUBJECT,
    o.OPPORTUNITY_ID,
    o.OPPORTUNITY_NAME,
    o.STAGE_NAME,
    o.AMOUNT,

    DATEDIFF(
      'day',
      TO_TIMESTAMP(e.START_DATE_TIME),
      TO_DATE(o.CREATED_DATE)
    ) AS days_to_opportunity,

    CASE 
      WHEN o.OPPORTUNITY_ID IS NOT NULL THEN 'Has Opportunity'
      WHEN e.RELATED_LEAD_ID IS NOT NULL THEN 'Has Lead'
      WHEN e.RELATED_CONTACT_ID IS NOT NULL THEN 'Has Contact'
      ELSE 'No Link'
    END AS outcome_status

  FROM filtered_event e
  LEFT JOIN filtered_opportunity o 
    ON (e.RELATED_CONTACT_ID = o.ACCOUNT_ID OR e.RELATED_LEAD_ID = o.ACCOUNT_ID)
    AND TO_DATE(o.CREATED_DATE) BETWEEN 
        TO_TIMESTAMP(e.START_DATE_TIME)::DATE 
        AND DATEADD('day', 30, TO_TIMESTAMP(e.START_DATE_TIME)::DATE)
)

SELECT 
  *,
  CURRENT_TIMESTAMP() AS last_modified_timestamp
FROM event_outcome_data
