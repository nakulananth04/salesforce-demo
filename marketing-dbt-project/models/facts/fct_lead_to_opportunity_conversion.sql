{{
  config(
    schema='marketing',
    materialized='incremental',
    unique_key='lead_sk'
  )
}}

WITH filtered_lead AS (
  SELECT * FROM {{ ref('dim_lead') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz)
      FROM {{ this }}
    )
  {% endif %}
),

filtered_lead_conversion AS (
  SELECT * FROM {{ ref('dim_lead_conversion') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz)
      FROM {{ this }}
    )
  {% endif %}
),

filtered_opportunity AS (
  SELECT * FROM {{ ref('dim_opportunity') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp_ntz)
      FROM {{ this }}
    )
  {% endif %}
),

lead_base AS (
  SELECT
    l.lead_id,
    l.first_name,
    l.last_name,
    l.status,
    lc.is_converted,
    lc.conversion_date,
    lc.days_to_convert,
    l.converted_contact_id
  FROM filtered_lead l
  LEFT JOIN filtered_lead_conversion lc
    ON l.lead_id = lc.lead_id
),

opportunity_data AS (
  SELECT
    opportunity_id,
    amount,
    account_id
  FROM filtered_opportunity
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['lead_id']) }} AS lead_sk,
  lb.lead_id,
  lb.first_name,
  lb.last_name,
  lb.status,
  lb.is_converted,
  lb.conversion_date,
  lb.days_to_convert,

  COUNT(DISTINCT o.opportunity_id) AS opportunities_created,
  SUM(o.amount) AS total_pipeline,

  CURRENT_TIMESTAMP() AS last_modified_timestamp

FROM lead_base lb
LEFT JOIN opportunity_data o
  ON lb.converted_contact_id = o.account_id  -- adjust if needed

GROUP BY
  lb.lead_id, lb.first_name, lb.last_name, lb.status,
  lb.is_converted, lb.conversion_date, lb.days_to_convert
