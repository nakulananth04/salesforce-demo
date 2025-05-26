{{
  config(
    schema='marketing',
    materialized='incremental',
    unique_key='lead_sk'
  )
}}

WITH filtered_lead AS (
  SELECT * FROM {{ ref('lead') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp)
      FROM {{ this }}
    )
  {% endif %}
),

filtered_contact AS (
  SELECT * FROM {{ ref('contact') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp)
      FROM {{ this }}
    )
  {% endif %}
),

lead_data AS (
  SELECT 
    {{ dbt_utils.generate_surrogate_key(['l.lead_id']) }} AS lead_sk,
    l.lead_id,
    l.first_name,
    l.last_name,
    l.company,
    l.email,
    l.phone,
    l.status,
    l.converted_contact_id,
    c.first_name AS converted_first_name,
    c.last_name AS converted_last_name,
    TO_DATE(l.created_date) AS created_date
  FROM filtered_lead l
  LEFT JOIN filtered_contact c
    ON l.converted_contact_id = c.contact_id
)

SELECT 
  *,
  CURRENT_TIMESTAMP() AS last_modified_timestamp
FROM lead_data
