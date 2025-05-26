WITH filtered_lead AS (
  SELECT * FROM {{ ref('lead') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp) FROM {{ this }}
    )
  {% endif %}
),

filtered_contact AS (
  SELECT * FROM {{ ref('contact') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (
      SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01'::timestamp) FROM {{ this }}
    )
  {% endif %}
),

source_data AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['l.lead_id']) }} AS lead_sk,
    l.lead_id,
    l.first_name,
    l.last_name,
    l.company,
    l.email,
    l.phone,
    l.status,
    l.converted_contact_id IS NOT NULL AS is_converted,
    l.converted_contact_id,
    MIN(c.created_date) AS conversion_date,
    DATEDIFF('day', l.created_date, MIN(c.created_date)) AS days_to_convert
  FROM filtered_lead l
  LEFT JOIN filtered_contact c ON l.converted_contact_id = c.contact_id
  GROUP BY
    l.lead_id,
    l.first_name,
    l.last_name,
    l.company,
    l.email,
    l.phone,
    l.status,
    l.converted_contact_id,
    l.created_date
)

SELECT 
  *,
  CURRENT_TIMESTAMP() AS last_modified_timestamp
FROM source_data
