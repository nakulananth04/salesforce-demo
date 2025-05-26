{{
  config(
    materialized = 'incremental',
    unique_key = 'opportunity_sk'
  )
}}

WITH filtered_opportunity AS (
  SELECT * FROM {{ ref('opportunity') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01') FROM {{ this }})
  {% endif %}
),
filtered_account AS (
  SELECT * FROM {{ ref('account') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01') FROM {{ this }})
  {% endif %}
),
filtered_campaign AS (
  SELECT * FROM {{ ref('campaign') }}
  {% if is_incremental() %}
    WHERE last_modified_timestamp > (SELECT COALESCE(MAX(last_modified_timestamp), '1900-01-01') FROM {{ this }})
  {% endif %}
),
source_data AS (
  SELECT
    o.opportunity_sk,
    o.opportunity_id,
    o.opportunity_name,
    o.account_id,
    a.account_name,
    o.stage_name,
    o.amount,
    o.close_date,
    o.campaign_id,
    c.campaign_name,
    DATEDIFF('day', o.created_date, COALESCE(o.close_date, CURRENT_DATE())) AS days_open,
    o.stage_name = 'Closed Won' AS is_won
  FROM filtered_opportunity o
  LEFT JOIN filtered_account a ON o.account_id = a.account_id
  LEFT JOIN filtered_campaign c ON o.campaign_id = c.campaign_id
)

SELECT *, CURRENT_TIMESTAMP() AS last_modified_timestamp FROM source_data