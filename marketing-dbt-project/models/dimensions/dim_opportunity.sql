{{ 
  config(
    materialized='incremental',
    unique_key='opportunity_sk'
  ) 
}}

WITH opportunity_data AS (

  SELECT 
    o.opportunity_sk, 
    o.OPPORTUNITY_ID,
    o.OPPORTUNITY_NAME,
    o.STAGE_NAME,
    o.AMOUNT,
    o.CLOSE_DATE,
    o.ACCOUNT_ID,
    a.ACCOUNT_NAME,
    a.INDUSTRY,
    c.CAMPAIGN_NAME,
    
    DATEDIFF('day', CURRENT_DATE(), o.CLOSE_DATE) AS DAYS_TO_CLOSE

  FROM {{ ref('opportunity') }} o

  LEFT JOIN {{ ref('account') }} a 
    ON o.ACCOUNT_ID = a.ACCOUNT_ID

  LEFT JOIN {{ ref('campaign') }} c 
    ON o.CAMPAIGN_ID = c.CAMPAIGN_ID

  {% if is_incremental() %}
    WHERE o.CLOSE_DATE > (
      SELECT COALESCE(MAX(CLOSE_DATE), '1900-01-01'::date)
      FROM {{ this }}
    )
  {% endif %}

)

SELECT 
  *,
  CURRENT_TIMESTAMP() AS last_modified_timestamp
FROM opportunity_data
