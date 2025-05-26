{{
  config(
    schema='marketing',
    materialized='incremental',
    unique_key=['campaign_sk', 'member_sk', 'contact_sk', 'lead_sk', 'opportunity_sk', 'date_key'],
    post_hook=[
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_fct_daily_performance_campaign FOREIGN KEY (CAMPAIGN_SK) REFERENCES {{ ref('dim_campaign') }} (CAMPAIGN_SK) NOT ENFORCED",
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_fct_daily_performance_member FOREIGN KEY (MEMBER_SK) REFERENCES {{ ref('dim_campaign_member_engagement') }} (MEMBER_SK) NOT ENFORCED",
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_fct_daily_performance_contact FOREIGN KEY (CONTACT_SK) REFERENCES {{ ref('dim_contact_engagement') }} (CONTACT_SK) NOT ENFORCED",
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_fct_daily_performance_lead FOREIGN KEY (LEAD_SK) REFERENCES {{ ref('dim_lead_conversion') }} (LEAD_SK) NOT ENFORCED",
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_fct_daily_performance_opportunity FOREIGN KEY (OPPORTUNITY_SK) REFERENCES {{ ref('dim_opportunity_performance') }} (OPPORTUNITY_SK) NOT ENFORCED",
      "ALTER TABLE {{ this }} ADD CONSTRAINT fk_fct_daily_performance_date FOREIGN KEY (DATE_KEY) REFERENCES {{ ref('dim_date') }} (DATE_KEY) NOT ENFORCED"
    ]
  )
}}

WITH base_joins AS (
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
        CASE WHEN dp.UNIQUE_RECIPIENTS > 0
             THEN dp.TOTAL_OPENS / dp.UNIQUE_RECIPIENTS
             ELSE NULL END AS OPEN_RATE,
        CASE WHEN dp.TOTAL_OPENS > 0
             THEN dp.TOTAL_CLICKS / dp.TOTAL_OPENS
             ELSE NULL END AS CLICK_THROUGH_RATE,
        CURRENT_TIMESTAMP() AS SOURCE_MODIFIED_TIMESTAMP
    FROM
        {{ ref('dim_campaign') }} dc
    LEFT JOIN
        {{ ref('dim_campaign_performance') }} dp ON dc.CAMPAIGN_ID = dp.CAMPAIGN_ID
    LEFT JOIN
        {{ ref('dim_campaign_member_engagement') }} dcm ON dc.CAMPAIGN_ID = dcm.CAMPAIGN_ID
    LEFT JOIN
        {{ ref('dim_contact_engagement') }} dce ON dcm.CONTACT_ID = dce.CONTACT_ID
    LEFT JOIN
        {{ ref('dim_lead_conversion') }} dlc ON dce.CONTACT_ID = dlc.CONVERTED_CONTACT_ID
    LEFT JOIN
        {{ ref('dim_opportunity_performance') }} dop ON dc.CAMPAIGN_ID = dop.CAMPAIGN_ID
    LEFT JOIN
        {{ ref('dim_date') }} dd ON dc.START_DATE <= dd.DATE_KEY AND dc.END_DATE >= dd.DATE_KEY
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
    CURRENT_TIMESTAMP() AS LAST_MODIFIED_TIMESTAMP
FROM base_joins
{% if is_incremental() %}
WHERE SOURCE_MODIFIED_TIMESTAMP > (SELECT MAX(LAST_MODIFIED_TIMESTAMP) FROM {{ this }})
{% endif %}