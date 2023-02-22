
-- pak změnit table_suffix na 93 dnů

WITH cte_web_and_app AS (
  SELECT -- web
    IF(user_id = '-1', NULL, user_id) AS user_id,
    CAST(PARSE_DATE('%Y%m%d', event_date) AS DATE) AS event_date,
    event_name,
    platform,
    device.category AS device_category
  FROM {{ source('ga4_web', 'events') }}
  WHERE _TABLE_SUFFIX >= FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("Europe/Prague"), INTERVAL 7 DAY))
    AND event_name <> 'scroll'

  UNION ALL

  SELECT -- app
    IF(user_id = '-1', NULL, user_id) AS user_id,
    CAST(PARSE_DATE('%Y%m%d', event_date) AS DATE) AS event_date,
    event_name,
    platform,
    device.category AS device_category
  FROM {{ source('ga4_app', 'events') }}
  WHERE _TABLE_SUFFIX >= FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE("Europe/Prague"), INTERVAL 7 DAY))
    AND event_name <> 'scroll'
    AND event_name NOT IN ('notification_dismiss', 'notification_foreground', 'notification_open', 'notification_receive')
),
cte_web_and_app_grouped AS (
  SELECT
    user_id,
    MAX(IF(platform = 'WEB' AND device_category NOT IN ('mobile', 'tablet'), 1, 0)) AS web_user_90d,
    MAX(IF(platform = 'WEB' AND device_category IN ('mobile', 'tablet'), 1, 0)) AS mobile_web_user_90d,
    MAX(IF(platform = 'IOS', 1, 0)) AS ios_user_90d,
    MAX(IF(platform = 'ANDROID', 1, 0)) AS android_user_90d,
    MAX(event_date) AS last_activity
  FROM cte_web_and_app
  WHERE user_id IS NOT NULL
  GROUp BY user_id
)
SELECT
  ga.user_id,
  c.customer_id_sap,
  IF(ga.web_user_90d + ga.mobile_web_user_90d + ga.ios_user_90d + ga.android_user_90d > 1,'cross-device', 'single-device') AS user_type_90d,
  ga.web_user_90d,
  ga.mobile_web_user_90d,
  ga.ios_user_90d,
  ga.android_user_90d,
  c.registration_date,
  ga.last_activity
FROM cte_web_and_app_grouped ga
LEFT JOIN `DATAMART.BI_CUSTOMERS` c
  ON ga.user_id = CAST(c.customer_id_k3w AS STRING)


