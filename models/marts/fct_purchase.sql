
WITH cte_purchse_conversion AS (
  SELECT
    event_date,
    DATETIME(TIMESTAMP(event_timestamp_utc), 'Europe/Prague') AS event_timestamp_cet,
    COALESCE( -- doplnění user_id kde uživatel není přihlášený, popř. kde zakazuje analytiku
        IF(LENGTH(user_id) > 12 OR user_id = '-1', NULL, user_id),
        SUBSTRING(MIN(CAST(UNIX_SECONDS(TIMESTAMP(event_timestamp_utc)) AS STRING) || IF(LENGTH(user_id) > 12 OR user_id = '-1', NULL, user_id)) OVER(PARTITION BY user_pseudo_id ORDER BY event_timestamp_utc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), 11),  -- hledám nejbližší budoucí user_id
        SUBSTRING(MAX(CAST(UNIX_SECONDS(TIMESTAMP(event_timestamp_utc)) AS STRING) || IF(LENGTH(user_id) > 12 OR user_id = '-1', NULL, user_id)) OVER(PARTITION BY user_pseudo_id ORDER BY event_timestamp_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 11), -- hledám nejbližší minulé user_id
        user_id
      ) AS user_id_calculated,
    user_pseudo_id,
    customer_type,
    source_platform,
    ga_session_id,
    transaction_id,
    event_name,
  FROM {{ ref('stg_events') }}
  WHERE (event_name IN ('add_to_cart', 'view_cart', 'begin_checkout', 'add_contact_info', 'add_shipping_info')
      OR (event_name = 'purchase' AND transaction_id IN (SELECT order_id_sap FROM {{ source('BI', 'BI_ORDER_ADDITIONAL') }}))) -- vylučuji chybné / duplicitní purchase eventy
  QUALIFY user_id_calculated IS NOT NULL AND user_id_calculated <> '-1'
),
cte_purchse AS (
  SELECT
    pc.event_date,
    pc.event_timestamp_cet,
    pc.user_id_calculated AS user_id,
    pc.event_name,
    pc.source_platform,
    pc.customer_type,
    pc.ga_session_id,
    FIRST_VALUE(IF(pc.event_name = 'purchase', pc.transaction_id, NULL) IGNORE NULLS) OVER(PARTITION BY pc.user_id_calculated ORDER BY pc.event_timestamp_cet ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS nearest_transaction_id
  FROM cte_purchse_conversion pc
)
SELECT
  pur.event_date,
  pur.user_id,
  pur.nearest_transaction_id AS transaction_id,
  oa.is_additional_order,
  pur.source_platform,
  pur.customer_type,
  pur.ga_session_id,
  CASE
    WHEN pur.customer_type IN ('Registered', 'Anonymous') OR pur.customer_type IS NULL THEN 'Potential Buyer'
    ELSE 'Existing Buyer'
  END AS ga_segment,
  MIN(IF(pur.event_name = 'add_to_cart', event_timestamp_cet, NULL)) OVER(PARTITION BY pur.nearest_transaction_id) AS first_add_to_cart,
  MAX(IF(pur.event_name = 'add_to_cart', event_timestamp_cet, NULL)) OVER(PARTITION BY pur.nearest_transaction_id) AS last_add_to_cart,
  MIN(IF(pur.event_name = 'begin_checkout', event_timestamp_cet, NULL)) OVER(PARTITION BY pur.nearest_transaction_id) AS first_begin_checkout,
  MAX(IF(pur.event_name = 'begin_checkout', event_timestamp_cet, NULL)) OVER(PARTITION BY pur.nearest_transaction_id) AS last_begin_checkout,
  MAX(IF(pur.event_name = 'begin_checkout', event_timestamp_cet, NULL)) OVER(PARTITION BY pur.nearest_transaction_id, pur.ga_session_id) AS last_begin_checkout_same_session,
  pur.event_timestamp_cet AS purchase_timestamp
FROM cte_purchse pur
INNER JOIN {{ source('BI', 'BI_ORDER_ADDITIONAL') }} oa
    ON pur.nearest_transaction_id = oa.order_id_sap
QUALIFY pur.event_name = 'purchase'
ORDER BY pur.event_timestamp_cet DESC