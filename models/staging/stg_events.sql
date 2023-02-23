{{
    config(
        pre_hook=[
            "DELETE FROM `ga-kosik-02-2022.dbt_zhanik.test_table` WHERE customer_id_sap = 1039635043;"
        ]
    )
}}

{%- set events_list = ['add_shipping_info', 'add_contact_info', 'add_to_cart', 'begin_checkout', 'delete_cart',
    'login', 'login_start', 'purchase', 'remove_from_cart', 'search_click', 'view_cart', 'view_item', 'error_message'] -%}

WITH cte_ga_events AS (
  SELECT -- web
    event_date,
    event_timestamp,
    user_id,
    user_pseudo_id,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'customer_type') AS customer_type,
    event_name,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_type') AS page_type,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') AS ga_session_number,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'login_status') AS login_status,
    ROUND((SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value'), 2) AS value,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'transaction_id') AS transaction_id,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'purchase_type') AS purchase_type,
    platform,
    device.category AS device_category,
    device.operating_system,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'warehouse_name') AS warehouse_name,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'destination_name') AS destination_name,
    geo.country,
    geo.region,
    geo.city,
    -- položky (items) pouze pro event add_to_cart, ostatní (purchase, remove_from_cart) mohou generovat více položek
    -- položky mají vždy pouze jeden item proto LIMIT 1
    IF(event_name = 'add_to_cart', (SELECT item_id FROM UNNEST(items) LIMIT 1), NULL) AS item_id,
    IF(event_name = 'add_to_cart', (SELECT item_name FROM UNNEST(items) LIMIT 1), NULL) AS item_name,
    IF(event_name = 'add_to_cart', (SELECT price FROM UNNEST(items) LIMIT 1), NULL) AS item_price,
    IF(event_name = 'add_to_cart', (SELECT quantity FROM UNNEST(items) LIMIT 1), NULL) AS item_quantity,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'full_url') AS full_url,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location, -- potrebujeme k necemu?
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'widget_category') AS widget_category,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'widget_name') AS widget_name,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'widget_code') AS widget_code,
    null as app_item_source -- only for app
  FROM {{ source('ga4_web', 'events') }}
  WHERE _TABLE_SUFFIX BETWEEN {{ get_table_suffix(10) }} AND {{ get_table_suffix(1) }}
    AND event_name IN UNNEST({{ events_list }})

  UNION ALL

  SELECT -- app
    event_date,
    event_timestamp,
    user_id,
    user_pseudo_id,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'customer_type') AS customer_type, -- není v app
    event_name,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'firebase_screen') AS page_type, -- jiný název v app
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') AS ga_session_number,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'login_status') AS login_status, -- není v app
    ROUND((SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value'), 2) AS value,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'transaction_id') AS transaction_id,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'order_type') AS purchase_type, -- jiný název v app
    platform,
    device.category AS device_category,
    device.operating_system,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'warehouse_name') AS warehouse_name, -- není v app
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'destination_id') AS destination_name, -- jiný název v app
    geo.country,
    geo.region,
    geo.city,
    -- položky (items) pouze pro event add_to_cart, ostatní (purchase, remove_from_cart) mohou generovat více položek
    -- položky mají vždy pouze jeden item proto LIMIT 1
    IF(event_name = 'add_to_cart', (SELECT item_id FROM UNNEST(items) LIMIT 1), NULL) AS item_id,
    IF(event_name = 'add_to_cart', (SELECT item_name FROM UNNEST(items) LIMIT 1), NULL) AS item_name,
    IF(event_name = 'add_to_cart', (SELECT price FROM UNNEST(items) LIMIT 1), NULL) AS item_price,
    IF(event_name = 'add_to_cart', (SELECT quantity FROM UNNEST(items) LIMIT 1), NULL) AS item_quantity,
    NULL AS full_url, -- není v app
    null as page_location, -- neni v app
    null as widget_category, -- asi neni v app?
    null as widget_name, -- asi neni v app?
    null as widget_code, -- asi neni v app?
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'item_source') AS app_item_source
  FROM {{ source('ga4_app', 'events') }}
  WHERE _TABLE_SUFFIX BETWEEN {{ get_table_suffix(10) }} AND {{ get_table_suffix(1) }}
    AND event_name IN UNNEST(({{ events_list }}))
)
SELECT
  PARSE_DATE('%Y%m%d', event_date) AS event_date, -- je v časové zóně webu, tzn. pro nás v CET
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', TIMESTAMP_MICROS(event_timestamp)) AS event_timestamp_utc, -- by default je v UTC
  user_id,
  user_pseudo_id,
  customer_type,
  event_name,
  page_type,
  ga_session_id,
  ga_session_number,
  login_status,
  value,
  transaction_id,
  purchase_type,
  platform,
  device_category,
  operating_system,
  warehouse_name,
  destination_name,
  country,
  region,
  city,
  item_id,
  item_name,
  item_price,
  item_quantity,
  CASE
    WHEN platform = 'WEB' AND device_category IN ('mobile', 'tablet') THEN 'mobile-web'
    WHEN platform = 'WEB' THEN 'web'
    WHEN platform = 'IOS' THEN 'mobile-ios'
    WHEN platform = 'ANDROID' THEN 'mobile-android'
  END AS source_platform,
  full_url,
  CASE
    WHEN page_type IN ('cart', 'alternatives') THEN 'cart'
    WHEN page_type IN ('muj_kosik', 'muj_kosik_categories') OR (page_type = 'category' AND full_url = 'https://www.kosik.cz/muj-kosik') THEN 'muj_kosik'
    WHEN page_type IN ('category', 'categories') THEN 'category'
    WHEN page_type IN ('specific_category', 'specific-category') THEN 'specific_category' -- akce, vice za mene atd - chceme drzet oddelene od beznych L kategorii kvuli sledovani page type detailu. Ne vsechny zalozky z menu jsou jako specific kategorie, napr. Delmart, ty jsou bohuzel jako bezne
    WHEN page_type IN ('homepage', 'home') THEN 'homepage'
    WHEN page_type IN ('search', 'searchresults') THEN 'search'
    WHEN page_type IN ('product', 'product_detail', 'product_pictograms_detail', 'similar_products') THEN 'product_detail'
    WHEN page_type IN ('profile', 'profile_orders', 'order_detail') THEN 'order_detail'
    ELSE 'other'
  END AS page_type_grouped, -- nepotrebujeme
  page_type as page_type_detail, -- nepotrebujeme
  page_location, -- nepotrebujeme

-- reporting rozdeleni pro add to cart zdroj - nevychazet z page_type, tam je originalni misto odkud byl otevreny modal (napr. category, i kdyz je pridane ze searche) 
  case 
    --web
    when app_item_source is null and event_name = 'add_to_cart' and  widget_name in ('specific_category','product_page_category') and page_location like '%kampan=hp_top%' then "specific_category" -- horni menu
    when app_item_source is null and event_name = 'add_to_cart' and widget_name = 'specific_category' then "specific_category"
    when app_item_source is null and event_name = 'add_to_cart' and widget_name = 'product_page_category' then "category"
    when app_item_source is null and event_name = 'add_to_cart' and widget_name in ('product_page_search','search_suggestions','search_multiple') then "search"
    when app_item_source is null and event_name = 'add_to_cart' and widget_name in ('checkout_lower','checkout_upper','checkout_cart') then "checkout"    
    when app_item_source is null and event_name = 'add_to_cart' and widget_name = 'other_widgets' and full_url = "https://www.kosik.cz/muj-kosik" then "muj_kosik"
    when app_item_source is null and event_name = 'add_to_cart' and widget_name = 'hp' then "homepage"
    when app_item_source is null and event_name = 'add_to_cart' and widget_name = 'microsites' and widget_code = 'LPBenjaminek' then "homepage" -- benjaminek widget
    
    when app_item_source is null and event_name = 'add_to_cart' and widget_name in ('product_detail','my_orders','preview_cart') then widget_name -- prevzit beze zmeny, zbytek others
    when app_item_source is null and event_name = 'add_to_cart' then "others"

    --app
    when app_item_source is not null and event_name = 'add_to_cart' and page_type = 'cart' then 'preview_cart'  -- sjednotit na web nazvy
    when app_item_source is not null and event_name = 'add_to_cart' and page_type = 'order_detail' then 'my_orders'  
    when app_item_source is not null and event_name = 'add_to_cart' and page_type in ('category','search', 'product_detail', 'homepage', 'muj_kosik') then page_type  -- vybrat ktere chceme mit detailne, zbytek jako others
    when app_item_source is not null and event_name = 'add_to_cart' then 'others'

    else null
  end as reporting_page_type,
  
-- detail pro zdroj
  case 
  -- web
    when app_item_source is null and event_name = 'add_to_cart' and  widget_name in ('specific_category','product_page_category') and page_location like '%kampan=hp_top%' then SPLIT(SPLIT(replace(full_url, 'https://www.kosik.cz/', ''), '?')[OFFSET(0)], '/')[OFFSET(0)] -- odstranit prefix (kosik.cz) a sufix ( vsechno za ?) z url
    
    when app_item_source is null and event_name = 'add_to_cart' and widget_name in ('specific_category','product_page_category') then 'L'||cast(LENGTH(full_url) - LENGTH(REGEXP_REPLACE(full_url, '/', ''))-2 as string) -- poznat L1-L7 z full url podle /
    when app_item_source is null and event_name = 'add_to_cart' and widget_name = 'hp' then widget_code -- detail widgetu z HP
    when app_item_source is null and event_name = 'add_to_cart' and widget_name = 'microsites' and widget_code = 'LPBenjaminek' then "LPBenjaminek" -- benjaminek widget
    
    when app_item_source is null and event_name = 'add_to_cart' and widget_name in ('product_page_search', 'search_suggestions', 'search_multiple','checkout_lower', 'checkout_upper','checkout_cart') then widget_name -- kde v predchozim kroku groupujeme, vzit detail
    
  -- app
    when app_item_source is not null and event_name = 'add_to_cart' and page_type ='category'  then 'L'||cast(LENGTH(app_item_source) - LENGTH(REGEXP_REPLACE(app_item_source, '/', '')) as string) -- poznat L1-L7 z app_item_source podle /
    when app_item_source is not null and event_name = 'add_to_cart' and page_type ='search' and app_item_source = 'search' then 'search_suggestions' -- naparovat na stejnou search kategorii z webu
    when app_item_source is not null and event_name = 'add_to_cart' and page_type ='search' and app_item_source = 'searchUpsell' then app_item_source 
    when app_item_source is not null and event_name = 'add_to_cart' and page_type <> 'search' then app_item_source -- vypsat detail (krome zbytku search, tam je nejaky sum)

    else null

  end as reporting_page_type_detail

FROM cte_ga_events