{{ config(schema="alchemer", materialized='table') }}

SELECT DISTINCT resp.session_id,

       "'2'"::INT  AS q_how_likely_recommend,
       "'3'"  AS q_areas_to_improve,
       SPLIT_PART("'3'",' | ',1) AS q_areas_to_improve_1,
       SPLIT_PART("'3'",' | ',2) AS q_areas_to_improve_2,

       "'4'"  AS q_cs_to_improve,
       "'6'"  AS q_price_to_improve,
       "'7'"  AS q_delivery_to_improve,
       "'9'"  AS q_returns_to_improve,
       "'10'" AS q_products_to_improve,
       "'11'" AS q_communication_to_improve,
       "'12'" AS q_website_to_improve,
       "'13'" AS q_company_to_improve,
       SPLIT_PART("'13'",' | ',1) AS q_company_to_improve_1,
       SPLIT_PART("'13'",' | ',2) AS q_company_to_improve_2,
       SPLIT_PART("'13'",' | ',3) AS q_company_to_improve_3,

       "'16'" AS q_feedback_comments,
       "'21'" AS q_main_reason_for_score,
       resp.response_id                AS response_id,
       COALESCe(NULLIF(resp.data_:email.value::string,''), shopify_order.customer_email)  AS email,

       NULLIF(COALESCE(resp.data_:order_number.value::string,
                resp.data_:last_order_reference.value::string),'') AS order_reference,

       resp.utm_campaign AS utm_campaign,
       resp.utm_medium   AS utm_medium,
       resp.utm_source   AS utm_source,

       resp.response_status,
       resp.response_is_test_data,
       resp.date_submitted,
       resp.date_submitted_utc,
       resp.date_started,
       resp.date_started_utc,
       resp.url_variables,
       resp.ip_address,
       resp.referer,
       resp.user_agent,
       resp.response_time,
       resp.data_quality,
       resp.longitude,
       resp.latitude,
       resp.country,
       resp.city,
       resp.region,
       resp.postal
  from (SELECT session_id,
               question_id,
               answer
          FROM {{ ref('survey_response') }}
         WHERE survey_title =  'Zookal NPS - New eCommerce'
       )
    pivot (MAX(answer) for question_id in ('2','3','4','6','7','9','10','11','12','13','16','21')
       ) as p
       JOIN (SELECT *,
                    TRY_PARSE_JSON(URL_VARIABLES) data_
               FROM {{ ref('survey_response') }}
              WHERE survey_title =  'Zookal NPS - New eCommerce'

            ) resp
         ON resp.session_id = p.session_id
       LEFT
       JOIN dwh.analytics_shopify.shopify_order
         ON shopify_order.order_reference = NULLIF(COALESCE(resp.data_:order_number.value::string,
                                                            resp.data_:last_order_reference.value::string),'')