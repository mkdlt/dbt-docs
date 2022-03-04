{{ config(schema="alchemer", materialized='table') }}

SELECT DISTINCT resp.session_id,
       "'2'"::INT  AS q_how_likely_recommend,
       "'4'"  AS q_feedback_comments,
       "'6'"  AS q_what_to_do_for_recommend,
       resp.response_id                AS response_id,

       NULLIF(resp.data_:user_id.value::string,'') AS user_id,

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
         WHERE survey_title =  'Zookal Study NPS Survey'
       )
    pivot (MAX(answer) for question_id in ('2','4','6')
       ) as p
       JOIN (SELECT *,
                    TRY_PARSE_JSON(URL_VARIABLES) data_
               FROM {{ ref('survey_response') }}
              WHERE survey_title =  'Zookal Study NPS Survey'
            ) resp
         ON resp.session_id = p.session_id