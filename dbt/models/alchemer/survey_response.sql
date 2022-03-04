{{ config(schema="alchemer", materialized='view') }}

WITH
response AS (
    SELECT resp.survey_id,
           resp.id             AS response_id,
           resp.status         AS response_status,
           resp.is_test_data   AS response_is_test_data,
           CONVERT_TIMEZONE('UTC','Australia/Sydney',resp.date_submitted) AS date_submitted,
           resp.date_submitted AS date_submitted_utc,
           resp.session_id,
           CONVERT_TIMEZONE('UTC','Australia/Sydney',resp.date_started)   AS date_started,
           resp.date_started   AS date_started_utc,
           resp.url_variables,
           resp.utm_medium,
           resp.utm_source,
           resp.utm_campaign,
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
           resp.postal,
           survey_data.question_id,
           survey_data.id          AS survey_data_id,
           TRIM(REGEXP_REPLACE(REGEXP_REPLACE(regexp_replace(survey_data.question,' ',''), '<[^>]*>',' '),'\n',''))  AS question,
           survey_data.answer      AS answer,
           survey_data.option      AS option_value,
           survey_data.answer_id   AS answer_id,
           survey_data.section_id  AS section_id,
           survey_data.shown       AS question_shown,
           survey_data.type        AS entry_type
      FROM raw.alchemer.survey_response resp
        LEFT
        JOIN raw.alchemer.survey_response_data survey_data
          ON survey_data.response_id = resp.response_id
)
SELECT survey.title      AS survey_title,
       survey.statistics AS survey_statistics,
       response.*
  FROM response
   JOIN raw.alchemer.survey
     ON response.survey_id = survey.id
WHERE response_status = 'Complete'