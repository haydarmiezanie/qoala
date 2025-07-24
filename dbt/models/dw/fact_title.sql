{{ config(
    alias='fact_title',
    materialized='table',
    unique_key='title_id'
) }}

with movies_title as (
  select 
    md5(
      concat(
        'movies', 
        cast(id as varchar)
      )
    ) as title_id, 
    'movies' as type, 
    original_title as title, 
    release_date, 
    runtime, 
    popularity, 
    vote_average, 
    vote_count, 
    revenue, 
    budget 
  from 
    {{ source('main', 'movies') }}
), 
series_title as (
  select 
    md5(
      concat(
        'series', 
        unnest(episode_run_time), 
        cast(id as varchar)
      )
    ) as title_id, 
    'series' as type, 
    original_name as title, 
    first_air_date as release_date, 
    unnest(episode_run_time) as runtime, 
    popularity, 
    vote_average, 
    vote_count, 
    null as revenue, 
    null as budget 
  from 
    {{ source('main', 'series') }}
) 
select 
  * 
from 
  series_title full 
  join movies_title using (title_id) 
ORDER BY 
  title_id asc


