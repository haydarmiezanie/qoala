{{ config(
    alias='dim_genre',
    materialized='table',
    unique_key='genre_id'
) }}

select 
  distinct unnest(genres).id as genre_id, 
  unnest(genres).name as genre_name 
from 
  {{ source('main', 'series') }} full 
  join {{ source('main', 'movies') }} using (genres) 
ORDER BY 
  genre_id asc
