{{ config(
    alias='dim_country',
    materialized='table',
    unique_key='country_code'
) }}

WITH movies_countries AS (
  SELECT 
    distinct unnest(production_countries).iso_3166_1 AS country_code, 
    unnest(production_countries).name AS country_name 
  FROM 
    {{ source('main', 'movies') }}
), 
series_countries AS (
  SELECT 
    distinct unnest(origin_country) AS country_code 
  FROM 
    {{ source('main', 'series') }}
) 
SELECT 
  DISTINCT sc.country_code, 
  sc.country_name 
FROM 
  movies_countries sc FULL 
  JOIN series_countries mc ON sc.country_code = mc.country_code 
where 
  sc.country_code is not null 
ORDER BY 
  sc.country_code ASC

