

select 
  distinct unnest(genres).id as genre_id, 
  unnest(genres).name as genre_name 
from 
  "tmdb_movies_and_series"."main"."series" full 
  join "tmdb_movies_and_series"."main"."movies" using (genres) 
ORDER BY 
  genre_id asc