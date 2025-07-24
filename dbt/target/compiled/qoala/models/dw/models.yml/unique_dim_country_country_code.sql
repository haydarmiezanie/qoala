
    
    

select
    country_code as unique_field,
    count(*) as n_records

from tmdb_movies_and_series.main_dw.dim_country
where country_code is not null
group by country_code
having count(*) > 1


