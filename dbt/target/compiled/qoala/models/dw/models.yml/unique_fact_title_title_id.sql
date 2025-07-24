
    
    

select
    title_id as unique_field,
    count(*) as n_records

from tmdb_movies_and_series.main_dw.fact_title
where title_id is not null
group by title_id
having count(*) > 1


