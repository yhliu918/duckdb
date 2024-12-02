SELECT movie_companies.note, title.title, title.production_year
from company_type AS ct, info_type AS it, movie_companies, movie_info_idx AS mi_idx, title
WHERE ct.kind = 'production companies'
  AND it.info = 'top 250 rank'
  AND movie_companies.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
  AND (movie_companies.note LIKE '%(co-production)%'
       OR movie_companies.note LIKE '%(presents)%')
  AND ct.id = movie_companies.company_type_id
  AND title.id = movie_companies.movie_id
  AND title.id = mi_idx.movie_id
  AND movie_companies.movie_id = mi_idx.movie_id
  AND it.id = mi_idx.info_type_id;