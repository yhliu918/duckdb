SELECT title.title
from info_type AS it,
     keyword AS k,
     movie_info_idx,
     movie_keyword AS mk,
     title
WHERE it.info ='rating'
  AND k.keyword LIKE '%sequel%'
  AND movie_info_idx.info > '5.0'
  AND title.production_year > 2005
  AND title.id = movie_info_idx.movie_id
  AND title.id = mk.movie_id
  AND mk.movie_id = movie_info_idx.movie_id
  AND k.id = mk.keyword_id
  AND it.id = movie_info_idx.info_type_id;

