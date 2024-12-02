SELECT min(title.title)
from keyword AS k,
     movie_info AS mi,
     movie_keyword AS mk,
     title
WHERE k.keyword LIKE '%sequel%'
  AND mi.info IN ('Bulgaria')
  AND title.production_year > 2005
  AND title.id = mi.movie_id
  AND title.id = mk.movie_id
  AND mk.movie_id = mi.movie_id
  AND k.id = mk.keyword_id;

