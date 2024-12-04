SELECT min(title.title)
from keyword,
     movie_info,
     movie_keyword,
     title
WHERE keyword.keyword LIKE '%sequel%'
  AND movie_info.info IN ('Bulgaria')
  AND title.production_year > 2005
  AND title.id = movie_info.movie_id
  AND title.id = movie_keyword.movie_id
  AND movie_keyword.movie_id = movie_info.movie_id
  AND keyword.id = movie_keyword.keyword_id;

