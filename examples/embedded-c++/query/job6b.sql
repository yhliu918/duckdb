SELECT min(keyword.keyword),
       min(name.name),
       min(title.title)
from cast_info AS ci,
     keyword,
     movie_keyword AS mk,
     name,
     title
WHERE keyword.keyword IN ('superhero',
                    'sequel',
                    'second-part',
                    'marvel-comics',
                    'based-on-comic',
                    'tv-special',
                    'fight',
                    'violence')
  AND name.name LIKE '%Downey%Robert%'
  AND title.production_year > 2014
  AND keyword.id = mk.keyword_id
  AND title.id = mk.movie_id
  AND title.id = ci.movie_id
  AND ci.movie_id = mk.movie_id
  AND name.id = ci.person_id;
