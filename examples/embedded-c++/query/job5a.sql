SELECT title.title
from company_type AS ct,
     info_type AS it,
     movie_companies AS mc,
     movie_info AS mi,
     title
WHERE ct.kind = 'production companies'
  AND mc.note LIKE '%(theatrical)%'
  AND mc.note LIKE '%(France)%'
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Denish',
                  'Norwegian',
                  'German')
  AND title.production_year > 2005
  AND title.id = mi.movie_id
  AND title.id = mc.movie_id
  AND mc.movie_id = mi.movie_id
  AND ct.id = mc.company_type_id
  AND it.id = mi.info_type_id;

