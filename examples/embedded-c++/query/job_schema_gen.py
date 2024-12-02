import sys
import pyarrow as pa
from pyarrow import csv
import pyarrow.parquet as pq
import os
import json
from enum import Enum, auto

logical_types = {
    pa.int8(): "int8",
    pa.int16(): "int16",
    pa.int32(): "int32",
    pa.int64(): "int64",
    pa.date32(): "date32",
    pa.time32("s"): "TIME",
    pa.string(): "string",
    pa.float64(): "double",
    pa.float32(): "float32"
}
class LogicalTypeId(Enum):
    int8 = 11
    int16 = 12
    int32 = 13
    int64 = 14
    date32 = 15
    TIME = 16
    DECIMAL = 21
    float32 = 22
    double = 23
    string = 25
    BLOB = 26
    INTERVAL = 27
    UTINYINT = 28
    USMALLINT = 29
    UINTEGER = 30
    UBIGINT = 31
    TIMESTAMP_TZ = 32
    TIME_TZ = 34
    BIT = 36
    STRING_LITERAL = 37  # string literals, used for constant strings - only exists while binding
    INTEGER_LITERAL = 38  # integer literals, used for constant integers - only exists while binding
    VARINT = 39
    UHUGEINT = 49
    HUGEINT = 50
    POINTER = 51
    VALIDITY = 53
    UUID = 54
    STRUCT = 100
    LIST = 101
    MAP = 102
    TABLE = 103
    ENUM = 104
    AGGREGATE_STATE = 105
    LAMBDA = 106
    UNION = 107
    ARRAY = 108



tables = ['aka_name', 'aka_title', 'cast_info', 'char_name', 'comp_cast_type', 'company_name', 'company_type', 'complete_cast', 'info_type', 'keyword', 'kind_type', 'link_type', 'movie_companies', 'movie_info', 'movie_info_idx', 'movie_keyword', 'movie_link', 'name', 'person_info', 'role_type', 'title']

column_types = {
    "aka_name": {
        "id": pa.int32(),
        "person_id": pa.int32(),
        "name": pa.string(),
        "imdb_index": pa.string(),
        "name_pcode_cf": pa.string(),
        "name_pcode_nf": pa.string(),
        "surname_pcode": pa.string(),
        "md5sum": pa.string()
    },
    "aka_title": {
        "id": pa.int32(),
        "movie_id": pa.int32(),
        "title": pa.string(),
        "imdb_index": pa.string(),
        "kind_id": pa.int32(),
        "production_year": pa.int32(),
        "phonetic_code": pa.string(),
        "episode_of_id": pa.int32(),
        "season_nr": pa.int32(),
        "episode_nr": pa.int32(),
        "note": pa.string(),
        "md5sum": pa.string()
    },
    "cast_info": {
        "id": pa.int32(),
        "person_id": pa.int32(),
        "movie_id": pa.int32(),
        "person_role_id": pa.int32(),
        "note": pa.string(),
        "nr_order": pa.int32(),
        "role_id": pa.int32()
    },
    "char_name": {
        "id": pa.int32(),
        "name": pa.string(),
        "imdb_index": pa.string(),
        "imdb_id": pa.string(),
        "name_pcode_nf": pa.string(),
        "surname_pcode": pa.string(),
        "md5sum": pa.string()
    },
    "comp_cast_type": {
        "id": pa.int32(),
        "kind": pa.string()
    },
    "company_name": {
        "id": pa.int32(),
        "name": pa.string(),
        "country_code": pa.string(),
        "imdb_id": pa.string(),
        "name_pcode_nf": pa.string(),
        "name_pcode_sf": pa.string(),
        "md5sum": pa.string()
    },
    "company_type": {
        "id": pa.int32(),
        "kind": pa.string()
    },
    "complete_cast": {
        "id": pa.int32(),
        "movie_id": pa.int32(),
        "subject_id": pa.int32(),
        "status_id": pa.int32()
    },
    "info_type": {
        "id": pa.int32(),
        "info": pa.string()
    },
    "keyword": {
        "id": pa.int32(),
        "keyword": pa.string(),
        "phonetic_code": pa.string()
    },
    "kind_type": {
        "id": pa.int32(),
        "kind": pa.string()
    },
    "link_type": {
        "id": pa.int32(),
        "link": pa.string()
    },
    "movie_companies": {
        "id": pa.int32(),
        "movie_id": pa.int32(),
        "company_id": pa.int32(),
        "company_type_id": pa.int32(),
        "note": pa.string()
    },
    "movie_info_idx": {
        "id": pa.int32(),
        "movie_id": pa.int32(),
        "info_type_id": pa.int32(),
        "info": pa.string(),
        "note": pa.string()
    },
    "movie_keyword": {
        "id": pa.int32(),
        "movie_id": pa.int32(),
        "keyword_id": pa.int32()
    },
    "movie_link": {
        "id": pa.int32(),
        "movie_id": pa.int32(),
        "linked_movie_id": pa.int32(),
        "link_type_id": pa.int32()
    },
    "name":{
        "id": pa.int32(),
        "name": pa.string(),
        "imdb_index": pa.string(),
        "imdb_id": pa.string(),
        "gender": pa.string(),
        "name_pcode_cf": pa.string(),
        "name_pcode_nf": pa.string(),
        "surname_pcode": pa.string(),
        "md5sum": pa.string()
    },
    "role_type": {
        "id": pa.int32(),
        "role": pa.string()
    },
    "title": {
        "id": pa.int32(),
        "title": pa.string(),
        "imdb_index": pa.string(),
        "kind_id": pa.int32(),
        "production_year": pa.int32(),
        "imdb_id": pa.string(),
        "phonetic_code": pa.string(),
        "episode_of_id": pa.int32(),
        "season_nr": pa.int32(),
        "episode_nr": pa.int32(),
        "series_years": pa.string(),
        "md5sum": pa.string()
    },
    "movie_info": {
        "id": pa.int32(),
        "movie_id": pa.int32(),
        "info_type_id": pa.int32(),
        "info": pa.string(),
        "note": pa.string()
    },
    "person_info": {
        "id": pa.int32(),
        "person_id": pa.int32(),
        "info_type_id": pa.int32(),
        "info": pa.string(),
        "note": pa.string()
    }
}

data = {}
    
for table in tables:
    print(f"Creating schema for table {table}")
    schema = pa.schema([pa.field(name, type) for name, type in column_types[table].items()])
    data[table] = {}
    col_id = 0
    data[table]["rowid({})".format(table)] = {'col_id':col_id, 'type':LogicalTypeId.int64.value}
    col_id+=1
    for field in schema:
        data[table][table+"."+field.name] = {'col_id':col_id, 'type':LogicalTypeId[logical_types[field.type]].value}
        col_id+=1
    
with open(f"job_schema.json", "w") as f:
    json.dump(data, f, indent=4)

