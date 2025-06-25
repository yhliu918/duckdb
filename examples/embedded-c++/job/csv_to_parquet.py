import sys
import pyarrow as pa
from pyarrow import csv
import pyarrow.parquet as pq
import os

tables = ['aka_name', 'aka_title', 'cast_info', 'char_name', 'comp_cast_type', 'company_name', 'company_type', 'complete_cast', 'info_type', 'keyword', 'kind_type', 'link_type', 'movie_companies', 'movie_info', 'movie_info_idx', 'movie_keyword', 'movie_link', 'name', 'person_info', 'role_type', 'title']
tables = ['movie_info_idx', 'movie_keyword', 'movie_link', 'name', 'person_info', 'role_type', 'title']
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
        "imdb_id": pa.int32(),
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
        "imdb_id": pa.int32(),
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
        "imdb_id": pa.int32(),
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
        "imdb_id": pa.int32(),
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

def skip_comment(row):
    if row[0] == "#":
        return None
def gen(table_name, file_name):
    ff = open(file_name, "r")
    header = [item for item in ff.readlines()]
    with open(file_name + ".new", "w") as f:
        for line in header:
            start = line.find(',"')
            end = line.find('",')
            
            if start != -1 and end != -1:
                # print(line[:start+1])
                # print(line[start+1:end+1])
                # print(line[end+1:])
                line = line[:start+1] + line[start+1:end+1].replace(',', '').replace('"','') + line[end+1:]
                # print(line)
            if len(line.split(",")) != len(column_types[table_name]):
                continue
            f.write(line)
    column_names = list(column_types[table_name].keys())
    table = csv.read_csv(
        file_name + ".new",
        read_options=csv.ReadOptions(column_names=column_names),
        parse_options=csv.ParseOptions(delimiter=",",  double_quote=True),
        convert_options=csv.ConvertOptions(
            column_types=column_types[table_name],
            null_values=["", "NULL", "NA"]  # 指定哪些值应被视为 NULL
        )
    )
    # table = csv.read_csv(file_name + ".new",
    #                     read_options=csv.ReadOptions(column_names=column_names),
    #                     parse_options=csv.ParseOptions(delimiter=","),
    #                     convert_options=csv.ConvertOptions(
    #                         column_types=column_types[table_name],
    #                         auto_dict_encode=True,
    #                         null_values=['',"", "NULL", "NA", "null", "na", "N/A", "n/a", "None", "none"])
    #                     )
    os.system("rm %s" % (file_name + ".new"))
    chunk_lengths = [len(chunk) for chunk in table[0].chunks]
    # rowid = []
    # start = 0
    # for lens in chunk_lengths:
    #     rowid.append(pa.array(range(start, start + lens), type=pa.int64()))
    #     start += lens
    # rowid = pa.chunked_array(rowid)
    
    # table = table.append_column("rowid", rowid)
    print(table)
    return table


# if __name__ == "__main__":
#   if len(sys.argv) < 4:
#     print("Usage: %s <table_name> <input_file> <output_file>")
#     exit()
#   table_name, input_file, output_file = sys.argv[1], sys.argv[2], sys.argv[3]
#   # print("Transforming %s..." % input_file)
#   table = gen(table_name, input_file)
#   pq.write_table(table, output_file, row_group_size=1228800)
#   print("Transforming %s done..." % input_file)

for table in tables:
    print(table)
    table_name = table
    input_file = f"/home/yihao/JOB/{table}.csv"
    output_file = f"/home/yihao/JOB/{table}.parquet"
    table = gen(table_name, input_file)
    pq.write_table(table, output_file, row_group_size=1228800)
    print("Transforming %s done..." % input_file)