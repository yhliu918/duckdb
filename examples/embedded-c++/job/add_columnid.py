import duckdb
import pandas as pd

con = duckdb.connect(database='JOB/job_uncomtest.db')

table_name = ['aka_name', 'aka_title', 'cast_info', 'char_name', 'comp_cast_type', 'company_name', 'company_type', 'complete_cast', 'info_type', 'keyword', 'kind_type', 'link_type', 'movie_companies', 'movie_info', 'movie_info_idx', 'movie_keyword', 'movie_link', 'name', 'person_info', 'role_type', 'title']
# table_name=['movie_info']

with open('JOB/schematext.sql', 'r') as file:
    sql_script = file.read()
    con.execute(sql_script)
con.execute("Set threads TO 48;")
con.execute("PRAGMA force_compression='uncompressed';")

for table in table_name:
    print(table)
    # con.execute(f"DROP TABLE IF EXISTS {table}")
    copy_expr = f"COPY {table} FROM 'JOB/{table}.parquet' (FORMAT 'parquet')"
    con.execute(copy_expr)
    con.execute("PRAGMA force_compression='uncompressed';")
    con.execute(f"CREATE TABLE {table}2 AS SELECT row_number() OVER () AS rowid, * FROM {table}")
    con.execute(f"Update {table}2 SET rowid = rowid - 1")
    con.execute(f"DROP TABLE IF EXISTS {table}")
    con.execute(f"ALTER TABLE {table}2 RENAME TO {table}")
    
    
    

con.close()