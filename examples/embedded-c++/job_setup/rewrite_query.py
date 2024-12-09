import re
import sys
import glob

def parse_sql(sql):
    # 将大写的 FROM 替换为小写
    sql = re.sub(r'\bMIN\b', 'min', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bMAX\b', 'max', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bCOUNT\b', 'count', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bSUM\b', 'sum', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bAVG\b', 'avg', sql, flags=re.IGNORECASE)
    
    sql = re.sub(r'\bFROM\b', 'from', sql, flags=re.IGNORECASE)

    # 查找所有的表名和别名
    table_aliases = re.findall(r'(\w+)\s+AS\s+(\w+)', sql, flags=re.IGNORECASE)
    
    # 去掉 from 和 where 之间的表别名
    for table, alias in table_aliases:
        sql = re.sub(r'\b' + table + r'\s+AS\s+' + alias + r'\b', table, sql, flags=re.IGNORECASE)
    
    # 替换所有的表别名为表名
    for table, alias in table_aliases:
        sql = re.sub(r'\b' + alias + r'\.', table + '.', sql, flags=re.IGNORECASE)

    sql = re.sub(r'\bAS\s+\w+\b', '', sql, flags=re.IGNORECASE)

    return sql


sqls = glob.glob("/home/yihao/JOB/*.sql")

for query_file in sqls:
    sql_query = open(query_file).read()

    parsed_sql = parse_sql(sql_query)
    with open('/home/yihao/JOB/parsed/'+query_file.split('/')[-1], "w") as f:
        f.write(parsed_sql)
# query_file = sys.argv[1]
# sql_query = open(query_file).read()

# parsed_sql = parse_sql(sql_query)
# with open('/home/yihao/JOB/parsed/'+query_file, "w") as f:
#     f.write(parsed_sql)