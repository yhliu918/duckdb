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



tables = ['dim_date', 'customer', 'part', 'supplier', 'lineorder']


column_types = {
    'dim_date': {
        'd_datekey': pa.int32(),
        'd_date': pa.string(),
        'd_dayofweek': pa.string(),
        'd_month': pa.string(),
        'd_year': pa.int32(),
        'd_yearmonthnum': pa.int32(),
        'd_yearmonth': pa.string(),
        'd_daynuminweek': pa.int32(),
        'd_daynuminmonth': pa.int32(),
        'd_daynuminyear': pa.int32(),
        'd_monthnuminyear': pa.int32(),
        'd_weeknuminyear': pa.int32(),
        'd_sellingseason': pa.string(),
        'd_lastdayinweekfl': pa.int32(),
        'd_lastdayinmonthfl': pa.int32(),
        'd_holidayfl': pa.int32(),
        'd_weekdayfl': pa.int32()
    },
    'customer': {
        'c_custkey': pa.int32(),
        'c_name': pa.string(),
        'c_address': pa.string(),
        'c_city': pa.string(),
        'c_nation': pa.string(),
        'c_region': pa.string(),
        'c_phone': pa.string(),
        'c_mktsegment': pa.string()
    },
    'part': {
        'p_partkey': pa.int32(),
        'p_name': pa.string(),
        'p_mfgr': pa.string(),
        'p_category': pa.string(),
        'p_brand1': pa.string(),
        'p_color': pa.string(),
        'p_type': pa.string(),
        'p_size': pa.int32(),
        'p_container': pa.string()
    },
    'supplier': {
        's_suppkey': pa.int32(),
        's_name': pa.string(),
        's_address': pa.string(),
        's_city': pa.string(),
        's_nation': pa.string(),
        's_region': pa.string(),
        's_phone': pa.string()
    },
    'lineorder': {
        'lo_orderkey': pa.int32(),
        'lo_linenumber': pa.int32(),
        'lo_custkey': pa.int32(),
        'lo_partkey': pa.int32(),
        'lo_suppkey': pa.int32(),
        'lo_orderdate': pa.int32(),
        'lo_orderpriority': pa.string(),
        'lo_shippriority': pa.string(),
        'lo_quantity': pa.int32(),
        'lo_extendedprice': pa.int32(),
        'lo_ordtotalprice': pa.int32(),
        'lo_discount': pa.int32(),
        'lo_revenue': pa.int32(),
        'lo_supplycost': pa.int32(),
        'lo_tax': pa.int32(),
        'lo_commitdate': pa.int32(),
        'lo_shipmode': pa.string()
    }
}

data = {}
    
for table in tables:
    print(f"Creating schema for table {table}")
    parquet_file = pq.ParquetFile(f'/home/yihao/StarSchemaBenchmark/{table}.parquet')

    num_rows = parquet_file.metadata.num_rows

    print(f"Number of rows: {num_rows}")
    schema = pa.schema([pa.field(name, type) for name, type in column_types[table].items()])
    data[table] = {}
    col_id = 0
    for field in schema:
        data[table][table+'.'+field.name] = {'col_id':col_id, 'type':LogicalTypeId[logical_types[field.type]].value}
        col_id+=1
    data[table]["rowid({})".format(table)] = {'col_id':col_id, 'type':LogicalTypeId.int32.value}
    col_id+=1
    data[table]["table_size"] = num_rows
with open(f"ssb_schema.json", "w") as f:
    json.dump(data, f, indent=4)

