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



tables = [
    "customer", "lineitem", "nation", "orders", "partsupp", "part", "region",
    "supplier"
]

column_types = {
    "customer": {
        "c_custkey": pa.int64(),
        "c_name": pa.string(),
        "c_address": pa.string(),
        "c_nationkey": pa.int32(),
        "c_phone": pa.string(),
        "c_acctbal": pa.float64(),
        "c_mktsegment": pa.string(),
        "c_comment": pa.string(),
    },
    "lineitem": {
        "l_orderkey": pa.int64(),
        "l_partkey": pa.int64(),
        "l_suppkey": pa.int64(),
        "l_linenumber": pa.int64(),
        "l_quantity": pa.float64(),
        "l_extendedprice": pa.float64(),
        "l_discount": pa.float64(),
        "l_tax": pa.float64(),
        "l_returnflag": pa.string(),
        "l_linestatus": pa.string(),
        "l_shipdate": pa.date32(),
        "l_commitdate": pa.date32(),
        "l_receiptdate": pa.date32(),
        "l_shipinstruct": pa.string(),
        "l_shipmode": pa.string(),
        "l_comment": pa.string(),
    },
    "nation": {
        "n_nationkey": pa.int32(),
        "n_name": pa.string(),
        "n_regionkey": pa.int32(),
        "n_comment": pa.string()
    },
    "orders": {
        "o_orderkey": pa.int64(),
        "o_custkey": pa.int64(),
        "o_orderstatus": pa.string(),
        "o_totalprice": pa.float64(),
        "o_orderdate": pa.date32(),
        "o_orderpriority": pa.string(),
        "o_clerk": pa.string(),
        "o_shippriority": pa.int32(),
        "o_comment": pa.string()
    },
    "partsupp": {
        "ps_partkey": pa.int64(),
        "ps_suppkey": pa.int64(),
        "ps_availqty": pa.int64(),
        "ps_supplycost": pa.float64(),
        "ps_comment": pa.string()
    },
    "part": {
        "p_partkey": pa.int64(),
        "p_name": pa.string(),
        "p_mfgr": pa.string(),
        "p_brand": pa.string(),
        "p_type": pa.string(),
        "p_size": pa.int32(),
        "p_container": pa.string(),
        "p_retailprice": pa.float64(),
        "p_comment": pa.string()
    },
    "region": {
        "r_regionkey": pa.int32(),
        "r_name": pa.string(),
        "r_comment": pa.string()
    },
    "supplier": {
        "s_suppkey": pa.int64(),
        "s_name": pa.string(),
        "s_address": pa.string(),
        "s_nationkey": pa.int32(),
        "s_phone": pa.string(),
        "s_acctbal": pa.float64(),
        "s_comment": pa.string()
    }
}

data = {}
    
for table in tables:
    print(f"Creating schema for table {table}")
    schema = pa.schema([pa.field(name, type) for name, type in column_types[table].items()])
    data[table] = {}
    col_id = 0
    for field in schema:
        data[table][field.name] = {'col_id':col_id, 'type':LogicalTypeId[logical_types[field.type]].value}
        col_id+=1
    data[table]["rowid({})".format(table)] = {'col_id':col_id, 'type':LogicalTypeId.int64.value}
    col_id+=1
with open(f"tpch_schema.json", "w") as f:
    json.dump(data, f, indent=4)

