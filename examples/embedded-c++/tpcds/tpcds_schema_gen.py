import sys
import pyarrow as pa
from pyarrow import csv
import pyarrow.parquet as pq
import os
import json
from enum import Enum, auto
import duckdb

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
   'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales', 'customer_address', 'customer_demographics', 'customer', 'date_dim', 'household_demographics', 'income_band', 'inventory', 'item', 'promotion', 'reason', 'ship_mode', 'store_returns', 'store_sales', 'time_dim', 'warehouse', 'web_page', 'web_returns', 'web_sales', 'web_site','store'
]
tables = ['store']

column_types =  {
    # ================== 维度表 (17张) ==================
    'call_center': {
    "cc_call_center_sk": pa.int32(),
    "cc_call_center_id": pa.string(),
    "cc_rec_start_date": pa.date32(),
    "cc_rec_end_date": pa.date32(),
    "cc_closed_date_sk": pa.int32(),
    "cc_open_date_sk": pa.int32(),
    "cc_name": pa.string(),
    "cc_class": pa.string(),
    "cc_employees": pa.int32(),
    "cc_sq_ft": pa.int32(),
    "cc_hours": pa.string(),
    "cc_manager": pa.string(),
    "cc_mkt_id": pa.int32(),
    "cc_mkt_class": pa.string(),
    "cc_mkt_desc": pa.string(),
    "cc_market_manager": pa.string(),
    "cc_division": pa.int32(),
    "cc_division_name": pa.string(),
    "cc_company": pa.int32(),
    "cc_company_name": pa.string(),
    "cc_street_number": pa.string(),
    "cc_street_name": pa.string(),
    "cc_street_type": pa.string(),
    "cc_suite_number": pa.string(),
    "cc_city": pa.string(),
    "cc_county": pa.string(),
    "cc_state": pa.string(),
    "cc_zip": pa.string(),
    "cc_country": pa.string(),
    "cc_gmt_offset": pa.float64(),
    "cc_tax_percentage": pa.float64(),
    "rowid(call_center)": pa.int32()
},
    'catalog_page': {
        "cp_catalog_page_sk": pa.int32(),
        "cp_catalog_page_id": pa.string(),
        "cp_start_date_sk": pa.int32(),
        "cp_end_date_sk": pa.int32(),
        "cp_catalog_number": pa.string(),
        "cp_catalog_page_number": pa.int32(),
        "cp_description": pa.string(),
        "cp_type": pa.string(),
        "cp_start_date": pa.date32(),
        "cp_end_date": pa.date32(),
        "rowid(catalog_page)": pa.int32()
    },
    'customer': {
        "c_customer_sk": pa.int32(),
        "c_customer_id": pa.string(),
        "c_current_cdemo_sk": pa.int32(),
        "c_current_hdemo_sk": pa.int32(),
        "c_current_addr_sk": pa.int32(),
        "c_first_shipto_date_sk": pa.int32(),
        "c_first_sales_date_sk": pa.int32(),
        "c_salutation": pa.string(),
        "c_first_name": pa.string(),
        "c_last_name": pa.string(),
        "c_preferred_cust_flag": pa.string(),
        "c_birth_day": pa.int32(),
        "c_birth_month": pa.int32(),
        "c_birth_year": pa.int32(),
        "c_birth_country": pa.string(),
        "c_login": pa.string(),
        "c_email_address": pa.string(),
        "c_last_review_date": pa.string(),
        "rowid(customer)": pa.int32()
    },
    'customer_address': {
        "ca_address_sk": pa.int32(),
        "ca_address_id": pa.string(),
        "ca_street_number": pa.string(),
        "ca_street_name": pa.string(),
        "ca_street_type": pa.string(),
        "ca_suite_number": pa.string(),
        "ca_city": pa.string(),
        "ca_county": pa.string(),
        "ca_state": pa.string(),
        "ca_zip": pa.string(),
        "ca_country": pa.string(),
        "ca_gmt_offset": pa.float64(),
        "ca_location_type": pa.string(),
        "rowid(customer_address)": pa.int32()
    },
    'customer_demographics': {
        "cd_demo_sk": pa.int32(),
        "cd_gender": pa.string(),
        "cd_marital_status": pa.string(),
        "cd_education_status": pa.string(),
        "cd_purchase_estimate": pa.int32(),
        "cd_credit_rating": pa.string(),
        "cd_dep_count": pa.int32(),
        "cd_dep_employed_count": pa.int32(),
        "cd_dep_college_count": pa.int32(),
        "rowid(customer_demographics)": pa.int32()
    },
    'date_dim': {
        "d_date_sk": pa.int32(),
        "d_date_id": pa.string(),
        "d_date": pa.date32(),
        "d_month_seq": pa.int32(),
        "d_week_seq": pa.int32(),
        "d_quarter_seq": pa.int32(),
        "d_year": pa.int32(),
        "d_dow": pa.int32(),
        "d_moy": pa.int32(),
        "d_dom": pa.int32(),
        "d_qoy": pa.int32(),
        "d_fy_year": pa.int32(),
        "d_fy_quarter_seq": pa.int32(),
        "d_fy_week_seq": pa.int32(),
        "d_day_name": pa.string(),
        "d_quarter_name": pa.string(),
        "d_holiday": pa.string(),
        "d_weekend": pa.string(),
        "d_following_holiday": pa.string(),
        "d_first_dom": pa.int32(),
        "d_last_dom": pa.int32(),
        "d_same_day_ly": pa.int32(),
        "d_same_day_lq": pa.int32(),
        "d_current_day": pa.string(),
        "d_current_week": pa.string(),
        "d_current_month": pa.string(),
        "d_current_quarter": pa.string(),
        "d_current_year": pa.string(),
        "rowid(date_dim)": pa.int32()
    },
    'household_demographics': {
        "hd_demo_sk": pa.int32(),
        "hd_income_band_sk": pa.int32(),
        "hd_buy_potential": pa.string(),
        "hd_dep_count": pa.int32(),
        "hd_vehicle_count": pa.int32(),
        "rowid(household_demographics)": pa.int32()
    },
    'income_band': {
        "ib_income_band_sk": pa.int32(),
        "ib_lower_bound": pa.int32(),
        "ib_upper_bound": pa.int32(),
        "rowid(income_band)": pa.int32()
    },
    'item': {
        "i_item_sk": pa.int32(),
        "i_item_id": pa.string(),
        "i_rec_start_date": pa.date32(),
        "i_rec_end_date": pa.date32(),
        "i_item_desc": pa.string(),
        "i_current_price": pa.float64(),
        "i_wholesale_cost": pa.float64(),
        "i_brand_id": pa.int32(),
        "i_brand": pa.string(),
        "i_class_id": pa.int32(),
        "i_class": pa.string(),
        "i_category_id": pa.int32(),
        "i_category": pa.string(),
        "i_manufact_id": pa.int32(),
        "i_manufact": pa.string(),
        "i_size": pa.string(),
        "i_formulation": pa.string(),
        "i_color": pa.string(),
        "i_units": pa.string(),
        "i_container": pa.string(),
        "i_manager_id": pa.int32(),
        "i_product_name": pa.string(),
        "rowid(item)": pa.int32()
    },
    'promotion': {
        "p_promo_sk": pa.int32(),
        "p_promo_id": pa.string(),
        "p_start_date_sk": pa.int32(),
        "p_end_date_sk": pa.int32(),
        "p_item_sk": pa.int32(),
        "p_cost": pa.float64(),
        "p_response_target": pa.int32(),
        "p_promo_name": pa.string(),
        "p_channel_dmail": pa.string(),
        "p_channel_email": pa.string(),
        "p_channel_catalog": pa.string(),
        "p_channel_tv": pa.string(),
        "p_channel_radio": pa.string(),
        "p_channel_press": pa.string(),
        "p_channel_event": pa.string(),
        "p_channel_demo": pa.string(),
        "p_channel_details": pa.string(),
        "p_purpose": pa.string(),
        "p_discount_active": pa.string(),
        "rowid(promotion)": pa.int32()
    },
    'reason': {
        "r_reason_sk": pa.int32(),
        "r_reason_id": pa.string(),
        "r_reason_desc": pa.string(),
        "rowid(reason)": pa.int32()
    },
    'ship_mode': {
        "sm_ship_mode_sk": pa.int32(),
        "sm_ship_mode_id": pa.string(),
        "sm_type": pa.string(),
        "sm_code": pa.string(),
        "sm_carrier": pa.string(),
        "sm_contract": pa.string(),
        "rowid(ship_mode)": pa.int32()
    },
    'store': {
        "s_store_sk": pa.int32(),
        "s_store_id": pa.string(),
        "s_rec_start_date": pa.date32(),
        "s_rec_end_date": pa.date32(),
        "s_closed_date_sk": pa.int32(),
        "s_store_name": pa.string(),
        "s_number_employees": pa.int32(),
        "s_floor_space": pa.int32(),
        "s_hours": pa.string(),
        "s_manager": pa.string(),
        "s_market_id": pa.int32(),
        "s_geography_class": pa.string(),
        "s_market_desc": pa.string(),
        "s_market_manager": pa.string(),
        "s_division_id": pa.int32(),
        "s_division_name": pa.string(),
        "s_company_id": pa.int32(),
        "s_company_name": pa.string(),
        "s_street_number": pa.string(),
        "s_street_name": pa.string(),
        "s_street_type": pa.string(),
        "s_suite_number": pa.string(),
        "s_city": pa.string(),
        "s_county": pa.string(),
        "s_state": pa.string(),
        "s_zip": pa.string(),
        "s_country": pa.string(),
        "s_gmt_offset": pa.float64(),
        "s_tax_precentage": pa.float64(),
        "rowid(store)": pa.int32()
    },
    'time_dim': {
        "t_time_sk": pa.int32(),
        "t_time_id": pa.string(),
        "t_time": pa.int32(),
        "t_hour": pa.int32(),
        "t_minute": pa.int32(),
        "t_second": pa.int32(),
        "t_am_pm": pa.string(),
        "t_shift": pa.string(),
        "t_sub_shift": pa.string(),
        "t_meal_time": pa.string(),
        "rowid(time_dim)": pa.int32()
    },
    'warehouse': {
        "w_warehouse_sk": pa.int32(),
        "w_warehouse_id": pa.string(),
        "w_warehouse_name": pa.string(),
        "w_warehouse_sq_ft": pa.int32(),
        "w_street_number": pa.string(),
        "w_street_name": pa.string(),
        "w_street_type": pa.string(),
        "w_suite_number": pa.string(),
        "w_city": pa.string(),
        "w_county": pa.string(),
        "w_state": pa.string(),
        "w_zip": pa.string(),
        "w_country": pa.string(),
        "w_gmt_offset": pa.float64(),
        "rowid(warehouse)": pa.int32()
    },
    'web_page': {
        "wp_web_page_sk": pa.int32(),
        "wp_web_page_id": pa.string(),
        "wp_rec_start_date": pa.date32(),
        "wp_rec_end_date": pa.date32(),
        "wp_creation_date_sk": pa.int32(),
        "wp_access_date_sk": pa.int32(),
        "wp_autogen_flag": pa.string(),
        "wp_customer_sk": pa.int32(),
        "wp_url": pa.string(),
        "wp_type": pa.string(),
        "wp_char_count": pa.int32(),
        "wp_link_count": pa.int32(),
        "wp_image_count": pa.int32(),
        "wp_max_ad_count": pa.int32(),
        "rowid(web_page)": pa.int32()
    },
    'web_site': {
        "web_site_sk": pa.int32(),
        "web_site_id": pa.string(),
        "web_rec_start_date": pa.date32(),
        "web_rec_end_date": pa.date32(),
        "web_name": pa.string(),
        "web_open_date_sk": pa.int32(),
        "web_close_date_sk": pa.int32(),
        "web_class": pa.string(),
        "web_manager": pa.string(),
        "web_mkt_id": pa.int32(),
        "web_mkt_class": pa.string(),
        "web_mkt_desc": pa.string(),
        "web_market_manager": pa.string(),
        "web_company_id": pa.int32(),
        "web_company_name": pa.string(),
        "web_street_number": pa.string(),
        "web_street_name": pa.string(),
        "web_street_type": pa.string(),
        "web_suite_number": pa.string(),
        "web_city": pa.string(),
        "web_county": pa.string(),
        "web_state": pa.string(),
        "web_zip": pa.string(),
        "web_country": pa.string(),
        "web_gmt_offset": pa.float64(),
        "web_tax_percentage": pa.float64(),
        "rowid(web_site)": pa.int32()
    },

    # ================== 事实表 (7张) ==================
    'catalog_sales': {
        "cs_sold_date_sk": pa.int32(),
        "cs_sold_time_sk": pa.int32(),
        "cs_ship_date_sk": pa.int32(),
        "cs_bill_customer_sk": pa.int32(),
        "cs_bill_cdemo_sk": pa.int32(),
        "cs_bill_hdemo_sk": pa.int32(),
        "cs_bill_addr_sk": pa.int32(),
        "cs_ship_customer_sk": pa.int32(),
        "cs_ship_cdemo_sk": pa.int32(),
        "cs_ship_hdemo_sk": pa.int32(),
        "cs_ship_addr_sk": pa.int32(),
        "cs_call_center_sk": pa.int32(),
        "cs_catalog_page_sk": pa.int32(),
        "cs_ship_mode_sk": pa.int32(),
        "cs_warehouse_sk": pa.int32(),
        "cs_item_sk": pa.int32(),
        "cs_promo_sk": pa.int32(),
        "cs_order_number": pa.int64(),
        "cs_quantity": pa.int32(),
        "cs_wholesale_cost": pa.float64(),
        "cs_list_price": pa.float64(),
        "cs_sales_price": pa.float64(),
        "cs_ext_discount_amt": pa.float64(),
        "cs_ext_sales_price": pa.float64(),
        "cs_ext_wholesale_cost": pa.float64(),
        "cs_ext_list_price": pa.float64(),
        "cs_ext_tax": pa.float64(),
        "cs_coupon_amt": pa.float64(),
        "cs_ext_ship_cost": pa.float64(),
        "cs_net_paid": pa.float64(),
        "cs_net_paid_inc_tax": pa.float64(),
        "cs_net_paid_inc_ship": pa.float64(),
        "cs_net_paid_inc_ship_tax": pa.float64(),
        "cs_net_profit": pa.float64(),
        "rowid(catalog_sales)": pa.int32()
    },
    'catalog_returns': {
        "cr_returned_date_sk": pa.int32(),
        "cr_returned_time_sk": pa.int32(),
        "cr_item_sk": pa.int32(),
        "cr_refunded_customer_sk": pa.int32(),
        "cr_refunded_cdemo_sk": pa.int32(),
        "cr_refunded_hdemo_sk": pa.int32(),
        "cr_refunded_addr_sk": pa.int32(),
        "cr_returning_customer_sk": pa.int32(),
        "cr_returning_cdemo_sk": pa.int32(),
        "cr_returning_hdemo_sk": pa.int32(),
        "cr_returning_addr_sk": pa.int32(),
        "cr_call_center_sk": pa.int32(),
        "cr_catalog_page_sk": pa.int32(),
        "cr_ship_mode_sk": pa.int32(),
        "cr_warehouse_sk": pa.int32(),
        "cr_reason_sk": pa.int32(),
        "cr_order_number": pa.int64(),
        "cr_return_quantity": pa.int32(),
        "cr_return_amount": pa.float64(),
        "cr_return_tax": pa.float64(),
        "cr_return_amt_inc_tax": pa.float64(),
        "cr_fee": pa.float64(),
        "cr_return_ship_cost": pa.float64(),
        "cr_refunded_cash": pa.float64(),
        "cr_reversed_charge": pa.float64(),
        "cr_store_credit": pa.float64(),
        "cr_net_loss": pa.float64(),
        "rowid(catalog_returns)": pa.int32()
    },
    'inventory': {
        "inv_date_sk": pa.int32(),
        "inv_item_sk": pa.int32(),
        "inv_warehouse_sk": pa.int32(),
        "inv_quantity_on_hand": pa.int32(),
        "rowid(inventory)": pa.int32()
    },
    'store_sales': {
        "ss_sold_date_sk": pa.int32(),
        "ss_sold_time_sk": pa.int32(),
        "ss_item_sk": pa.int32(),
        "ss_customer_sk": pa.int32(),
        "ss_cdemo_sk": pa.int32(),
        "ss_hdemo_sk": pa.int32(),
        "ss_addr_sk": pa.int32(),
        "ss_store_sk": pa.int32(),
        "ss_promo_sk": pa.int32(),
        "ss_ticket_number": pa.int64(),
        "ss_quantity": pa.int32(),
        "ss_wholesale_cost": pa.float64(),
        "ss_list_price": pa.float64(),
        "ss_sales_price": pa.float64(),
        "ss_ext_discount_amt": pa.float64(),
        "ss_ext_sales_price": pa.float64(),
        "ss_ext_wholesale_cost": pa.float64(),
        "ss_ext_list_price": pa.float64(),
        "ss_ext_tax": pa.float64(),
        "ss_coupon_amt": pa.float64(),
        "ss_net_paid": pa.float64(),
        "ss_net_paid_inc_tax": pa.float64(),
        "ss_net_profit": pa.float64(),
        "rowid(store_sales)": pa.int32()
    },
    'store_returns': {
        "sr_returned_date_sk": pa.int32(),
        "sr_return_time_sk": pa.int32(),
        "sr_item_sk": pa.int32(),
        "sr_customer_sk": pa.int32(),
        "sr_cdemo_sk": pa.int32(),
        "sr_hdemo_sk": pa.int32(),
        "sr_addr_sk": pa.int32(),
        "sr_store_sk": pa.int32(),
        "sr_reason_sk": pa.int32(),
        "sr_ticket_number": pa.int64(),
        "sr_return_quantity": pa.int32(),
        "sr_return_amt": pa.float64(),
        "sr_return_tax": pa.float64(),
        "sr_return_amt_inc_tax": pa.float64(),
        "sr_fee": pa.float64(),
        "sr_return_ship_cost": pa.float64(),
        "sr_refunded_cash": pa.float64(),
        "sr_reversed_charge": pa.float64(),
        "sr_store_credit": pa.float64(),
        "sr_net_loss": pa.float64(),
        "rowid(store_returns)": pa.int32()
    },
    'web_sales': {
        "ws_sold_date_sk": pa.int32(),
        "ws_sold_time_sk": pa.int32(),
        "ws_ship_date_sk": pa.int32(),
        "ws_item_sk": pa.int32(),
        "ws_bill_customer_sk": pa.int32(),
        "ws_bill_cdemo_sk": pa.int32(),
        "ws_bill_hdemo_sk": pa.int32(),
        "ws_bill_addr_sk": pa.int32(),
        "ws_ship_customer_sk": pa.int32(),
        "ws_ship_cdemo_sk": pa.int32(),
        "ws_ship_hdemo_sk": pa.int32(),
        "ws_ship_addr_sk": pa.int32(),
        "ws_web_page_sk": pa.int32(),
        "ws_web_site_sk": pa.int32(),
        "ws_ship_mode_sk": pa.int32(),
        "ws_warehouse_sk": pa.int32(),
        "ws_promo_sk": pa.int32(),
        "ws_order_number": pa.int64(),
        "ws_quantity": pa.int32(),
        "ws_wholesale_cost": pa.float64(),
        "ws_list_price": pa.float64(),
        "ws_sales_price": pa.float64(),
        "ws_ext_discount_amt": pa.float64(),
        "ws_ext_sales_price": pa.float64(),
        "ws_ext_wholesale_cost": pa.float64(),
        "ws_ext_list_price": pa.float64(),
        "ws_ext_tax": pa.float64(),
        "ws_coupon_amt": pa.float64(),
        "ws_ext_ship_cost": pa.float64(),
        "ws_net_paid": pa.float64(),
        "ws_net_paid_inc_tax": pa.float64(),
        "ws_net_paid_inc_ship": pa.float64(),
        "ws_net_paid_inc_ship_tax": pa.float64(),
        "ws_net_profit": pa.float64(),
        "rowid(web_sales)": pa.int32()
    },
    'web_returns': {
        "wr_returned_date_sk": pa.int32(),
        "wr_returned_time_sk": pa.int32(),
        "wr_item_sk": pa.int32(),
        "wr_refunded_customer_sk": pa.int32(),
        "wr_refunded_cdemo_sk": pa.int32(),
        "wr_refunded_hdemo_sk": pa.int32(),
        "wr_refunded_addr_sk": pa.int32(),
        "wr_returning_customer_sk": pa.int32(),
        "wr_returning_cdemo_sk": pa.int32(),
        "wr_returning_hdemo_sk": pa.int32(),
        "wr_returning_addr_sk": pa.int32(),
        "wr_web_page_sk": pa.int32(),
        "wr_reason_sk": pa.int32(),
        "wr_order_number": pa.int64(),
        "wr_return_quantity": pa.int32(),
        "wr_return_amt": pa.float64(),
        "wr_return_tax": pa.float64(),
        "wr_return_amt_inc_tax": pa.float64(),
        "wr_fee": pa.float64(),
        "wr_return_ship_cost": pa.float64(),
        "wr_refunded_cash": pa.float64(),
        "wr_reversed_charge": pa.float64(),
        "wr_account_credit": pa.float64(),
        "wr_net_loss": pa.float64(),
        "rowid(web_returns)": pa.int32()
    }
}

data = {}
    
for table in tables:
    print(f"Creating schema for table {table}")
    schema = pa.schema([pa.field(name, type) for name, type in column_types[table].items()])
    con = duckdb.connect(database="/home/yihao/duckdb/origin/duckdb/release/tpcds_10_uncom.db")
    table_size = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    con.close()
    print(f"Table {table} size: {table_size}")
    data[table] = {}
    col_id = 0
    for field in schema:
        data[table][table+'.'+field.name] = {'col_id':col_id, 'type':LogicalTypeId[logical_types[field.type]].value}
        col_id+=1
    data[table]["rowid({})".format(table)] = {'col_id':col_id, 'type':LogicalTypeId.int32.value}
    data[table]['table_size'] = table_size
    col_id+=1
with open(f"tpcds_schema_10_store.json", "w") as f:
    json.dump(data, f, indent=4)

