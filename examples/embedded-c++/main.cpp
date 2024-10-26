#include "duckdb.hpp"

#include <iostream>
#include <sys/time.h>

using namespace duckdb;

double getNow() {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}

int main(int argc, char *argv[]) {
	std::string thread = argv[1];
	std::string date;
	std::string date2;
	bool print_result = false;
	if (argc < 5) {
		date = "1995-01-01";
		date2 = "1995-03-17";
	} else {
		date = argv[2];
		date2 = argv[3];
		print_result = atoi(argv[4]);
	}
	DuckDB db("/home/yihao/duckdb/origin/duckdb/examples/embedded-c++/release/tpch_uncom.db");

	Connection con(db);

	con.Query("SET threads TO " + thread + ";");
	// con.Query("create table partsupp as from '/home/yihao/tpch-dbgen/partsupp.parquet';");
	// con.Query("create table supplier as from '/home/yihao/tpch-dbgen/supplier.parquet';");
	// con.Query("PRAGMA force_compression='uncompressed';");
	// con.Query("create table orders as from '/home/yihao/tpch-dbgen/orders.parquet';");
	// con.Query("create table lineitem as from '/home/yihao/tpch-dbgen/lineitem.parquet';");
	// con.Query("create table part as from '/home/yihao/tpch-dbgen/part.parquet';");
	// con.Query("SET disabled_optimizers = 'join_order,build_side_probe_side';");

	// std::string query =
	//     "select p_name, ps_supplycost, l_quantity from lineitem,partsupp,part where l_partkey = "
	//     "p_partkey and l_partkey =  ps_partkey and l_shipdate > date '1995-03-17' and ps_supplycost<500;";
	std::string query = "select l_orderkey, o_custkey, o_shippriority, o_comment, orders.rowid  from "
	                    "orders,lineitem where l_orderkey "
	                    "= o_orderkey and o_orderdate < date '" +
	                    date + "' and l_shipdate > date '" + date2 + "';";
	// std::cout << query << std::endl;
	// int a;
	// std::cin >> a;
	double start = getNow();
	auto result = con.Query(query);
	// auto result = con.Query("select ps_partkey, s_name, s_address, s_nationkey, s_comment from partsupp,supplier "
	//                         "where ps_suppkey = s_suppkey and ps_availqty > 9000;");
	double end = getNow();

	if (print_result) {
		result->Print();
	}
	// result->PrintRowNumber();
	std::cout << date << " " << date2 << " " << end - start << std::endl;
}