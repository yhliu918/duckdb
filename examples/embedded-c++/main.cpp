#include "duckdb.hpp"

#include <fstream>
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
	std::ofstream file("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/config_num");

	if (file.is_open()) {
		file << thread;
		file.close();
	}
	DuckDB db("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/debug/tpch_uncom.db");
	// DuckDB db(nullptr);

	Connection con(db);

	con.Query("SET threads TO " + thread + ";");
	// con.Query("SET streaming_buffer_size = '10GB';");
	// con.Query("SELECT * FROM duckdb_settings() WHERE name = 'streaming_buffer_size';")->Print();

	// con.Query("create table partsupp as from '/home/yihao/tpch-dbgen/partsupp.parquet';");
	// con.Query("create table supplier as from '/home/yihao/tpch-dbgen/supplier.parquet';");
	// con.Query("PRAGMA force_compression='uncompressed';");
	// con.Query("create table orders as from '/home/yihao/tpch-dbgen/orders.parquet';");
	// con.Query("PRAGMA force_compression='uncompressed';");
	// con.Query("create table lineitem as from '/home/yihao/tpch-dbgen/lineitem.parquet';");
	// con.Query("PRAGMA storage_info('tpch.db');");
	std::string query = "select l_orderkey, orders.rowid from "
	                    "orders,lineitem where l_orderkey "
	                    "= o_orderkey and o_orderdate < date '" +
	                    date + "' and l_shipdate > date '" + date2 + "';";
	// std::cout << query << std::endl;
	// int a;
	// std::cin >> a;
	double start = getNow();
	// auto result = con.Query("select ps_partkey, supplier.rowid from partsupp,supplier "
	//                         "where ps_suppkey = s_suppkey and ps_availqty > 9000;");
	// std::string query = "select l_orderkey, o_custkey, o_shippriority, o_comment, orders.rowid  from "
	//                     "orders,lineitem where l_orderkey "
	//                     "= o_orderkey and o_orderdate < date '" +
	//                     date + "' and l_shipdate > date '" + date2 + "';";
	auto result = con.Query(query);
	double end = getNow();
	if (print_result) {
		result->Print();
	}
	result->PrintRowNumber();
	std::cout << date << " " << date2 << " " << end - start << std::endl;
	// std::cout << "Execution time: " << end - start << "ms" << std::endl;
}