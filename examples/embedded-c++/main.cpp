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
	std::ofstream file("/root/duckdb_ht/duckdb/examples/embedded-c++/config_num");

	if (file.is_open()) {
		file << thread;
		file.close();
	}
	DuckDB db(nullptr);

	Connection con(db);

	con.Query("SET threads TO " + thread + ";");
	// con.Query("create table partsupp as from '/root/tpch-dbgen/partsupp.parquet';");
	// con.Query("create table supplier as from '/root/tpch-dbgen/supplier_rid.parquet';");
	con.Query("create table orders as from '/root/tpch-dbgen/orders_rid.parquet';");
	con.Query("create table lineitem as from '/root/tpch-dbgen/lineitem.parquet';");

	double start = getNow();
	// auto result = con.Query("select ps_partkey, supplier.rowid from partsupp,supplier "
	//                         "where ps_suppkey = s_suppkey and ps_availqty > 9000;");
	auto result = con.Query("select l_orderkey, orders.rowid from "
	                        "orders,lineitem where l_orderkey "
	                        "= o_orderkey and o_orderdate < date '1995-01-01' and l_shipdate > date '1995-03-17';");
	double end = getNow();

	result->Print();
	std::cout << "Execution time: " << end - start << "ms" << std::endl;
}