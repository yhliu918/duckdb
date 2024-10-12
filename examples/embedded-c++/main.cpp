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
	DuckDB db(nullptr);

	Connection con(db);

	con.Query("SET threads TO " + thread + ";");
	con.Query("create table partsupp as from '/root/tpch-dbgen/partsupp.parquet';");
	con.Query("create table supplier as from '/root/tpch-dbgen/supplier_rid.parquet';");

	double start = getNow();
	auto result = con.Query("select ps_partkey, supplier.rowid from partsupp,supplier "
	                        "where ps_suppkey = s_suppkey and s_nationkey = 1 and ps_availqty > 100;");
	double end = getNow();

	result->Print();
	std::cout << "Execution time: " << end - start << "ms" << std::endl;
}