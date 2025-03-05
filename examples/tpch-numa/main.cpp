#include "duckdb.hpp"

#include "ittnotify.h"

#include <fstream>
#include <iostream>
#include <sys/time.h>

using namespace duckdb;

int print_tag = 0;
int numa_tag = 0;
int parallel_build_tag = 0;
int split_probe_tag = 0;
int debug_tag = 0;
double query_start;
double prepare_payloads_end;
double init_pointer_table_end;
double build_end;
double first_probe_end;
double combine_end;
double probe_end;
double total_time;

int probe_type = 0;
std::atomic<double> first_probe_time;
std::atomic<double> second_probe_time;
std::atomic<double> first_probe_calc_time;
std::atomic<double> second_probe_calc_time;
std::atomic<double> pipeline_breaker_time;

int split_probe_rest;

std::atomic<int> current_build_id;

double GetNow() {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}

double RunQuery(Connection &con, std::string query) {
	auto query_start = GetNow();
	split_probe_rest = split_probe_tag;
	con.Query(query);
	auto query_end = GetNow();
	return query_end - query_start;
}

std::string Query(int);

int main(int argc, char *argv[]) {
	std::string thread = argv[1];
	std::string sf = argv[2];
	int query_number = atoi(argv[3]);
	numa_tag = atoi(argv[4]);
	split_probe_tag = atoi(argv[5]);
	parallel_build_tag = atoi(argv[6]);
	debug_tag = 1;

	std::string db_file = "/home/hangrui/sf" + sf + ".db";
	DuckDB db(db_file);
	Connection con(db);
	con.Query("SET threads TO " + thread + ";");

	for (int i = 0; i <= 5; i++) {
		if (i == 1) {
			__itt_resume();
		}
		auto time_usage = RunQuery(con, Query(query_number));
		if (i == 1) {
			__itt_pause();
		}
		if (i != 0) {
			std::cout << time_usage << "\n";
		}
	}
}

std::string Query(int number) {
	if (number == 9) {
		// use 1 4 1
		return 
			"select "
			"nation, "
			"o_year, "
			"sum(amount) as sum_profit "
			"from "
			"(select "
			"n_name as nation,  "
			"extract(year from o_orderdate) as o_year, "
			"l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount "
			"from "
			"part,supplier,lineitem,partsupp,orders,nation "
			"where "
			"s_suppkey = l_suppkey "
			"and ps_suppkey = l_suppkey "
			"and ps_partkey = l_partkey "
			"and p_partkey = l_partkey "
			"and o_orderkey = l_orderkey "
			"and s_nationkey = n_nationkey "
			"and p_name like '%chocolate%' "
			") as profit "
			"group by "
			"nation, "
			"o_year "
			"order by "
			"nation, "
			"o_year desc;";
	}
	if (number == 3) {
		// use 1 2 1
		return
			"SELECT "
			"L_ORDERKEY, "
			"SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE, "
			"O_ORDERDATE, "
			"O_SHIPPRIORITY "
			"FROM  "
			"CUSTOMER, "
			"ORDERS, "
			"LINEITEM "
			"WHERE "
			"C_MKTSEGMENT = 'BUILDING' "
			"AND C_CUSTKEY = O_CUSTKEY "
			"AND L_ORDERKEY = O_ORDERKEY "
			"AND O_ORDERDATE < DATE '1995-03-15' "
			"AND L_SHIPDATE > DATE '1995-03-15' "
			"GROUP BY "
			"L_ORDERKEY, "
			"O_ORDERDATE, "
			"O_SHIPPRIORITY "
			"ORDER BY "
			"REVENUE DESC, "
			"O_ORDERDATE "
			"limit 10;";
	}
	if (number == 21) {
		return
			"select "
			"s_name, "
			"count(*) as numwait "
			"from "
			"supplier,lineitem l1,orders,nation "
			"where "
			"s_suppkey = l1.l_suppkey "
			"and o_orderkey = l1.l_orderkey "
			"and o_orderstatus = 'F' "
			"and l1.l_receiptdate > l1.l_commitdate "
			"and exists ( "
			"select "
			"* "
			"from "
			"lineitem l2 "
			"where "
			"l2.l_orderkey = l1.l_orderkey "
			"and l2.l_suppkey <> l1.l_suppkey "
			") "
			"and not exists ( "
			"select "
			"* "
			"from "
			"lineitem l3 "
			"where "
			"l3.l_orderkey = l1.l_orderkey "
			"and l3.l_suppkey <> l1.l_suppkey "
			"and l3.l_receiptdate > l3.l_commitdate "
			") "
			"and s_nationkey = n_nationkey "
			"and n_name = 'CHINA' "
			"group by "
			"s_name "
			"order by "
			"numwait desc, "
			"s_name "
			"limit 100;";
	}
	if (number == 22) {
		return
			"select "
			"cntrycode, "
			"count(*) as numcust, "
			"sum(c_acctbal) as totacctbal "
			"from ( "
			"select "
			"substring(c_phone from 1 for 2) as cntrycode, "
			"c_acctbal "
			"from "
			"customer "
			"where "
			"substring(c_phone from 1 for 2) in ('23', '11', '14', '13', '30', '28', '27') "
			"and c_acctbal > ( "
			"select "
			"avg(c_acctbal) "
			"from "
			"customer "
			"where "
			"c_acctbal > 0.00 "
			"and substring(c_phone from 1 for 2) in ('23', '11', '14', '13', '30', '28', '27') "
			") "
			"and not exists ( "
			"select "
			"* "
			"from "
			"orders "
			"where "
			"o_custkey = c_custkey "
			") "
			") as custsale "
			"group by "
			"cntrycode "
			"order by "
			"cntrycode;";

	}
	abort();
}