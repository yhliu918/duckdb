#include "duckdb.hpp"

#include "ittnotify.h"

#include <fstream>
#include <iostream>
#include <sys/time.h>

using namespace duckdb;

int print_tag = 0;
int numa_tag = 1;
int parallel_build_tag = 1;
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

int split_probe_rest = split_probe_tag;

std::atomic<int> current_build_id;

double getNow() {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}

int main(int argc, char *argv[]) {
	std::string thread = argv[1];
	int64_t probe_size = atoi(argv[2]);
	int64_t build_size_0 = atoi(argv[3]);
	int64_t build_size_1 = atoi(argv[4]);

	std::string file_path = "~/parallel-build/gen_data/probe" + std::to_string(probe_size) + "_build" +
	                        std::to_string(build_size_0) + "_" + std::to_string(build_size_1);

	DuckDB db(nullptr);
	Connection con(db);
	con.Query("SET threads TO " + thread + ";");
	con.Query("create table build0 as from '" + file_path + "/build0.parquet';");
	con.Query("create table build1 as from '" + file_path + "/build1.parquet';");
	con.Query("create table probe as from '" + file_path + "/probe.parquet';");
	con.Query("SET disabled_optimizers = 'join_order,build_side_probe_side';");
	std::string query = "select build_side_rowid_0, build_side_rowid_1 from probe,build0,build1 where build_key_0 = probe_key_0 and build_key_1 = probe_key_1;";

	con.Query(query);
	duckdb::Printer::Print("start query");
	std::cout << "start query\n";
	query_start = getNow();
	__itt_resume();
	debug_tag = 1;
	auto result = con.Query(query);
	__itt_pause();
	probe_end = getNow();

	result->Print();

	// std::cout << con.Query("explain " + query)->Fetch()->GetValue(1, 0).GetValue<string>() << std::endl;
	std::cout << prepare_payloads_end - query_start << " " << init_pointer_table_end - prepare_payloads_end << " "
			<< build_end - init_pointer_table_end << " " << probe_end - build_end << " " << probe_end - query_start << std::endl;
	std::cerr << prepare_payloads_end - query_start << " " << init_pointer_table_end - prepare_payloads_end << " "
			<< build_end - init_pointer_table_end << " " << probe_end - build_end << " " << probe_end - query_start << std::endl;
}