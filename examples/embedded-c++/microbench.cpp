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
	bool print_result = false;
	std::ofstream file("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/config_num");

	if (file.is_open()) {
		file << thread;
		file.close();
	}

	int mode = atoi(argv[2]);
	int64_t probe_size = atoi(argv[3]);
	int64_t build_size = atoi(argv[4]);
	int selectivity = atoi(argv[5]);
	int unique_key_ratio = atoi(argv[6]);
	int heavy_hitter_ratio = atoi(argv[7]);
	int payload_column_num = atoi(argv[8]);
	int payload_tuple_size = atoi(argv[9]);
	print_result = atoi(argv[10]);

	std::string file_path = "/home/yihao/data_gen/probe" + std::to_string(probe_size) + "_build" +
	                        std::to_string(build_size) + "_sel" + std::to_string(selectivity) + "_skew" +
	                        std::to_string(heavy_hitter_ratio) + "_" + std::to_string(unique_key_ratio) + "_payload" +
	                        std::to_string(payload_column_num) + "_" + std::to_string(payload_tuple_size);
	std::ofstream config_file("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/config");
	std::ifstream file_read(file_path + "/config");
	std::string line;
	config_file << 0 << std::endl;
	if (file_read.is_open()) {
		while (getline(file_read, line)) {
			config_file << line << std::endl;
		}
		file_read.close();
	}
	std::string build_file_name = "build_" + std::to_string(build_size) + "_" + std::to_string(selectivity) + "_" +
	                              std::to_string(int(heavy_hitter_ratio)) + "_" + std::to_string(payload_column_num) +
	                              "_" + std::to_string(payload_tuple_size);
	std::string probe_file_name = "probe_" + std::to_string(probe_size) + "_" + std::to_string(selectivity) + "_" +
	                              std::to_string(unique_key_ratio) + "_" + std::to_string(heavy_hitter_ratio) + "_" +
	                              std::to_string(payload_column_num) + "_" + std::to_string(payload_tuple_size);
	std::cout << build_file_name << " " << probe_file_name << std::endl;
	if (mode == 0) // load from parquet
	{
		DuckDB db(nullptr);
		Connection con(db);
		con.Query("SET threads TO " + thread + ";");
		con.Query("create table build as from '" + file_path + "/build.parquet';");
		con.Query("create table probe as from '" + file_path + "/probe.parquet';");
		con.Query("SET disabled_optimizers = 'join_order,build_side_probe_side';");
		std::string query = "select build_side_rowid from probe,build where build_key = probe_key;";
		double start = getNow();
		auto result = con.Query(query);
		double end = getNow();
		if (print_result) {
			result->Print();
		}
		result->PrintRowNumber();
		std::cout << mode << " " << thread << " " << probe_size << " " << build_size << " " << selectivity << " "
		          << payload_column_num << " " << payload_tuple_size << " " << heavy_hitter_ratio << " " << end - start
		          << std::endl;
	} else if (mode == 1) // load from uncompressed duckdb storage
	{
		DuckDB db("/home/yihao/duckdb/origin/duckdb/examples/embedded-c++/release/tpch_uncom.db");
		Connection con(db);
		con.Query("SET threads TO " + thread + ";");
		con.Query("SET disabled_optimizers = 'join_order,build_side_probe_side';");
		// std::cout << probe_file_name << " " << build_file_name << std::endl;
		std::string query = "select build_side_rowid from " + probe_file_name + ", " + build_file_name +
		                    " where build_key = probe_key;";
		double start = getNow();
		auto result = con.Query(query);
		double end = getNow();
		if (print_result) {
			result->Print();
		}
		result->PrintRowNumber();
		std::cout << mode << " " << thread << " " << probe_size << " " << build_size << " " << selectivity << " "
		          << payload_column_num << " " << payload_tuple_size << " " << heavy_hitter_ratio << " " << end - start
		          << std::endl;
	} else // load from compressed duckdb storage
	{
		DuckDB db("/home/yihao/duckdb/origin/duckdb/examples/embedded-c++/release/tpch.db");
		Connection con(db);
		con.Query("SET threads TO " + thread + ";");
		con.Query("SET disabled_optimizers = 'join_order,build_side_probe_side';");
		std::string query = "select build_side_rowid from " + probe_file_name + ", " + build_file_name +
		                    " where build_key = probe_key;";
		double start = getNow();
		auto result = con.Query(query);
		double end = getNow();
		if (print_result) {
			result->Print();
		}
		result->PrintRowNumber();
		std::cout << mode << " " << thread << " " << probe_size << " " << build_size << " " << selectivity << " "
		          << payload_column_num << " " << payload_tuple_size << " " << heavy_hitter_ratio << " " << end - start
		          << std::endl;
	}
}