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
void write_config(std::vector<string> payload, std::vector<int> type, int mat_strat, std::string build_name,
                  int queue_thr = 100) {
	std::ofstream file("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/config/op_mat_3", std::ios::out);
	if (file.is_open()) {
		for (auto &attr : payload) {
			file << build_name + "." + attr << std::endl;
		}
	}

	std::ofstream file_dis("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/config/op_dis_3", std::ios::out);
	if (file_dis.is_open()) {
		file_dis << "build_side_rowid" << std::endl;
	}

	std::ofstream file_pipeline2("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/config/pipeline2",
	                             std::ios::out);
	if (file_pipeline2.is_open()) {
		file_pipeline2 << "0 1 0" << std::endl;
		file_pipeline2 << "0" << std::endl;
	}

	std::ofstream file_pipeline1("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/config/pipeline1",
	                             std::ios::out);
	if (file_pipeline1.is_open()) {
		file_pipeline1 << mat_strat << " " << 0 << " " << queue_thr << std::endl;
		file_pipeline1 << 1 << std::endl;
		file_pipeline1 << 2 << " " << 0 << " " << payload.size() << " " << build_name + ".build_side_rowid"
		               << std::endl;
		for (int i = 0; i < payload.size(); i++) {
			file_pipeline1 << build_name + "." + payload[i] << " " << 2 + i << " " << type[i];
			if (type[i] == 25) {
				file_pipeline1 << " 0";
			}
			file_pipeline1 << std::endl;
		}
	}

	std::ofstream file_build("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/config/table" + build_name,
	                         std::ios::out);
	if (file_build.is_open()) {
		for (int i = 0; i < payload.size(); i++) {
			file_build << 2 + i << std::endl;
		}
	}
}

int main(int argc, char *argv[]) {
	std::string thread = argv[1];
	bool print_result = false;

	int mode = atoi(argv[2]);
	int64_t probe_size = atoi(argv[3]);
	int64_t build_size = atoi(argv[4]);
	int selectivity = atoi(argv[5]);
	int unique_key_ratio = atoi(argv[6]);
	int heavy_hitter_ratio = atoi(argv[7]);
	int payload_column_num = atoi(argv[8]);
	int payload_tuple_size = atoi(argv[9]);
	int mat_stat = atoi(argv[10]);
	print_result = atoi(argv[11]);
	int queue_thr = atoi(argv[12]);

	std::string command = "rm /home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/config/*";
	int cmd_result = system(command.c_str());

	std::string file_path = "/home/yihao/data_gen/probe" + std::to_string(probe_size) + "_build" +
	                        std::to_string(build_size) + "_sel" + std::to_string(selectivity) + "_skew" +
	                        std::to_string(heavy_hitter_ratio) + "_" + std::to_string(unique_key_ratio) + "_payload" +
	                        std::to_string(payload_column_num) + "_" + std::to_string(payload_tuple_size);
	std::string build_file_name = "build_" + std::to_string(build_size) + "_" + std::to_string(selectivity) + "_" +
	                              std::to_string(int(heavy_hitter_ratio)) + "_" + std::to_string(payload_column_num) +
	                              "_" + std::to_string(payload_tuple_size);
	std::string probe_file_name = "probe_" + std::to_string(probe_size) + "_" + std::to_string(selectivity) + "_" +
	                              std::to_string(unique_key_ratio) + "_" + std::to_string(heavy_hitter_ratio) + "_" +
	                              std::to_string(payload_column_num) + "_" + std::to_string(payload_tuple_size);
	std::cout << build_file_name << " " << probe_file_name << std::endl;
	std::ifstream file_types(file_path + "/types", std::ios::in);
	std::vector<int> types;
	if (file_types.is_open()) {
		int type;
		while (file_types >> type) {
			types.push_back(type);
		}
	}

	if (mode == 0) // load from parquet
	{
		DuckDB db(nullptr);
		Connection con(db);
		con.Query("SET threads TO " + thread + ";");
		con.Query("create table build as from '" + file_path + "/build.parquet';");
		con.Query("create table probe as from '" + file_path + "/probe.parquet';");
		con.Query("SET disabled_optimizers = 'join_order,build_side_probe_side,COMPRESSED_MATERIALIZATION';");
		std::vector<std::string> payload;
		std::string project_keys = "";
		for (int i = 0; i < payload_column_num; i++) {
			project_keys += "payload_" + std::to_string(i) + ",";
			payload.push_back("payload_" + std::to_string(i));
		}
		project_keys += "build_side_rowid";
		std::string query = "select " + project_keys + " from probe,build where build_key = probe_key;";
		if (mat_stat) {
			write_config(payload, types, mat_stat, build_file_name, queue_thr);
		}
		double start = getNow();
		std::cout << query << std::endl;
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
		con.Query("SET disabled_optimizers = 'join_order,build_side_probe_side,COMPRESSED_MATERIALIZATION';");
		// std::cout << probe_file_name << " " << build_file_name << std::endl;
		std::vector<std::string> payload;
		std::string project_keys = "";
		for (int i = 0; i < payload_column_num; i++) {
			project_keys += "payload_" + std::to_string(i) + ",";
			payload.push_back("payload_" + std::to_string(i));
		}
		project_keys += "build_side_rowid";

		std::string query = "select " + project_keys + " from " + probe_file_name + ", " + build_file_name +
		                    " where build_key = probe_key;";
		std::cout << query << std::endl;
		if (mat_stat) {
			write_config(payload, types, mat_stat, build_file_name, queue_thr);
		}
		// int a;
		// std::cout << "input a to continue" << std::endl;
		// std::cin >> a;

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