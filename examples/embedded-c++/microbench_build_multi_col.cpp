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
                  int build_size, int queue_thr = 100, int string_length = 0) {
	std::ofstream file("/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/config/op_mat_3", std::ios::out);
	if (file.is_open()) {
		for (auto &attr : payload) {
			file << build_name + "." + attr << std::endl;
		}
	}

	std::ofstream file_dis("/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/config/op_dis_3",
	                       std::ios::out);
	if (file_dis.is_open()) {
		file_dis << "build_side_rowid" << std::endl;
	}

	std::ofstream file_pipeline2("/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/config/pipeline2",
	                             std::ios::out);
	if (file_pipeline2.is_open()) {
		file_pipeline2 << "0 1 0" << std::endl;
		file_pipeline2 << "0" << std::endl;
	}

	std::ofstream file_pipeline1("/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/config/pipeline1",
	                             std::ios::out);
	if (file_pipeline1.is_open()) {
		file_pipeline1 << mat_strat << " " << 0 << " " << queue_thr << std::endl;
		file_pipeline1 << 1 << std::endl;
		file_pipeline1 << 2 << " " << 0 << " " << payload.size() << " " << build_name + ".build_side_rowid "
		               << build_size << std::endl;
		for (int i = 0; i < payload.size(); i++) {
			file_pipeline1 << build_name + "." + payload[i] << " " << 2 + i << " " << type[i];
			if (type[i] == 25) {
				file_pipeline1 << " " << string_length;
			}
			file_pipeline1 << std::endl;
		}
	}

	std::ofstream file_build("/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/config/table" + build_name,
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
	double selectivity = atof(argv[5]);
	int payload_size = atoi(argv[6]);
	std::string probe_distribution = argv[7];
	double build_side_hit_ratio = atof(argv[8]);
	std::string build_key_pattern = argv[9];
	std::string probe_key_pattern = argv[10];
	std::string key_set_file = "";
	std::string payload_file = "";
	int mat_stat = atoi(argv[11]);
	int queue_thr = atoi(argv[12]);
	int payload_column_num = atoi(argv[13]);
	if (argc > 14) {
		print_result = atoi(argv[14]);
	}
	if (argc > 15) {
		key_set_file = argv[15];
	}
	if (argc > 16) {
		payload_file = argv[16];
	}

	std::string command = "rm /home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/config/*";
	int cmd_result = system(command.c_str());

	std::string file_path = "/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/microbench/";
	std::string build_file_name = "build_" + std::to_string(build_size) + "_" +
	                              std::to_string(int(build_side_hit_ratio * 100)) + "_" + build_key_pattern + "_" +
	                              std::to_string(payload_size) + "_" + std::to_string(payload_column_num);
	std::string probe_file_name = "probe_" + std::to_string(build_size) + "_" + std::to_string(probe_size) + "_" +
	                              std::to_string(int(selectivity * 10000)) + "_" + probe_key_pattern + "_" +
	                              probe_distribution + "_" + std::to_string(int(build_side_hit_ratio * 100));
	std::cout << build_file_name << " " << probe_file_name << std::endl;
	std::vector<int> types;
	int string_length = 0;
	for (int i = 0; i < payload_column_num; i++) {

		if (payload_size == 4) {
			types.push_back(13);
		} else if (payload_size == 8) {
			types.push_back(14);
		} else {
			types.push_back(25);
			if (payload_size % 10 == 0) {
				string_length = payload_size;
			}
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
			write_config(payload, types, mat_stat, build_file_name, build_size, queue_thr, string_length);
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
		          << payload_size << " " << probe_distribution << " " << build_side_hit_ratio << " "
		          << build_key_pattern << " " << probe_key_pattern << " " << end - start << std::endl;
		std::ofstream out(
		    "/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/benchmark_multicol_dropcache.txt",
		    std::ios::app);
		out << mode << " " << thread << " " << probe_size << " " << build_size << " " << selectivity << " "
		    << payload_size << " " << probe_distribution << " " << build_side_hit_ratio << " " << build_key_pattern
		    << " " << probe_key_pattern << " " << mat_stat << " " << queue_thr << " " << payload_column_num << " "
		    << end - start << std::endl;
	} else if (mode == 1) // load from uncompressed duckdb storage
	{
		DuckDB db("/home/yihao/duckdb/origin/duckdb/examples/embedded-c++/release/micro_multi_uncom.db");
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
			write_config(payload, types, mat_stat, build_file_name, build_size, queue_thr, string_length);
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
		          << payload_size << " " << probe_distribution << " " << build_side_hit_ratio << " "
		          << build_key_pattern << " " << probe_key_pattern << " " << end - start << std::endl;
		// std::ofstream out("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/payload_build_time.txt",
		//                   std::ios::app);
		// out << mat_stat << " " << queue_thr << " " << end - start << std::endl;
		std::ofstream out(
		    "/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/benchmark_multicol_dropcache.txt",
		    std::ios::app);
		out << mode << " " << thread << " " << probe_size << " " << build_size << " " << selectivity << " "
		    << payload_size << " " << probe_distribution << " " << build_side_hit_ratio << " " << build_key_pattern
		    << " " << probe_key_pattern << " " << mat_stat << " " << queue_thr << " " << payload_column_num << " "
		    << end - start << std::endl;
	} else // load from compressed duckdb storage
	{
		DuckDB db("/home/yihao/duckdb/origin/duckdb/examples/embedded-c++/release/micro_multi.db");
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
			write_config(payload, types, mat_stat, build_file_name, build_size, queue_thr, string_length);
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
		          << payload_size << " " << probe_distribution << " " << build_side_hit_ratio << " "
		          << build_key_pattern << " " << probe_key_pattern << " " << end - start << std::endl;
		std::ofstream out(
		    "/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/benchmark_multicol_dropcache.txt",
		    std::ios::app);
		out << mode << " " << thread << " " << probe_size << " " << build_size << " " << selectivity << " "
		    << payload_size << " " << probe_distribution << " " << build_side_hit_ratio << " " << build_key_pattern
		    << " " << probe_key_pattern << " " << mat_stat << " " << queue_thr << " " << payload_column_num << " "
		    << end - start << std::endl;
	}
}