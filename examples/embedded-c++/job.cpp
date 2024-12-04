#include "duckdb.hpp"
#include "json.hpp"

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <sys/time.h>

using namespace duckdb;
using json = nlohmann::json;
#define QUEUE_THR 1

bool file_exists(const std::string &path) {
	struct stat buffer;
	return (stat(path.c_str(), &buffer) == 0);
}

double getNow() {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}
json schema;
json plan;

std::unordered_set<std::string> parse_plan(bool dump) {
	if (dump) {
		return std::unordered_set<std::string>();
	}
	std::string directory = "/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/query/pipeline";
	int pipeline_id = 1;
	std::string file_path = directory + std::to_string(pipeline_id) + ".json";
	while (!file_exists(file_path)) {
		pipeline_id++;
		file_path = directory + std::to_string(pipeline_id) + ".json";
	}
	while (true) {
		json j;
		file_path = directory + std::to_string(pipeline_id) + ".json";
		if (!file_exists(file_path)) {
			break;
		}
		std::ifstream file(file_path);
		file >> j;
		file.close();
		plan[std::to_string(pipeline_id)] = std::move(j);
		pipeline_id++;
	}

	std::unordered_set<std::string> all_attributes;
	for (json::iterator it = plan.begin(); it != plan.end(); ++it) {
		if (it.value().contains("must_enable_columns_start")) {
			auto must_enable_columns_start = it.value()["must_enable_columns_start"].get<std::vector<std::string>>();
			for (auto &attr : must_enable_columns_start) {
				all_attributes.insert(attr);
			}
		}
		if (it.value().contains("must_enable_columns_end")) {
			auto must_enable_columns_end = it.value()["must_enable_columns_end"].get<std::vector<std::string>>();
			for (auto &attr : must_enable_columns_end) {
				all_attributes.insert(attr);
			}
		}
	}

	return all_attributes;
}
std::unordered_map<std::string, vector<int>>
find_materialize_position(std::vector<std::string> attribute, std::unordered_map<std::string, int> &from_pipeline,
                          std::unordered_set<std::string> &table_names) {
	std::ifstream file("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/query/job_schema.json");
	file >> schema;
	file.close();
	std::unordered_map<std::string, bool> correct_mat_key;
	for (auto &attr : attribute) {
		correct_mat_key[attr] = false;
	}
	std::unordered_map<std::string, std::string> table_info;
	std::unordered_map<std::string, vector<int>> possible_mat_pos;

	// write table info
	for (json::iterator it = schema.begin(); it != schema.end(); ++it) {
		std::ofstream file("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/config/table" + it.key(),
		                   std::ios::out);
		for (json::iterator it2 = it.value().begin(); it2 != it.value().end(); ++it2) {
			if (std::find(attribute.begin(), attribute.end(), it2.key()) != attribute.end()) {
				file << schema[it.key()][it2.key()]["col_id"] << std::endl;
				table_info[it2.key()] = it.key();
				correct_mat_key[it2.key()] = true;
				table_names.insert(it.key());
			}
		}
	}
	for (auto &attr : attribute) {
		if (!correct_mat_key[attr] && attr != "NULL") {
			std::cout << "Invalid materialize key: " << attr << std::endl;
			exit(0);
		}
	}
	// enumate all the pipelines
	for (auto &[attr, table_name] : table_info) {
		for (json::iterator it = plan.begin(); it != plan.end(); ++it) {
			if (it.value()["table"] == table_name) {
				int pipeline_id = std::stoi(it.key());
				from_pipeline[attr] = pipeline_id;
				std::vector<std::string> must_enable_start_current =
				    it.value().contains("must_enable_columns_start")
				        ? it.value()["must_enable_columns_start"].get<std::vector<std::string>>()
				        : std::vector<std::string>();
				std::vector<std::string> must_enable_end_current =
				    it.value().contains("must_enable_columns_end")
				        ? it.value()["must_enable_columns_end"].get<std::vector<std::string>>()
				        : std::vector<std::string>();
				std::string attr_ = attr;
				// if (attr.find(".") != std::string::npos) {
				// 	attr_ = attr.substr(attr.find(".") + 1);
				// }
				if (std::find(must_enable_start_current.begin(), must_enable_start_current.end(), attr_) !=
				    must_enable_start_current.end()) {
					//! should be materialized at first of this pipeline
					continue;
				}
				if (std::find(must_enable_end_current.begin(), must_enable_end_current.end(), attr_) !=
				    must_enable_end_current.end()) {
					possible_mat_pos[attr].push_back(pipeline_id);
					//! must be materialized inside this pipeline
					continue;
				}
				while (true) {
					if (pipeline_id == 0 ||
					    !file_exists("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/query/pipeline" +
					                 std::to_string(pipeline_id) + ".json")) {
						break;
					}
					int parent_pipeline_id = plan[std::to_string(pipeline_id)]["parent"];
					if (parent_pipeline_id == 0 ||
					    !file_exists("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/query/pipeline" +
					                 std::to_string(parent_pipeline_id) + ".json")) {
						break;
					}
					auto parent = plan[std::to_string(parent_pipeline_id)];
					pipeline_id = parent_pipeline_id;
					std::vector<std::string> must_enable_columns_start =
					    parent.contains("must_enable_columns_start")
					        ? parent["must_enable_columns_start"].get<std::vector<std::string>>()
					        : std::vector<std::string>();
					std::vector<std::string> must_enable_columns_end =
					    parent.contains("must_enable_columns_end")
					        ? parent["must_enable_columns_end"].get<std::vector<std::string>>()
					        : std::vector<std::string>();
					if (std::find(must_enable_columns_end.begin(), must_enable_columns_end.end(), attr_) !=
					    must_enable_columns_end.end()) {
						if (std::find(must_enable_columns_start.begin(), must_enable_columns_start.end(), attr_) !=
						    must_enable_columns_start.end()) {
							//! cannot be materialized in this pipeline
							break;
						} else {
							possible_mat_pos[attr].push_back(pipeline_id);
							break;
						}
					}

					possible_mat_pos[attr].push_back(pipeline_id);
				}
			}
		}
	}
	return possible_mat_pos;
}

void write_materialize_config(std::unordered_map<int, std::vector<std::string>> &materialize_config,
                              std::unordered_map<int, bool> &push_source,
                              unordered_map<std::string, int> &from_pipeline) {
	std::unordered_map<int, int> inverted_from_pipeline;
	for (auto &[attr, pipeline_id] : from_pipeline) {
		inverted_from_pipeline[pipeline_id] = 0;
	}
	for (auto &[attr, pipeline_id] : from_pipeline) {
		inverted_from_pipeline[pipeline_id]++;
	}

	for (auto &[pos, attrs] : materialize_config) {
		auto pipeline_ops = plan[std::to_string(pos)]["operators"];
		assert(pipeline_ops.size() >= 2);
		int mat_op_index = pipeline_ops[pipeline_ops.size() - 2]["op_index"];
		unordered_map<int, std::vector<std::string>> from_pipeline_to_attr;
		std::ofstream file("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/config/op_mat_" +
		                       std::to_string(mat_op_index),
		                   std::ios::out);
		for (auto &attr : attrs) {
			from_pipeline_to_attr[from_pipeline[attr]].push_back(attr);
			// if (attr.find(".") != std::string::npos) {
			// 	file << attr.substr(attr.find(".") + 1) << std::endl;
			// } else {
			// 	file << attr << std::endl;
			// }
			file << attr << std::endl;
			inverted_from_pipeline[from_pipeline[attr]]--;
		}
		std::ofstream disable_file("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/config/op_dis_" +
		                               std::to_string(mat_op_index),
		                           std::ios::out);
		for (auto &[pipeline_id, count] : inverted_from_pipeline) {
			if (count == 0) {
				std::string table_name = plan[std::to_string(pipeline_id)]["table"];
				disable_file << "rowid(" << table_name << ")" << std::endl;
			}
		}

		std::ofstream pipeline_file("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/config/pipeline" +
		                                std::to_string(pos),
		                            std::ios::out);
		bool push_src = false;
		if (push_source.find(pos) != push_source.end()) {
			push_src = push_source[pos];
		}

		pipeline_file << "1 " << push_src << " " << QUEUE_THR << std::endl;
		pipeline_file << from_pipeline_to_attr.size() << std::endl;
		for (auto &[pipeline, attrs] : from_pipeline_to_attr) {
			//! fix me: keep_rowid is 1 for now
			std::string table_name = plan[std::to_string(pipeline)]["table"];
			pipeline_file << pipeline << " 1 " << attrs.size() << " rowid(" << table_name << ")" << std::endl;
			for (auto &attr : attrs) {
				int colid_in_basetable = schema[table_name][attr]["col_id"];
				int attri_type = schema[table_name][attr]["type"];
				std::string attr_name = attr;
				// if (attr.find(".") != std::string::npos) {
				// 	attr_name = attr.substr(attr.find(".") + 1);
				// }
				pipeline_file << attr_name << " " << colid_in_basetable << " " << attri_type;
				if (attri_type == 25) {
					//! string type, currently not supporting fixed length string
					pipeline_file << " 0";
				}
				pipeline_file << std::endl;
			}
		}
	}
	for (auto &[pipeid, push_src] : push_source) {
		std::string path =
		    "/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/config/pipeline" + std::to_string(pipeid);
		if (!file_exists(path)) {
			std::ofstream pipeline_file(path, std::ios::out);
			pipeline_file << "0 " << push_src << " " << QUEUE_THR << std::endl;
			pipeline_file << "0" << std::endl;
		}
	}
}
int main(int argc, char *argv[]) {
	std::string thread = argv[1];
	bool print_result = false;
	std::ofstream file("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/config_num");

	if (file.is_open()) {
		file << thread;
		file.close();
	}

	print_result = atoi(argv[2]);

	std::string query_config_path = "/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/query/";
	if (argc > 3) {
		query_config_path = argv[3];
	}
	std::string pipeline_config = query_config_path + "pipeline1.json";
	if (!file_exists(pipeline_config)) {
		std::cout << "You should first export DUMP_PIPELINE_INFO = 1 and collect the pipeline information first."
		          << std::endl;
	}

	std::string query_file = "tmp";
	if (argc > 4) {
		query_file = argv[4];
	}
	query_file = query_config_path + query_file;
	std::cout << query_file << std::endl;
	std::ifstream query_file_stream(query_file, std::ios::in);
	std::string query;
	std::string line;
	while (std::getline(query_file_stream, line)) {
		query += line + " ";
	}
	std::cout << query << std::endl;

	std::string config_directory = "/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/config/";
	if (argc > 5) {
		config_directory = argv[5];
	}

	std::cout << "Warning: will remove all content files in the config directory first" << std::endl;
	std::string command = "rm " + config_directory + "*";
	int cmd_result = system(command.c_str());

	std::string query_directory = "/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/query/pipeline";

	bool dump = false;
	const char *dump_env = std::getenv("DUMP_PIPELINE_INFO");
	if (dump_env != nullptr) {
		int dump_value = std::atoi(dump_env);
		dump = (dump_value == 1);
	}
	if (dump) {
		command = "rm " + query_directory + "*";
		cmd_result = system(command.c_str());
	}

	// DuckDB db(nullptr);
	DuckDB db("/home/yihao/duckdb/origin/duckdb/release/job_uncomtest.db");
	Connection con(db);
	con.Query("SET threads TO " + thread + ";");

	// con.Query("SET disabled_optimizers = 'COMPRESSED_MATERIALIZATION,join_order,build_side_probe_side';");
	con.Query("SET disabled_optimizers = 'COMPRESSED_MATERIALIZATION';");
	bool include_aggregate = false;
	std::string select_keys = query.substr(0, query.find("from"));
	if (select_keys.find("count") != std::string::npos || select_keys.find("sum") != std::string::npos ||
	    select_keys.find("avg") != std::string::npos || select_keys.find("min") != std::string::npos ||
	    select_keys.find("max") != std::string::npos) {
		include_aggregate = true;
	}
	std::string left_query = query.substr(query.find("from"));

	auto all_attributes = parse_plan(dump);
	std::vector<std::string> all_attributes_unique;
	// std::cout << "These are attributes involved in this query:" << std::endl;
	for (auto &attr : all_attributes) {
		// std::cout << attr << std::endl;
		all_attributes_unique.push_back(attr);
	}

	unordered_map<std::string, int> from_pipeline;
	std::unordered_set<std::string> table_name_sets;

	auto all_possible_pos = find_materialize_position(all_attributes_unique, from_pipeline, table_name_sets);
	for (auto &[attr, pipeline_ids] : all_possible_pos) {
		std::cout << attr << ": ";
		for (auto &pipeline_id : pipeline_ids) {
			if (pipeline_id == from_pipeline[attr]) {
				std::cout << pipeline_id << "(same as the source pipeline) ";
				continue;
			}
			std::cout << pipeline_id << " ";
		}
		std::cout << std::endl;
	}

	//! Materialize key selection Phase
	std::vector<std::string> materialize_keys;
	std::string attribute;
	std::cout << "These are current select keys:" << std::endl;
	std::cout << select_keys << std::endl;
	std::cout << "Please input the materialize keys, end with 'end'" << std::endl;
	while (std::cin >> attribute) {
		if (attribute == "end") {
			break;
		}
		materialize_keys.push_back(attribute);
	}

	from_pipeline.clear();
	std::unordered_map<std::string, vector<int>> possible_mat_options;
	if (materialize_keys.size() != 0) {
		possible_mat_options = find_materialize_position(materialize_keys, from_pipeline, table_name_sets);
	}

	for (auto &[attr, pipeline_ids] : possible_mat_options) {
		std::cout << attr << ": ";
		for (auto &pipeline_id : pipeline_ids) {
			if (pipeline_id == from_pipeline[attr]) {
				std::cout << pipeline_id << "(same as the source pipeline) ";
				continue;
			}
			std::cout << pipeline_id << " ";
		}
		std::cout << std::endl;
	}

	std::cout << "Please input the materialize position for each key" << std::endl;
	std::unordered_map<int, std::vector<std::string>> materialize_pos;
	std::unordered_map<int, bool> push_source;
	std::unordered_set<std::string> table_names_mat;
	for (auto &[attr, pipeline_ids] : possible_mat_options) {
		int pos;
		std::cout << attr << ": ";
		std::cin >> pos;
		if (std::find(pipeline_ids.begin(), pipeline_ids.end(), pos) == pipeline_ids.end()) {
			std::cout << "Invalid position" << std::endl;
			return 0;
		}
		for (auto &pipeid : possible_mat_options[attr]) {
			if (pipeid >= pos) {
				push_source[pipeid] = true;
				push_source[from_pipeline[attr]] = true;
			}
		}
		materialize_pos[pos].push_back(attr);
		table_names_mat.insert(attr.substr(0, attr.find(".")));
	}

	//! rewrite the select keys
	for (auto &table_name : table_names_mat) {
		if (include_aggregate) {
			select_keys += ",min(" + table_name + ".rowid) ";
		} else {
			select_keys += "," + table_name + ".rowid ";
		}
	}
	query = select_keys + left_query;
	std::cout << select_keys << std::endl;

	if (materialize_pos.size() == 0) {
		std::cout << "No materialize key is selected or no selected materilaize key is available" << std::endl;
		std::string command = "rm " + config_directory + "*";
		int cmd_result = system(command.c_str());
	}
	write_materialize_config(materialize_pos, push_source, from_pipeline);

	double start = getNow();
	std::cout << query << std::endl;
	auto result = con.Query(query);
	double end = getNow();
	if (print_result) {
		result->Print();
	}
	result->PrintRowNumber();
	std::cout << thread << " " << end - start << std::endl;
	exit(0);
}