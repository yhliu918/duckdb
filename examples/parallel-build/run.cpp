#include <fstream>
#include <iostream>
#include <cstdlib>

const int thread_list[] = {1, 2, 4, 8, 16, 24, 32, 48, 64, 96, 128, 192};

int main(int argc, char **argv) {
	int max_thread = atoi(argv[1]);
	int64_t probe_size = atoi(argv[2]);
	int64_t build_size = atoi(argv[3]);
	int selectivity = atoi(argv[4]);
	int unique_key_ratio = atoi(argv[5]);
	int heavy_hitter_ratio = atoi(argv[6]);
    bool gen_data = atoi(argv[7]);

    std::ifstream payload("payload.config");
	int payload_column_num = 0;
	int payload_tuple_size = 0;
    int type_, size_;
    while (payload >> type_ >> size_) {
        payload_column_num++;
        if (type_ == 0) {
            payload_tuple_size += 4;
        } else if (type_ == 1) {
            payload_tuple_size += 8;
        } else if (type_ == 2) {
            payload_tuple_size += size_;
        } else if (type_ == 3) {
            payload_tuple_size += size_;
        } else {
            abort();
        }
    }
    payload.close();

	std::string file_path = "~/benchmark/gen_data/probe" + std::to_string(probe_size) + "_build" +
	                        std::to_string(build_size) + "_sel" + std::to_string(selectivity) + "_skew" +
	                        std::to_string(heavy_hitter_ratio) + "_" + std::to_string(unique_key_ratio) + "_payload" +
	                        std::to_string(payload_column_num) + "_" + std::to_string(payload_tuple_size);

    if (gen_data) {
        auto result = system(("~/pyvenv/bin/python3 join_data_gen.py " + std::to_string(probe_size) + " " +
            std::to_string(build_size) + " " + std::to_string(selectivity) + " " +
            std::to_string(unique_key_ratio) + " " + std::to_string(heavy_hitter_ratio)).c_str());
        if (result != 0) {
            return 1;
        }
        system(("cp payload.config " + file_path).c_str());
    }

    for (auto thread : thread_list) {
        if (thread > max_thread) {
            break;
        }
        system(("./build/benchmark " + std::to_string(thread) + " 0 " + std::to_string(probe_size) + " " +
            std::to_string(build_size) + " " + std::to_string(selectivity) + " " +
            std::to_string(unique_key_ratio) + " " + std::to_string(heavy_hitter_ratio) + " " +
            std::to_string(payload_column_num) + " " + std::to_string(payload_tuple_size) + " 0 " +
            " > " + file_path + "/result_thread" + std::to_string(thread) + ".txt").c_str());
    }
}