#include "duckdb/optimizer/materialize_planner.hpp"

#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include <fstream>
namespace duckdb {

MaterializePlanner::MaterializePlanner(Optimizer &optimizer) : optimizer(optimizer) {
}
void MaterializePlanner::AddMaterialize(LogicalComparisonJoin &join) {
	// read a file, which indicates the materialized entry in this join
	std::ifstream file("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/config/join" +
	                       std::to_string(join_index++),
	                   std::ios::in);
	if (file.is_open()) {
		// materialize info
		int groups_of_info;
		file >> groups_of_info;
		for (int i = 0; i < groups_of_info; i++) {
			int table_col_idx;
			int keep_rowid;
			int logical_type;

			file >> table_col_idx >> keep_rowid >> logical_type;
			col_rowid_keep[table_col_idx] = keep_rowid;
			mat_type.push_back(logical_type);
		}
	}
	join.col_rowid_keep = col_rowid_keep;
	join.mat_type = mat_type;
}
void MaterializePlanner::VisitOperator(LogicalOperator &op) {
	// first get a leaf node
	LogicalOperatorVisitor::VisitOperator(op);
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		// add materialize info, if necessary
		AddMaterialize(op.Cast<LogicalComparisonJoin>());
	}
	if (this->col_rowid_keep.size() > 0 || this->mat_type.size() > 0) {
		// if there is materialize info, add it to the projection
		op.col_rowid_keep = this->col_rowid_keep;
		op.mat_type = this->mat_type;
	}
}
} // namespace duckdb