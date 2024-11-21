#pragma once
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
namespace duckdb {

class Optimizer;

class MaterializePlanner : public LogicalOperatorVisitor {
public:
	explicit MaterializePlanner(Optimizer &optimizer);

	void VisitOperator(LogicalOperator &op) override;

private:
	void AddMaterialize(LogicalComparisonJoin &join);

private:
	Optimizer &optimizer;
	int join_index = 0;
	std::unordered_map<int, bool> col_rowid_keep;
	std::vector<int8_t> mat_type;
};
} // namespace duckdb