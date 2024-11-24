#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalProjection &op) {
	D_ASSERT(op.children.size() == 1);
	auto plan = CreatePlan(*op.children[0]);

#ifdef DEBUG
	for (auto &expr : op.expressions) {
		D_ASSERT(!expr->IsWindow());
		D_ASSERT(!expr->IsAggregate());
	}
#endif
	if (plan->types.size() == op.types.size()) {
		// check if this projection can be omitted entirely
		// this happens if a projection simply emits the columns in the same order
		// e.g. PROJECTION(#0, #1, #2, #3, ...)
		bool omit_projection = true;
		for (idx_t i = 0; i < op.types.size(); i++) {
			if (op.expressions[i]->type == ExpressionType::BOUND_REF) {
				auto &bound_ref = op.expressions[i]->Cast<BoundReferenceExpression>();
				if (bound_ref.index == i) {
					continue;
				}
			}
			omit_projection = false;
			break;
		}
		if (omit_projection) {
			// the projection only directly projects the child' columns: omit it entirely
			return plan;
		}
	}
	vector<int> disable_column;
	auto projection = make_uniq<PhysicalProjection>(op.types, std::move(op.expressions), op.estimated_cardinality);
	for (auto &expr : projection->select_list) {
		if (expr->type == ExpressionType::BOUND_REF) {
			auto &bound_ref = (BoundReferenceExpression &)*expr;
			disable_column.push_back(plan->disable_columns[bound_ref.index]);
		}
	}
	projection->disable_columns = std::move(disable_column);
	projection->output_disable_columns = projection->disable_columns;
	projection->children.push_back(std::move(plan));

	return std::move(projection);
}

} // namespace duckdb
