#include "duckdb/execution/operator/filter/physical_filter.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
namespace duckdb {

void parse_filter(vector<idx_t> &filter_columns, Expression &expr) {
	if (expr.type == ExpressionType::BOUND_REF) {
		auto &bound_ref = (BoundReferenceExpression &)expr;
		filter_columns.push_back(bound_ref.index);
	} else if (expr.type == ExpressionType::BOUND_FUNCTION) {
		auto &bound_func = (BoundFunctionExpression &)expr;
		for (auto &child : bound_func.children) {
			parse_filter(filter_columns, *child);
		}
	} else if (expr.type == ExpressionType::CONJUNCTION_AND) {
		auto &conjunction = (BoundConjunctionExpression &)expr;
		for (auto &child : conjunction.children) {
			parse_filter(filter_columns, *child);
		}
	} else if (expr.type == ExpressionType::CONJUNCTION_OR) {
		auto &conjunction = (BoundConjunctionExpression &)expr;
		for (auto &child : conjunction.children) {
			parse_filter(filter_columns, *child);
		}
	} else if (expr.type == ExpressionType::COMPARE_GREATERTHAN ||
	           expr.type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
	           expr.type == ExpressionType::COMPARE_EQUAL || expr.type == ExpressionType::COMPARE_LESSTHAN ||
	           expr.type == ExpressionType::COMPARE_LESSTHANOREQUALTO ||
	           expr.type == ExpressionType::COMPARE_NOTEQUAL) {
		auto &comparison = (BoundComparisonExpression &)expr;
		parse_filter(filter_columns, *comparison.left.get());
		parse_filter(filter_columns, *comparison.right.get());
	}
}
PhysicalFilter::PhysicalFilter(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                               idx_t estimated_cardinality)
    : CachingPhysicalOperator(PhysicalOperatorType::FILTER, std::move(types), estimated_cardinality) {
	D_ASSERT(select_list.size() > 0);
	for (auto &expr : select_list) {
		parse_filter(filter_columns, *expr);
	}

	if (select_list.size() > 1) {
		// create a big AND out of the expressions
		auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		for (auto &expr : select_list) {
			conjunction->children.push_back(std::move(expr));
		}
		expression = std::move(conjunction);
	} else {
		expression = std::move(select_list[0]);
	}
}

class FilterState : public CachingOperatorState {
public:
	explicit FilterState(ExecutionContext &context, Expression &expr)
	    : executor(context.client, expr), sel(STANDARD_VECTOR_SIZE) {
	}

	ExpressionExecutor executor;
	SelectionVector sel;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op);
	}
};

unique_ptr<OperatorState> PhysicalFilter::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<FilterState>(context, *expression);
}

OperatorResultType PhysicalFilter::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                   GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<FilterState>();
	idx_t result_count = state.executor.SelectExpression(input, state.sel);
	if (result_count == input.size()) {
		// nothing was filtered: skip adding any selection vectors
		chunk.Reference(input);
	} else {
		chunk.Slice(input, state.sel, result_count);
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

InsertionOrderPreservingMap<string> PhysicalFilter::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["__expression__"] = expression->GetName();
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
