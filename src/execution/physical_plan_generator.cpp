#include "duckdb/execution/physical_plan_generator.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/helper/physical_verify_vector.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

#include <assert.h>
#include <fstream>
namespace duckdb {

class DependencyExtractor : public LogicalOperatorVisitor {
public:
	explicit DependencyExtractor(LogicalDependencyList &dependencies) : dependencies(dependencies) {
	}

protected:
	unique_ptr<Expression> VisitReplace(BoundFunctionExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		// extract dependencies from the bound function expression
		if (expr.function.dependency) {
			expr.function.dependency(expr, dependencies);
		}
		return nullptr;
	}

private:
	LogicalDependencyList &dependencies;
};

PhysicalPlanGenerator::PhysicalPlanGenerator(ClientContext &context) : context(context) {
}

PhysicalPlanGenerator::~PhysicalPlanGenerator() {
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(unique_ptr<LogicalOperator> op) {
	auto &profiler = QueryProfiler::Get(context);

	// first resolve column references
	profiler.StartPhase(MetricsType::PHYSICAL_PLANNER_COLUMN_BINDING);
	ColumnBindingResolver resolver;
	resolver.VisitOperator(*op);
	profiler.EndPhase();

	// now resolve types of all the operators
	profiler.StartPhase(MetricsType::PHYSICAL_PLANNER_RESOLVE_TYPES);
	op->ResolveOperatorTypes();
	profiler.EndPhase();

	// extract dependencies from the logical plan
	DependencyExtractor extractor(dependencies);
	extractor.VisitOperator(*op);

	// then create the main physical plan
	profiler.StartPhase(MetricsType::PHYSICAL_PLANNER_CREATE_PLAN);
	auto plan = CreatePlan(*op);
	profiler.EndPhase();

	plan->Verify();
	return plan;
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalOperator &op) {
	op.estimated_cardinality = op.EstimateCardinality(context);
	unique_ptr<PhysicalOperator> plan = nullptr;

	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
		plan = CreatePlan(op.Cast<LogicalGet>());
		break;
	case LogicalOperatorType::LOGICAL_PROJECTION:
		plan = CreatePlan(op.Cast<LogicalProjection>());
		break;
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		plan = CreatePlan(op.Cast<LogicalEmptyResult>());
		break;
	case LogicalOperatorType::LOGICAL_FILTER:
		plan = CreatePlan(op.Cast<LogicalFilter>());
		break;
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		plan = CreatePlan(op.Cast<LogicalAggregate>());
		break;
	case LogicalOperatorType::LOGICAL_WINDOW:
		plan = CreatePlan(op.Cast<LogicalWindow>());
		break;
	case LogicalOperatorType::LOGICAL_UNNEST:
		plan = CreatePlan(op.Cast<LogicalUnnest>());
		break;
	case LogicalOperatorType::LOGICAL_LIMIT:
		plan = CreatePlan(op.Cast<LogicalLimit>());
		break;
	case LogicalOperatorType::LOGICAL_SAMPLE:
		plan = CreatePlan(op.Cast<LogicalSample>());
		break;
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		plan = CreatePlan(op.Cast<LogicalOrder>());
		break;
	case LogicalOperatorType::LOGICAL_TOP_N:
		plan = CreatePlan(op.Cast<LogicalTopN>());
		break;
	case LogicalOperatorType::LOGICAL_COPY_TO_FILE:
		plan = CreatePlan(op.Cast<LogicalCopyToFile>());
		break;
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		plan = CreatePlan(op.Cast<LogicalDummyScan>());
		break;
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
		plan = CreatePlan(op.Cast<LogicalAnyJoin>());
		break;
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		plan = CreatePlan(op.Cast<LogicalComparisonJoin>());
		break;
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		plan = CreatePlan(op.Cast<LogicalCrossProduct>());
		break;
	case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
		plan = CreatePlan(op.Cast<LogicalPositionalJoin>());
		break;
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
		plan = CreatePlan(op.Cast<LogicalSetOperation>());
		break;
	case LogicalOperatorType::LOGICAL_INSERT:
		plan = CreatePlan(op.Cast<LogicalInsert>());
		break;
	case LogicalOperatorType::LOGICAL_DELETE:
		plan = CreatePlan(op.Cast<LogicalDelete>());
		break;
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
		plan = CreatePlan(op.Cast<LogicalColumnDataGet>());
		break;
	case LogicalOperatorType::LOGICAL_DELIM_GET:
		plan = CreatePlan(op.Cast<LogicalDelimGet>());
		break;
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
		plan = CreatePlan(op.Cast<LogicalExpressionGet>());
		break;
	case LogicalOperatorType::LOGICAL_UPDATE:
		plan = CreatePlan(op.Cast<LogicalUpdate>());
		break;
	case LogicalOperatorType::LOGICAL_CREATE_TABLE:
		plan = CreatePlan(op.Cast<LogicalCreateTable>());
		break;
	case LogicalOperatorType::LOGICAL_CREATE_INDEX:
		plan = CreatePlan(op.Cast<LogicalCreateIndex>());
		break;
	case LogicalOperatorType::LOGICAL_CREATE_SECRET:
		plan = CreatePlan(op.Cast<LogicalCreateSecret>());
		break;
	case LogicalOperatorType::LOGICAL_EXPLAIN:
		plan = CreatePlan(op.Cast<LogicalExplain>());
		break;
	case LogicalOperatorType::LOGICAL_DISTINCT:
		plan = CreatePlan(op.Cast<LogicalDistinct>());
		break;
	case LogicalOperatorType::LOGICAL_PREPARE:
		plan = CreatePlan(op.Cast<LogicalPrepare>());
		break;
	case LogicalOperatorType::LOGICAL_EXECUTE:
		plan = CreatePlan(op.Cast<LogicalExecute>());
		break;
	case LogicalOperatorType::LOGICAL_CREATE_VIEW:
	case LogicalOperatorType::LOGICAL_CREATE_SEQUENCE:
	case LogicalOperatorType::LOGICAL_CREATE_SCHEMA:
	case LogicalOperatorType::LOGICAL_CREATE_MACRO:
	case LogicalOperatorType::LOGICAL_CREATE_TYPE:
		plan = CreatePlan(op.Cast<LogicalCreate>());
		break;
	case LogicalOperatorType::LOGICAL_PRAGMA:
		plan = CreatePlan(op.Cast<LogicalPragma>());
		break;
	case LogicalOperatorType::LOGICAL_VACUUM:
		plan = CreatePlan(op.Cast<LogicalVacuum>());
		break;
	case LogicalOperatorType::LOGICAL_TRANSACTION:
	case LogicalOperatorType::LOGICAL_ALTER:
	case LogicalOperatorType::LOGICAL_DROP:
	case LogicalOperatorType::LOGICAL_LOAD:
	case LogicalOperatorType::LOGICAL_ATTACH:
	case LogicalOperatorType::LOGICAL_DETACH:
		plan = CreatePlan(op.Cast<LogicalSimple>());
		break;
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
		plan = CreatePlan(op.Cast<LogicalRecursiveCTE>());
		break;
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
		plan = CreatePlan(op.Cast<LogicalMaterializedCTE>());
		break;
	case LogicalOperatorType::LOGICAL_CTE_REF:
		plan = CreatePlan(op.Cast<LogicalCTERef>());
		break;
	case LogicalOperatorType::LOGICAL_EXPORT:
		plan = CreatePlan(op.Cast<LogicalExport>());
		break;
	case LogicalOperatorType::LOGICAL_SET:
		plan = CreatePlan(op.Cast<LogicalSet>());
		break;
	case LogicalOperatorType::LOGICAL_RESET:
		plan = CreatePlan(op.Cast<LogicalReset>());
		break;
	case LogicalOperatorType::LOGICAL_PIVOT:
		plan = CreatePlan(op.Cast<LogicalPivot>());
		break;
	case LogicalOperatorType::LOGICAL_COPY_DATABASE:
		plan = CreatePlan(op.Cast<LogicalCopyDatabase>());
		break;
	case LogicalOperatorType::LOGICAL_UPDATE_EXTENSIONS:
		plan = CreatePlan(op.Cast<LogicalSimple>());
		break;
	case LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR:
		plan = op.Cast<LogicalExtensionOperator>().CreatePlan(context, *this);

		if (!plan) {
			throw InternalException("Missing PhysicalOperator for Extension Operator");
		}
		break;
	case LogicalOperatorType::LOGICAL_JOIN:
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
	case LogicalOperatorType::LOGICAL_INVALID: {
		throw NotImplementedException("Unimplemented logical operator type!");
	}
	}
	plan->operator_index = operator_idx;
	operator_idx++;
	auto res_types = PrintOperatorCatalog(plan);
	// for (auto &res_type : res_types) {
	// 	std::cout << res_type << std::endl;
	// }
	if (res_types.size() < plan->types.size()) {
		for (int i = res_types.size(); i < plan->types.size(); i++) {
			res_types.push_back("NULL");
		}
	}
	if (res_types.size() > 0) {
		plan->names = std::move(res_types);
	}

	std::ifstream infile;
	infile.open("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/config/op_mat_" +
	                std::to_string(plan->operator_index),
	            std::ios::in);
	if (infile.is_open() && plan->type != PhysicalOperatorType::TRANSACTION &&
	    plan->type != PhysicalOperatorType::RESET && plan->type != PhysicalOperatorType::SET &&
	    plan->type != PhysicalOperatorType::PRAGMA) {
		std::string mat_name;
		while (infile >> mat_name) {
			int mat_col = std::find(plan->names.begin(), plan->names.end(), mat_name) - plan->names.begin();
			if (plan->output_disable_columns[mat_col] == 0) {
				std::cout << "Failed to disable column " << mat_name << std::endl;
			}
			assert(plan->output_disable_columns[mat_col] != 0);
			plan->output_disable_columns[mat_col] = 0;
		}
	}

	std::ifstream disable_columns_file;
	disable_columns_file.open("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/config/op_dis_" +
	                              std::to_string(plan->operator_index),
	                          std::ios::in);
	if (disable_columns_file.is_open() && plan->type != PhysicalOperatorType::TRANSACTION &&
	    plan->type != PhysicalOperatorType::RESET && plan->type != PhysicalOperatorType::SET &&
	    plan->type != PhysicalOperatorType::PRAGMA) {
		std::string mat_name;
		while (disable_columns_file >> mat_name) {
			int mat_col = std::find(plan->names.begin(), plan->names.end(), mat_name) - plan->names.begin();
			if (plan->output_disable_columns[mat_col] != 0) {
				std::cout << "Failed to enable column " << mat_name << std::endl;
			}
			assert(plan->output_disable_columns[mat_col] == 0);
			plan->output_disable_columns[mat_col] = 1;
		}
	}

	std::string op_str = PrintOperator(plan);
	std::cout << op_str << std::endl;
	if (!plan) {
		throw InternalException("Physical plan generator - no plan generated");
	}

	plan->estimated_cardinality = op.estimated_cardinality;
#ifdef DUCKDB_VERIFY_VECTOR_OPERATOR
	auto verify = make_uniq<PhysicalVerifyVector>(std::move(plan));
	plan = std::move(verify);
#endif

	return plan;
}

std::string PhysicalPlanGenerator::PrintOperator(const unique_ptr<PhysicalOperator> &plan) {
	std::string op_str = std::to_string(plan->operator_index) + " " + plan->GetName() + "\n";
	switch (plan->type) {
	case PhysicalOperatorType::ORDER_BY: {
		auto &order = plan->Cast<PhysicalOrder>();
		int i = 0;
		for (auto &type : plan->types) {
			op_str += "(" + std::to_string(i) + ", " + type.ToString() + ", " +
			          std::to_string(plan->disable_columns[i]) + " ," + plan->names[i] + ")\n";
			i++;
		}
		break;
	}
	case PhysicalOperatorType::HASH_GROUP_BY: {
		auto &aggregate = plan->Cast<PhysicalHashAggregate>();
		int i = 0;
		for (auto &type : plan->types) {
			op_str += "(" + std::to_string(i) + ", " + type.ToString() + ", " +
			          std::to_string(plan->disable_columns[i]) + " ," + plan->names[i] + ")\n";
			i++;
		}
		break;
	}
	case PhysicalOperatorType::UNGROUPED_AGGREGATE: {
		auto &aggregate = plan->Cast<PhysicalUngroupedAggregate>();
		int i = 0;
		for (auto &type : plan->types) {
			op_str += "(" + std::to_string(i) + ", " + type.ToString() + ", " +
			          std::to_string(plan->disable_columns[i]) + " ," + plan->names[i] + ")\n";
			i++;
		}
		break;
	}
	case PhysicalOperatorType::PROJECTION: {
		int i = 0;
		for (auto &type : plan->types) {
			op_str += "(" + std::to_string(i) + ", " + type.ToString() + ", " +
			          std::to_string(plan->disable_columns[i]) + " ," + plan->names[i] + ")\n";
			i++;
		}
		break;
	}
	case PhysicalOperatorType::TABLE_SCAN: {
		int i = 0;
		for (auto &type : plan->types) {
			op_str += "(" + std::to_string(i) + ", " + type.ToString() + ", " +
			          std::to_string(plan->disable_columns[i]) + " ," + plan->names[i] + ")\n";
			i++;
		}
		break;
	}
	case PhysicalOperatorType::HASH_JOIN: {

		int i = 0;
		for (auto &type : plan->types) {
			std::string entry_name;
			if (i < plan->names.size()) {
				entry_name = plan->names[i];
			} else {
				entry_name = "NULL";
			}
			op_str += "(" + std::to_string(i) + ", " + type.ToString() + ", " +
			          std::to_string(plan->disable_columns[i]) + " ," + entry_name + ")\n";
			i++;
		}
		break;
	}
	default:
		std::cout << "Unknown operator type: " << PhysicalOperatorToString(plan->type) << std::endl;
		break;
	}
	return op_str;
}

std::vector<std::string> PhysicalPlanGenerator::PrintOperatorCatalog(const unique_ptr<PhysicalOperator> &plan) {
	std::vector<std::string> op_str;
	switch (plan->type) {
	case PhysicalOperatorType::ORDER_BY: {
		auto &order = plan->Cast<PhysicalOrder>();
		std::vector<std::string> op_str_child = PrintOperatorCatalog(order.children[0]);
		for (int i = 0; i < order.orders.size(); i++) {
			auto &bound_ref = order.orders[i].expression->Cast<BoundReferenceExpression>();
			op_str.push_back(op_str_child[bound_ref.index]);
		}
		for (int i = 0; i < order.projections.size(); i++) {
			op_str.push_back(op_str_child[order.projections[i]]);
		}
		break;
	}
	case PhysicalOperatorType::HASH_GROUP_BY: {
		auto &aggregate = plan->Cast<PhysicalHashAggregate>();
		std::vector<std::string> op_str_child = PrintOperatorCatalog(aggregate.children[0]);
		for (int i = 0; i < aggregate.grouped_aggregate_data.groups.size(); i++) {
			auto &group = aggregate.grouped_aggregate_data.groups[i];
			if (group->type == ExpressionType::BOUND_REF) {
				auto &bound_ref = (BoundReferenceExpression &)*group;
				op_str.push_back(op_str_child[bound_ref.index]);
			}
		}
		for (int i = 0; i < aggregate.grouped_aggregate_data.aggregates.size(); i++) {
			auto &aggr = aggregate.grouped_aggregate_data.aggregates[i]->Cast<BoundAggregateExpression>();
			for (auto &child : aggr.children) {
				auto &bound_ref = child->Cast<BoundReferenceExpression>();
				op_str.push_back(op_str_child[bound_ref.index]);
			}
			if (aggr.filter) {
				auto &bound_ref = aggr.filter->Cast<BoundReferenceExpression>();
				op_str.push_back(op_str_child[bound_ref.index]);
			}
		}
		break;
	}
	case PhysicalOperatorType::UNGROUPED_AGGREGATE: {
		auto &aggregate = plan->Cast<PhysicalUngroupedAggregate>();
		std::vector<std::string> op_str_child = PrintOperatorCatalog(aggregate.children[0]);
		for (int i = 0; i < aggregate.aggregates.size(); i++) {
			op_str.push_back(op_str_child[i]);
		}
		break;
	}
	case PhysicalOperatorType::PROJECTION: {
		auto &projection = plan->Cast<PhysicalProjection>();
		std::vector<std::string> op_str_child = PrintOperatorCatalog(projection.children[0]);
		for (auto &expr : projection.select_list) {
			if (expr->type == ExpressionType::BOUND_REF) {
				auto &bound_ref = (BoundReferenceExpression &)*expr;
				op_str.push_back(op_str_child[bound_ref.index]);
			}
		}
		break;
	}
	case PhysicalOperatorType::FILTER: {
		auto &filter = plan->Cast<PhysicalFilter>();
		std::vector<std::string> op_str_child = PrintOperatorCatalog(filter.children[0]);
		op_str = op_str_child;
		if (plan->must_enables_left.size() == 0) {
			for (auto idx : filter.filter_columns) {
				plan->must_enables_left.push_back(op_str_child[idx]);
			}
		}
		break;
	}
	case PhysicalOperatorType::TABLE_SCAN: {
		auto &table_scan = plan->Cast<PhysicalTableScan>();
		bool has_disabled = false;
		for (int i = 0; i < table_scan.output_disable_columns.size(); i++) {
			if (table_scan.output_disable_columns[i] == 1) {
				has_disabled = true;
				break;
			}
		}
		if (table_scan.table_filters) {
			for (auto &[col_idx, _] : table_scan.table_filters->filters) {
				table_scan.must_enables_left.push_back(table_scan.names[table_scan.column_ids_total[col_idx]]);
			}
		}
		if (!has_disabled) {
			for (int i = 0; i < table_scan.projection_ids.size(); i++) {
				op_str.push_back(table_scan.names[table_scan.column_ids_total[table_scan.projection_ids[i]]]);
			}
			break;
		}
		//! include disabled columns, return all columns
		for (int i = 0; i < table_scan.projection_ids_total.size(); i++) {
			op_str.push_back(table_scan.names[table_scan.column_ids_total[table_scan.projection_ids_total[i]]]);
		}
		break;
	}
	case PhysicalOperatorType::HASH_JOIN: {
		auto &hash_join = plan->Cast<PhysicalHashJoin>();
		std::vector<std::string> op_str_child_left = PrintOperatorCatalog(hash_join.children[0]);
		std::vector<std::string> op_str_child_right = PrintOperatorCatalog(hash_join.children[1]);
		// join keys
		op_str = op_str_child_left;
		if (plan->must_enables_left.size() == 0) {
			for (int i = 0; i < hash_join.condition_types.size(); i++) {
				int left_idx = hash_join.conditions[i].left->Cast<BoundReferenceExpression>().index;
				plan->must_enables_left.push_back(op_str_child_left[left_idx]);
			}
		}
		if (op_str_child_right.size() == 0) {
			if (hash_join.types.size() > op_str.size()) {
				op_str.push_back("NULL");
			}
			break;
		}
		if (plan->must_enables_right.size() == 0) {
			for (int i = 0; i < hash_join.condition_types.size(); i++) {
				int right_idx = hash_join.conditions[i].right->Cast<BoundReferenceExpression>().index;
				plan->must_enables_right.push_back(op_str_child_right[right_idx]);
			}
		}
		// payload columns
		for (int i = 0; i < hash_join.payload_column_idxs_total.size(); i++) {
			op_str.push_back(op_str_child_right[hash_join.payload_column_idxs_total[i]]);
		}
		if (hash_join.types.size() > op_str.size()) {
			op_str.push_back("NULL");
		}
		break;
		// right join keys
		// vector<std::string> right_join_keys;
		// for (int i = 0; i < hash_join.condition_types.size(); i++) {
		// 	int right_idx = hash_join.conditions[i].right->Cast<BoundReferenceExpression>().index;
		// 	right_join_keys.push_back(op_str_child_right[right_idx]);
		// 	// op_str.push_back(op_str_child_right[right_idx]);
		// }
		// if (right_has_disabled) {
		// 	for (int i = 0; i < hash_join.payload_column_idxs_total.size(); i++) {
		// 		right_join_keys.push_back(op_str_child_right[hash_join.payload_column_idxs_total[i]]);
		// 	}
		// } else {
		// 	for (int i = 0; i < hash_join.payload_column_idxs.size(); i++) {
		// 		right_join_keys.push_back(op_str_child_right[hash_join.payload_column_idxs[i]]);
		// 	}
		// }
		// for (int i = 0; i < hash_join.rhs_output_columns.size(); i++) {
		// 	op_str.push_back(right_join_keys[hash_join.rhs_output_columns[i]]);
		// }
		// break;
	}
	default:
		break;
	}
	return op_str;
}

} // namespace duckdb
