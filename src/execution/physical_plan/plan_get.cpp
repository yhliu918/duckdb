#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/projection/physical_tableinout_function.hpp"
#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"
#include "duckdb/execution/operator/scan/physical_expression_scan.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <fstream>
namespace duckdb {

unique_ptr<TableFilterSet> CreateTableFilterSet(TableFilterSet &table_filters, const vector<column_t> &column_ids) {
	// create the table filter map
	auto table_filter_set = make_uniq<TableFilterSet>();
	for (auto &table_filter : table_filters.filters) {
		// find the relative column index from the absolute column index into the table
		idx_t column_index = DConstants::INVALID_INDEX;
		for (idx_t i = 0; i < column_ids.size(); i++) {
			if (table_filter.first == column_ids[i]) {
				column_index = i;
				break;
			}
		}
		if (column_index == DConstants::INVALID_INDEX) {
			throw InternalException("Could not find column index for table filter");
		}
		table_filter_set->filters[column_index] = std::move(table_filter.second);
	}
	return table_filter_set;
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalGet &op) {
	auto column_ids = op.GetColumnIds();
	if (!op.children.empty()) {
		auto child_node = CreatePlan(std::move(op.children[0]));
		// this is for table producing functions that consume subquery results
		// push a projection node with casts if required
		if (child_node->types.size() < op.input_table_types.size()) {
			throw InternalException(
			    "Mismatch between input table types and child node types - expected %llu but got %llu",
			    op.input_table_types.size(), child_node->types.size());
		}
		vector<LogicalType> return_types;
		vector<unique_ptr<Expression>> expressions;
		bool any_cast_required = false;
		for (idx_t proj_idx = 0; proj_idx < child_node->types.size(); proj_idx++) {
			auto ref = make_uniq<BoundReferenceExpression>(child_node->types[proj_idx], proj_idx);
			auto &target_type =
			    proj_idx < op.input_table_types.size() ? op.input_table_types[proj_idx] : child_node->types[proj_idx];
			if (child_node->types[proj_idx] != target_type) {
				// cast is required - push a cast
				any_cast_required = true;
				auto cast = BoundCastExpression::AddCastToType(context, std::move(ref), target_type);
				expressions.push_back(std::move(cast));
			} else {
				expressions.push_back(std::move(ref));
			}
			return_types.push_back(target_type);
		}
		if (any_cast_required) {
			auto proj = make_uniq<PhysicalProjection>(std::move(return_types), std::move(expressions),
			                                          child_node->estimated_cardinality);
			proj->children.push_back(std::move(child_node));
			child_node = std::move(proj);
		}

		auto node = make_uniq<PhysicalTableInOutFunction>(op.types, op.function, std::move(op.bind_data), column_ids,
		                                                  op.estimated_cardinality, std::move(op.projected_input));
		node->children.push_back(std::move(child_node));
		return std::move(node);
	}
	if (!op.projected_input.empty()) {
		throw InternalException("LogicalGet::project_input can only be set for table-in-out functions");
	}

	unique_ptr<TableFilterSet> table_filters;
	if (!op.table_filters.filters.empty()) {
		table_filters = CreateTableFilterSet(op.table_filters, column_ids);
	}

	if (op.function.dependency) {
		op.function.dependency(dependencies, op.bind_data.get());
	}
	unique_ptr<PhysicalFilter> filter;

	auto &projection_ids = op.projection_ids;

	if (table_filters && op.function.supports_pushdown_type) {
		vector<unique_ptr<Expression>> select_list;
		unique_ptr<Expression> unsupported_filter;
		unordered_set<idx_t> to_remove;
		for (auto &entry : table_filters->filters) {
			auto column_id = column_ids[entry.first];
			auto &type = op.returned_types[column_id];
			if (!op.function.supports_pushdown_type(type)) {
				idx_t column_id_filter = entry.first;
				bool found_projection = false;
				for (idx_t i = 0; i < projection_ids.size(); i++) {
					if (column_ids[projection_ids[i]] == column_ids[entry.first]) {
						column_id_filter = i;
						found_projection = true;
						break;
					}
				}
				if (!found_projection) {
					projection_ids.push_back(entry.first);
					column_id_filter = projection_ids.size() - 1;
				}
				auto column = make_uniq<BoundReferenceExpression>(type, column_id_filter);
				select_list.push_back(entry.second->ToExpression(*column));
				to_remove.insert(entry.first);
			}
		}
		for (auto &col : to_remove) {
			table_filters->filters.erase(col);
		}

		if (!select_list.empty()) {
			vector<LogicalType> filter_types;
			for (auto &c : projection_ids) {
				filter_types.push_back(op.returned_types[column_ids[c]]);
			}
			filter = make_uniq<PhysicalFilter>(filter_types, std::move(select_list), op.estimated_cardinality);
		}
	}
	op.ResolveOperatorTypes();
	// create the table scan node
	if (!op.function.projection_pushdown) {
		// function does not support projection pushdown
		auto node = make_uniq<PhysicalTableScan>(op.returned_types, op.function, std::move(op.bind_data),
		                                         op.returned_types, column_ids, vector<column_t>(), op.names,
		                                         std::move(table_filters), op.estimated_cardinality, op.extra_info,
		                                         std::move(op.parameters), op.GetTable()->GetStorage().GetTableName());
		// first check if an additional projection is necessary
		if (column_ids.size() == op.returned_types.size()) {
			bool projection_necessary = false;
			for (idx_t i = 0; i < column_ids.size(); i++) {
				if (column_ids[i] != i) {
					projection_necessary = true;
					break;
				}
			}
			if (!projection_necessary) {
				// a projection is not necessary if all columns have been requested in-order
				// in that case we just return the node
				if (filter) {
					filter->children.push_back(std::move(node));
					return std::move(filter);
				}
				return std::move(node);
			}
		}
		// push a projection on top that does the projection
		vector<LogicalType> types;
		vector<unique_ptr<Expression>> expressions;
		for (auto &column_id : column_ids) {
			if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
				types.emplace_back(LogicalType::BIGINT);
				expressions.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(0)));
			} else {
				auto type = op.returned_types[column_id];
				types.push_back(type);
				expressions.push_back(make_uniq<BoundReferenceExpression>(type, column_id));
			}
		}
		unique_ptr<PhysicalProjection> projection =
		    make_uniq<PhysicalProjection>(std::move(types), std::move(expressions), op.estimated_cardinality);
		if (filter) {
			filter->children.push_back(std::move(node));
			projection->children.push_back(std::move(filter));
		} else {
			projection->children.push_back(std::move(node));
		}
		return std::move(projection);
	} else {
		std::string table_name;
		if (op.GetTable().get() != nullptr) {
			table_name = op.GetTable()->GetStorage().GetTableName();
		}
		std::ifstream file("/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/config/table" + table_name,
		                   std::ios::in);
		vector<int> del_column;
		int column_id;
		if (file.is_open()) {
			while (file >> column_id) {
				del_column.push_back(column_id);
			}
		}
		bool flag = true;
		for (int i = 0; i < column_ids.size() - 1; i++) {
			if (column_ids[i] + 1 != column_ids[i + 1]) {
				flag = false;
				break;
			}
		}
		flag = false;
		// std::ofstream file_out("/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/config/table_scan" +
		//                            std::to_string(op.table_index),
		//                        std::ios::out);
		// for (auto &column_id : column_ids) {
		// 	file_out << column_id << std::endl;
		// }
		if (flag) {
			vector<int> disable_column;
			for (int i = 0; i < column_ids.size(); i++) {
				disable_column.push_back(0);
			}

			auto node = make_uniq<PhysicalTableScan>(op.types, op.function, std::move(op.bind_data), op.returned_types,
			                                         column_ids, op.projection_ids, op.names, std::move(table_filters),
			                                         op.estimated_cardinality, op.extra_info, std::move(op.parameters),
			                                         table_name, std::move(disable_column));
			node->dynamic_filters = op.dynamic_filters;
			// node->disable_columns = std::move(disable_column);
			if (filter) {
				filter->children.push_back(std::move(node));
				return std::move(filter);
			}
			return std::move(node);
		} else {
			vector<int> disable_column;
			for (int i = 0; i < column_ids.size(); i++) {
				// column_ids_new[op.projection_ids[i]] in del_column
				if (std::find(del_column.begin(), del_column.end(), column_ids[i]) != del_column.end()) {
					disable_column.push_back(1);
				} else {
					disable_column.push_back(0);
				}
			}
			vector<column_t> column_ids_new;
			vector<idx_t> projection_ids_old = op.projection_ids;
			std::vector<int> column_project_bit_map;
			for (int i = 0; i < column_ids.size(); i++) {
				column_project_bit_map.push_back(0);
			}
			for (auto &i : op.projection_ids) {
				column_project_bit_map[i] = 1;
			}

			vector<int> erase_set;
			// op.types.clear();
			for (int i = 0; i < column_ids.size(); i++) {
				if (std::find(del_column.begin(), del_column.end(), column_ids[i]) == del_column.end()) {
					column_ids_new.push_back(column_ids[i]);
				} else {
					erase_set.push_back(i);
				}
			}
			vector<idx_t> projection_ids_new;
			int project_idx = 0;
			for (int i = 0; i < column_project_bit_map.size(); i++) {
				bool find = std::find(erase_set.begin(), erase_set.end(), i) != erase_set.end();
				if (!find) {
					if (column_project_bit_map[i] == 1) {
						projection_ids_new.push_back(project_idx);
						project_idx++;
					} else {
						project_idx++;
					}
				}
			}
			unique_ptr<TableFilterSet> table_filters_new;
			if (!op.table_filters.filters.empty()) {
				table_filters_new = make_uniq<TableFilterSet>();
				for (auto &[idx, filter] : table_filters->filters) {
					int column_idx_new = std::find(column_ids_new.begin(), column_ids_new.end(), column_ids[idx]) -
					                     column_ids_new.begin();
					table_filters_new->filters[column_idx_new] = std::move(table_filters->filters[idx]);
				}
			}

			// for (auto &i : op.projection_ids) {
			// 	op.types.push_back(op.returned_types[column_ids[i]]);
			// }
			auto node = make_uniq<PhysicalTableScan>(
			    op.types, op.function, std::move(op.bind_data), op.returned_types, column_ids, projection_ids_old,
			    op.names, std::move(table_filters_new), op.estimated_cardinality, op.extra_info,
			    std::move(op.parameters), table_name, std::move(disable_column), projection_ids_new);
			// node->disable_columns = std::move(disable_column);

			node->dynamic_filters = op.dynamic_filters;
			if (filter) {
				filter->children.push_back(std::move(node));
				return std::move(filter);
			}
			return std::move(node);
		}
	}
}

} // namespace duckdb
