#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/transaction/transaction.hpp"

#include <utility>

namespace duckdb {

PhysicalTableScan::PhysicalTableScan(vector<LogicalType> types, TableFunction function_p,
                                     unique_ptr<FunctionData> bind_data_p, vector<LogicalType> returned_types_p,
                                     vector<column_t> column_ids_p, vector<idx_t> projection_ids_p,
                                     vector<string> names_p, unique_ptr<TableFilterSet> table_filters_p,
                                     idx_t estimated_cardinality, ExtraOperatorInfo extra_info,
                                     vector<Value> parameters_p, std::string table_name_p, vector<int> disable_columns,
                                     vector<idx_t> projection_columns)
    : PhysicalOperator(PhysicalOperatorType::TABLE_SCAN, std::move(types), estimated_cardinality),
      function(std::move(function_p)), bind_data(std::move(bind_data_p)), returned_types(std::move(returned_types_p)),
      column_ids_total(std::move(column_ids_p)), projection_ids_total(std::move(projection_ids_p)),
      names(std::move(names_p)), table_filters(std::move(table_filters_p)), extra_info(extra_info),
      parameters(std::move(parameters_p)), table_name(std::move(table_name_p)) {
	for (auto &name : names) {
		if (name == "rowid") {
			name = "rowid(" + table_name + ")";
		} else {
			name = table_name + "." + name;
		}
	}
	this->disable_columns = std::move(disable_columns);
	for (int i = 0; i < column_ids_total.size(); i++) {
		if (this->disable_columns[i] == 0) {
			this->column_ids.push_back(column_ids_total[i]);
		}
	}
	if (projection_columns.size() > 0) {
		for (int i = 0; i < projection_columns.size(); i++) {
			this->projection_ids.push_back(projection_columns[i]);
		}
	} else {
		this->projection_ids = this->projection_ids_total;
	}
	for (auto &project_col : this->projection_ids_total) {
		this->output_disable_columns.push_back(this->disable_columns[project_col]);
	}
	this->disable_columns = this->output_disable_columns;
}

class TableScanGlobalSourceState : public GlobalSourceState {
public:
	TableScanGlobalSourceState(ClientContext &context, const PhysicalTableScan &op) {
		if (op.dynamic_filters && op.dynamic_filters->HasFilters()) {
			table_filters = op.dynamic_filters->GetFinalTableFilters(op, op.table_filters.get());
		}
		if (op.function.init_global) {
			TableFunctionInitInput input(op.bind_data.get(), op.column_ids, op.projection_ids, GetTableFilters(op));
			global_state = op.function.init_global(context, input);
			if (global_state) {
				max_threads = global_state->MaxThreads();
			}
		} else {
			max_threads = 1;
		}
		if (op.function.in_out_function) {
			// this is an in-out function, we need to setup the input chunk
			vector<LogicalType> input_types;
			for (auto &param : op.parameters) {
				input_types.push_back(param.type());
			}
			input_chunk.Initialize(context, input_types);
			for (idx_t c = 0; c < op.parameters.size(); c++) {
				input_chunk.data[c].SetValue(0, op.parameters[c]);
			}
			input_chunk.SetCardinality(1);
		}
	}

	idx_t max_threads = 0;
	unique_ptr<GlobalTableFunctionState> global_state;
	bool in_out_final = false;
	DataChunk input_chunk;
	//! Combined table filters, if we have dynamic filters
	unique_ptr<TableFilterSet> table_filters;

	optional_ptr<TableFilterSet> GetTableFilters(const PhysicalTableScan &op) const {
		return table_filters ? table_filters.get() : op.table_filters.get();
	}
	idx_t MaxThreads() override {
		return max_threads;
	}
};

class TableScanLocalSourceState : public LocalSourceState {
public:
	TableScanLocalSourceState(ExecutionContext &context, TableScanGlobalSourceState &gstate,
	                          const PhysicalTableScan &op) {
		if (op.function.init_local) {
			TableFunctionInitInput input(op.bind_data.get(), op.column_ids, op.projection_ids,
			                             gstate.GetTableFilters(op));
			local_state = op.function.init_local(context, input, gstate.global_state.get());
		}
	}

	unique_ptr<LocalTableFunctionState> local_state;
};

unique_ptr<LocalSourceState> PhysicalTableScan::GetLocalSourceState(ExecutionContext &context,
                                                                    GlobalSourceState &gstate) const {
	return make_uniq<TableScanLocalSourceState>(context, gstate.Cast<TableScanGlobalSourceState>(), *this);
}

unique_ptr<GlobalSourceState> PhysicalTableScan::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<TableScanGlobalSourceState>(context, *this);
}

SourceResultType PhysicalTableScan::GetData(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSourceInput &input) const {
	D_ASSERT(!column_ids.empty());
	auto &gstate = input.global_state.Cast<TableScanGlobalSourceState>();
	auto &state = input.local_state.Cast<TableScanLocalSourceState>();
	bool mat_flag = input.materialize_flag;
	TableFunctionInput data(bind_data.get(), state.local_state.get(), gstate.global_state.get(), mat_flag,
	                        input.rowid_column_id, input.materialize_col_id, input.fixed_len_strings_columns,
	                        input.use_inverted_index);
	if (input.use_inverted_index) {
		data.inverted_index = input.inverted_index;
		data.inverted_indexnew = input.inverted_indexnew;
	}
	if (function.function) {
		function.function(context.client, data, chunk);
	} else {
		if (gstate.in_out_final) {
			function.in_out_function_final(context, data, chunk);
		}
		function.in_out_function(context, data, gstate.input_chunk, chunk);
		if (chunk.size() == 0 && function.in_out_function_final) {
			function.in_out_function_final(context, data, chunk);
			gstate.in_out_final = true;
		}
	}

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

double PhysicalTableScan::GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	if (function.table_scan_progress) {
		return function.table_scan_progress(context, bind_data.get(), gstate.global_state.get());
	}
	// if table_scan_progress is not implemented we don't support this function yet in the progress bar
	return -1;
}

idx_t PhysicalTableScan::GetBatchIndex(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                       LocalSourceState &lstate) const {
	D_ASSERT(SupportsBatchIndex());
	D_ASSERT(function.get_batch_index);
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	auto &state = lstate.Cast<TableScanLocalSourceState>();
	return function.get_batch_index(context.client, bind_data.get(), state.local_state.get(),
	                                gstate.global_state.get());
}

string PhysicalTableScan::GetName() const {
	return StringUtil::Upper(function.name + " " + function.extra_info);
}

InsertionOrderPreservingMap<string> PhysicalTableScan::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	if (function.to_string) {
		result["__text__"] = function.to_string(bind_data.get());
	} else {
		result["Function"] = StringUtil::Upper(function.name);
	}
	if (function.projection_pushdown) {
		if (function.filter_prune) {
			string projections;
			for (idx_t i = 0; i < projection_ids.size(); i++) {
				const auto &column_id = column_ids[projection_ids[i]];
				if (column_id < names.size()) {
					if (i > 0) {
						projections += "\n";
					}
					projections += names[column_id];
				}
			}
			result["Projections"] = projections;
		} else {
			string projections;
			for (idx_t i = 0; i < column_ids.size(); i++) {
				const auto &column_id = column_ids[i];
				if (column_id < names.size()) {
					if (i > 0) {
						projections += "\n";
					}
					projections += names[column_id];
				}
			}
			result["Projections"] = projections;
		}
	}

	if (function.filter_pushdown && table_filters) {
		string filters_info;
		bool first_item = true;
		for (auto &f : table_filters->filters) {
			auto &column_index = f.first;
			auto &filter = f.second;
			if (column_index < names.size()) {
				if (!first_item) {
					filters_info += "\n";
				}
				first_item = false;
				filters_info += filter->ToString(names[column_ids[column_index]]);
			}
		}
		result["Filters"] = filters_info;
	}
	if (!extra_info.file_filters.empty()) {
		result["File Filters"] = extra_info.file_filters;
		if (extra_info.filtered_files.IsValid() && extra_info.total_files.IsValid()) {
			result["Scanning Files"] = StringUtil::Format("%llu/%llu", extra_info.filtered_files.GetIndex(),
			                                              extra_info.total_files.GetIndex());
		}
	}

	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

bool PhysicalTableScan::Equals(const PhysicalOperator &other_p) const {
	if (type != other_p.type) {
		return false;
	}
	auto &other = other_p.Cast<PhysicalTableScan>();
	if (function.function != other.function.function) {
		return false;
	}
	if (column_ids != other.column_ids) {
		return false;
	}
	if (!FunctionData::Equals(bind_data.get(), other.bind_data.get())) {
		return false;
	}
	return true;
}

bool PhysicalTableScan::ParallelSource() const {
	if (!function.function) {
		// table in-out functions cannot be executed in parallel as part of a PhysicalTableScan
		// since they have only a single input row
		return false;
	}
	return true;
}

} // namespace duckdb
