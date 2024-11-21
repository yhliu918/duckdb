#include "duckdb/execution/operator/helper/physical_materialized_collector.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"

namespace duckdb {

PhysicalMaterializedCollector::PhysicalMaterializedCollector(PreparedStatementData &data, bool parallel)
    : PhysicalResultCollector(data), parallel(parallel) {
}

SinkResultType PhysicalMaterializedCollector::Sink(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<MaterializedCollectorLocalState>();
	lstate.collection->Append(lstate.append_state, chunk);
	return SinkResultType::NEED_MORE_INPUT;
}
// SinkResultType PhysicalMaterializedCollector::Sink(ExecutionContext &context, DataChunk &chunk,
//                                                    OperatorSinkInput &input) const {
// 	//! the chunk layout is:
// 	//! |probe side columns|rowid|payload|rowid|payload|...
// 	auto &lstate = input.local_state.Cast<MaterializedCollectorLocalState>();
// 	if (input.materialize_strategy_mode == 1 && input.final_materilaize) {
// 		lstate.set_output = true;
// 		if (input.materialize_column_types.size() == lstate.collection->Types().size()) {
// 			// no probe payload or rowid is copied in the previous sink
// 			lstate.collection->AppendMaterialzeNew(lstate.append_state, chunk, input.materialize_column_types.size());
// 		} else {
// 			// probe payload or rowid is copied in the previous sink
// 			lstate.collection->AppendMaterialize(lstate.append_state, chunk, input.materialize_column_types.size());
// 		}
// 	} else {
// 		int index = 0;
// 		int origin_column_count = chunk.data.size();
// 		if (input.materialize_flag) {
// 			for (auto &[colid, keep_rowid] : input.colid_keep_rowid) {
// 				if (!keep_rowid) {
// 					if (!lstate.set_output) {
// 						lstate.collection->Types().erase(lstate.collection->Types().begin() + colid);
// 					}
// 					origin_column_count--;
// 				}
// 			}
// 		}

// 		if (input.materialize_flag && !lstate.set_output && input.materialize_column_types.size() > 0) {
// 			for (auto [col, type] : input.materialize_column_types) {
// 				lstate.collection->InitializeAppend(LogicalType(LogicalTypeId(type)));
// 			}
// 			lstate.append_state.vector_data.resize(lstate.collection->Types().size());
// 			for (int i = 0; i < chunk.data.size(); i++) {
// 				if (input.colid_keep_rowid.find(i) != input.colid_keep_rowid.end()) {
// 					if (!input.colid_keep_rowid[i]) {
// 						continue;
// 					}
// 				}
// 				lstate.column_map[i] = index;
// 				index++;
// 			}

// 			lstate.set_output = true;
// 		}
// 		int fill_columns =
// 		    input.materialize_strategy_mode == 0 ? lstate.collection->Types().size() : origin_column_count;

// 		lstate.collection->Append(lstate.append_state, chunk, fill_columns, &lstate.column_map);
// 	}
// 	return SinkResultType::NEED_MORE_INPUT;
// }

SinkCombineResultType PhysicalMaterializedCollector::Combine(ExecutionContext &context,
                                                             OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<MaterializedCollectorGlobalState>();
	auto &lstate = input.local_state.Cast<MaterializedCollectorLocalState>();
	if (lstate.collection->Count() == 0) {
		return SinkCombineResultType::FINISHED;
	}

	lock_guard<mutex> l(gstate.glock);
	if (!gstate.collection) {
		gstate.collection = std::move(lstate.collection);
	} else {
		gstate.collection->Combine(*lstate.collection);
	}

	return SinkCombineResultType::FINISHED;
}

unique_ptr<GlobalSinkState> PhysicalMaterializedCollector::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<MaterializedCollectorGlobalState>();
	state->context = context.shared_from_this();
	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalMaterializedCollector::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<MaterializedCollectorLocalState>();
	state->collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	state->collection->InitializeAppend(state->append_state);
	return std::move(state);
}

unique_ptr<QueryResult> PhysicalMaterializedCollector::GetResult(GlobalSinkState &state) {
	auto &gstate = state.Cast<MaterializedCollectorGlobalState>();
	if (!gstate.collection) {
		gstate.collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	}
	auto result = make_uniq<MaterializedQueryResult>(statement_type, properties, names, std::move(gstate.collection),
	                                                 gstate.context->GetClientProperties());
	return std::move(result);
}

bool PhysicalMaterializedCollector::ParallelSink() const {
	return parallel;
}

bool PhysicalMaterializedCollector::SinkOrderDependent() const {
	return true;
}

} // namespace duckdb
