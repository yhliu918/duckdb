#include "duckdb/parallel/pipeline_executor.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/execution/operator/helper/physical_materialized_collector.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/main/client_context.hpp"
#include "json.hpp"

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sys/time.h>
#include <thread>
#include <unistd.h>

#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
#include <chrono>
#include <thread>
#endif
using json = nlohmann::json;
namespace duckdb {
class TableScanLocalState;
class TableScanLocalSourceState;
double getNow() {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}
bool PipelineExecutor::parse_materialize_config(Pipeline &pipeline_p, bool read_mat_info) {
	std::ifstream file("/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/config/pipeline" +
	                       std::to_string(pipeline_p.pipeline_id),
	                   std::ios::in);
	int materialize_strategy_mode = 0;
	if (file.is_open()) {
		// basic config

		bool push_source;
		int chunk_queue_threshold;

		file >> materialize_strategy_mode >> push_source >> chunk_queue_threshold;
		pipeline_p.push_source = bool(push_source);
		pipeline_p.chunk_queue_threshold = chunk_queue_threshold;

		// materialize info
		if (read_mat_info) {
			int groups_of_info;
			file >> groups_of_info;
			for (int i = 0; i < groups_of_info; i++) {
				unordered_map<int64_t, int64_t> col_map;
				map<int64_t, int8_t> col_types;
				unordered_map<int64_t, int32_t> fixed_len_strings_columns;
				int rowid_col_idx;
				int keep_rowid;
				idx_t table_col_idx;
				idx_t result_col_idx;
				std::string mat_key_name;
				std::string rowid_name;
				int dependent_pipeline;
				int logical_type;
				int string_length = 0;
				int lines = 0;
				int table_size;
				file >> dependent_pipeline >> keep_rowid >> lines >> rowid_name >> table_size;
				auto target_op = pipeline_p.operators.size() == 0
				                     ? pipeline_p.source
				                     : pipeline_p.operators[pipeline_p.operators.size() - 1].get();
				auto &chunk_layout = target_op->names;
				if (target_op->column_to_entry.size() > 0) {
					chunk_layout = target_op->entry_names;
				}
				rowid_col_idx = std::find(chunk_layout.begin(), chunk_layout.end(), rowid_name) - chunk_layout.begin();

				for (int j = 0; j < lines; j++) {
					file >> mat_key_name >> table_col_idx >> logical_type;
					result_col_idx =
					    std::find(chunk_layout.begin(), chunk_layout.end(), mat_key_name) - chunk_layout.begin();
					col_map[table_col_idx] = result_col_idx;
					col_types[result_col_idx] = logical_type;
					if (LogicalTypeId(logical_type) == LogicalTypeId::VARCHAR) {
						file >> string_length;
						fixed_len_strings_columns[result_col_idx] = string_length;
					}
				}
				if (!col_map.empty()) {
					pipeline_p.SetMaterializeMap(materialize_strategy_mode, rowid_col_idx, dependent_pipeline,
					                             keep_rowid, col_map, col_types, fixed_len_strings_columns, table_size);
				}
			}
		}
		file.close();
	}
	return materialize_strategy_mode;
}
void PipelineExecutor::dump_pipeline_info(Pipeline &pipeline_p) {
	std::unordered_set<std::string> must_enable_columns_when_start;
	std::unordered_set<std::string> must_enable_columns_when_end;
	json j;
	int op_index = 0;
	for (auto &op : pipeline_p.GetOperators()) {
		if (op.get().type == PhysicalOperatorType::TABLE_SCAN) {
			auto &table_scan = op.get().Cast<PhysicalTableScan>();
			for (auto &name : table_scan.must_enables_left) {
				must_enable_columns_when_start.insert(name);
			}
		}
		if (op.get().type == PhysicalOperatorType::HASH_JOIN) {
			if (&op.get() != pipeline_p.GetSink().get()) {
				//! is probe side
				must_enable_columns_when_start.insert(op.get().must_enables_left.begin(),
				                                      op.get().must_enables_left.end());
			} else {
				//! is build side
				must_enable_columns_when_end.insert(op.get().must_enables_right.begin(),
				                                    op.get().must_enables_right.end());
			}
		}
		if (op.get().type == PhysicalOperatorType::FILTER) {
			must_enable_columns_when_start.insert(op.get().must_enables_left.begin(), op.get().must_enables_left.end());
		}
		int operator_index = 0;
		if (op.get().operator_index) {
			operator_index = op.get().operator_index;
		}
		j["operators"][op_index]["name"] = op.get().GetName();
		for (auto &name : op.get().names) {
			j["operators"][op_index]["op_index"] = operator_index;
			j["operators"][op_index]["names"].push_back(name);
		}
		op_index++;
	}
	auto sink = pipeline_p.GetSink();
	if (sink && (sink->type == PhysicalOperatorType::RESULT_COLLECTOR ||
	             sink->type == PhysicalOperatorType::UNGROUPED_AGGREGATE ||
	             sink->type == PhysicalOperatorType::ORDER_BY || sink->type == PhysicalOperatorType::HASH_GROUP_BY)) {
		auto &last_op = pipeline_p.operators.empty() ? *pipeline_p.source : pipeline_p.operators.back().get();
		for (int i = 0; i < last_op.names.size(); i++) {
			must_enable_columns_when_end.insert(last_op.names[i]);
		}
	}
	for (auto &col : must_enable_columns_when_start) {
		j["must_enable_columns_start"].push_back(col);
	}
	for (auto &col : must_enable_columns_when_end) {
		j["must_enable_columns_end"].push_back(col);
	}
	j["pipeline_id"] = pipeline_p.pipeline_id;
	j["parent"] = pipeline_p.parent;
	auto source = pipeline_p.GetSource();
	if (source && source->type == PhysicalOperatorType::TABLE_SCAN) {
		j["table"] = pipeline_p.GetSource()->Cast<PhysicalTableScan>().table_name;
	}

	std::ofstream file("/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/query/pipeline" +
	                       std::to_string(pipeline_p.pipeline_id) + ".json",
	                   std::ios::out);
	if (file.is_open()) {
		file << j.dump(4);
		file.close();
	}
}

PipelineExecutor::PipelineExecutor(ClientContext &context_p, Pipeline &pipeline_p)
    : pipeline(pipeline_p), thread(context_p), context(context_p, thread, &pipeline_p) {
	D_ASSERT(pipeline.source_state);
	int num = 0;
	if (pipeline.pipeline_id && ((pipeline.GetSource()->type == PhysicalOperatorType::TABLE_SCAN &&
	                              pipeline.GetSink()->type != PhysicalOperatorType::CREATE_TABLE_AS) ||
	                             pipeline.GetSink()->type == PhysicalOperatorType::ORDER_BY)) {
		bool dump = false;
		const char *dump_env = std::getenv("DUMP_PIPELINE_INFO");
		if (dump_env != nullptr) {
			int dump_value = std::atoi(dump_env);
			dump = (dump_value == 1);
		}
		if (dump) {
			// std::cout << "Dumping pipeline info" << std::endl;
			dump_pipeline_info(pipeline);
		}

		// std::cout << pipeline.pipeline_id << " " << pipeline.parent << " " << pipeline.ToString() << std::endl;
	}
	bool mat_mode = parse_materialize_config(pipeline, false);
	if (pipeline.sink) {
		local_sink_state = pipeline.sink->GetLocalSinkState(context);
		requires_batch_index = pipeline.sink->RequiresBatchIndex() && pipeline.source->SupportsBatchIndex();
		if (requires_batch_index) {
			auto &partition_info = local_sink_state->partition_info;
			D_ASSERT(!partition_info.batch_index.IsValid());
			// batch index is not set yet - initialize before fetching anything
			partition_info.batch_index = pipeline.RegisterNewBatchIndex();
			partition_info.min_batch_index = partition_info.batch_index;
		}
		if (pipeline.sink->type == PhysicalOperatorType::HASH_JOIN ||
		    pipeline.sink->type == PhysicalOperatorType::RESULT_COLLECTOR) {
			std::ifstream file("/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/pipeline_dop" +
			                       std::to_string(pipeline.pipeline_id),
			                   std::ios::in);
			if (file.is_open()) {
				file >> num;
				file.close();
			}
			pipeline.thread_num = num;
		}
	}
	pipeline.mat_lock.lock();
	for (int i = 0; i < pipeline.operators.size(); i++) {
		pipeline.operator_state_set.push_back(0);
	}
	if (pipeline.operator_total_time.empty()) {
		pipeline.operator_total_time.reserve(pipeline.operators.size() + 1);
		for (idx_t i = 0; i < pipeline.operators.size() + 1; i++) {
			pipeline.operator_total_time.push_back(0);
		}
	}
	if (!pipeline.config_parsed) {
		parse_materialize_config(pipeline, true);
		pipeline.config_parsed = true;
	}
	pipeline.mat_lock.unlock();

	local_source_state = pipeline.source->GetLocalSourceState(context, *pipeline.source_state);
	// (*local_source_state).Cast<TableScanLocalSourceState>().local_state.get()->Cast<TableScanLocalState>().scan_state;

	intermediate_chunks.reserve(pipeline.operators.size());
	intermediate_states.reserve(pipeline.operators.size());
	for (idx_t i = 0; i < pipeline.operators.size(); i++) {
		if (pipeline.operators[i].get().type == PhysicalOperatorType::HASH_JOIN) {
			auto &state = pipeline.operators[i].get().sink_state->Cast<HashJoinGlobalSinkState>();
			pipeline.mat_lock.lock();
			if (state.mat_table && state.source_state) {
				if (pipeline.operator_state_set[i] == 0) {
					pipeline.operator_state_set[i] = 1;
					pipeline.SetMaterializeSource(state.pipe_id, move(state.mat_table), state.source,
					                              move(state.source_state), move(state.local_source_state));
					if (state.materialize_sources.size() > 0) {
						for (auto &source : state.materialize_sources) {
							pipeline.SetMaterializeSource(source.first, move(source.second.table),
							                              source.second.materialize_source,
							                              move(source.second.materialize_source_state),
							                              move(source.second.materialize_local_source_state));
						}
					}
				}
			}
			pipeline.mat_lock.unlock();
		}
		auto &prev_operator = i == 0 ? *pipeline.source : pipeline.operators[i - 1].get();
		auto &current_operator = pipeline.operators[i].get();

		auto chunk = make_uniq<DataChunk>();
		chunk->Initialize(Allocator::Get(context.client), prev_operator.GetTypes());
		chunk->disable_columns = prev_operator.disable_columns;

		intermediate_chunks.push_back(std::move(chunk));

		auto op_state = current_operator.GetOperatorState(context);
		intermediate_states.push_back(std::move(op_state));

		if (current_operator.IsSink() && current_operator.sink_state->state == SinkFinalizeType::NO_OUTPUT_POSSIBLE) {
			// one of the operators has already figured out no output is possible
			// we can skip executing the pipeline
			FinishProcessing();
		}
	}

	if (pipeline.materialize_strategy_mode == 1) {
		int row_id_column_number = pipeline.materialize_maps.size();
		for (auto &[rowid_col_idx, mat_map] : pipeline.materialize_maps) {
			int reserve_size = 0;
			if (mat_map.source_pipeline_id == pipeline.pipeline_id) {
				// reserve_size = pipeline.source.get()
				//                        ->Cast<PhysicalTableScan>()
				//                        .bind_data->Cast<TableScanBindData>()
				//                        .table.GetDataTable()
				//                        ->GetRowGroupCollection()
				//                        ->GetTotalRows() /
				//                    STANDARD_ROW_GROUPS_SIZE +
				//                1;
				reserve_size = mat_map.table_size / STANDARD_ROW_GROUPS_SIZE + 1;
			} else {
				reserve_size = pipeline.materialize_sources[mat_map.source_pipeline_id].table->GetTotalRows() /
				                   STANDARD_ROW_GROUPS_SIZE +
				               1;
			}

			inverted_indexnew[rowid_col_idx].index_.resize(reserve_size);
			for (auto &[colid, type] : mat_map.materialize_column_types) {
				pipeline.final_materialize_column_types[colid] = type;
			}
			result_index[rowid_col_idx] = 0;
		}
	}
	int source_chunk_num = pipeline.chunk_queue_threshold > 0 ? pipeline.chunk_queue_threshold : 1;
	for (int i = 0; i < source_chunk_num; i++) {
		auto source_chunk = make_uniq<DataChunk>();
		source_chunk->Initialize(Allocator::Get(context.client), pipeline.source->GetTypes());
		source_chunk->disable_columns = pipeline.source->disable_columns;

		source_chunks.push_back(std::move(source_chunk));
	}
	InitializeChunk(final_chunk);
	if (pipeline.materialize_strategy_mode > 0) {
		for (int i = 0; i < pipeline.chunk_queue_threshold; i++) {
			auto chunk = make_uniq<DataChunk>();
			chunk->Initialize(Allocator::Get(context.client), final_chunk.GetTypes());
			chunk->disable_columns = final_chunk.disable_columns;

			final_chunks.push_back(std::move(chunk));

			auto chunk_new = make_uniq<DataChunk>();
			chunk_new->Initialize(Allocator::Get(context.client), final_chunk.GetTypes());
			chunk_new->disable_columns = final_chunk.disable_columns;
			chunk_queue_ptr.push_back(std::move(chunk_new));
		}
	}
}

bool PipelineExecutor::TryFlushCachingOperators() {
	if (!started_flushing) {
		// Remainder of this method assumes any in process operators are from flushing
		D_ASSERT(in_process_operators.empty());
		started_flushing = true;
		flushing_idx = IsFinished() ? idx_t(finished_processing_idx) : 0;
	}

	// Go over each operator and keep flushing them using `FinalExecute` until empty
	while (flushing_idx < pipeline.operators.size()) {
		if (!pipeline.operators[flushing_idx].get().RequiresFinalExecute()) {
			flushing_idx++;
			continue;
		}

		// This slightly awkward way of increasing the flushing idx is to make the code re-entrant: We need to call
		// this method again in the case of a Sink returning BLOCKED.
		if (!should_flush_current_idx && in_process_operators.empty()) {
			should_flush_current_idx = true;
			flushing_idx++;
			continue;
		}

		auto &curr_chunk =
		    flushing_idx + 1 >= intermediate_chunks.size() ? final_chunk : *intermediate_chunks[flushing_idx + 1];
		auto &current_operator = pipeline.operators[flushing_idx].get();

		OperatorFinalizeResultType finalize_result;
		OperatorResultType push_result;

		if (in_process_operators.empty()) {
			curr_chunk.Reset();
			StartOperator(current_operator);
			finalize_result = current_operator.FinalExecute(context, curr_chunk, *current_operator.op_state,
			                                                *intermediate_states[flushing_idx]);
			EndOperator(current_operator, &curr_chunk);
		} else {
			// Reset flag and reflush the last chunk we were flushing.
			finalize_result = OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
		}

		push_result = ExecutePushInternal(curr_chunk, flushing_idx + 1);

		if (finalize_result == OperatorFinalizeResultType::HAVE_MORE_OUTPUT) {
			should_flush_current_idx = true;
		} else {
			should_flush_current_idx = false;
		}

		if (push_result == OperatorResultType::BLOCKED) {
			remaining_sink_chunk = true;
			return false;
		} else if (push_result == OperatorResultType::FINISHED) {
			break;
		}
	}
	return true;
}

SinkNextBatchType PipelineExecutor::NextBatch(duckdb::DataChunk &source_chunk) {
	D_ASSERT(requires_batch_index);
	idx_t next_batch_index;
	auto max_batch_index = pipeline.base_batch_index + PipelineBuildState::BATCH_INCREMENT - 1;
	if (source_chunk.size() == 0) {
		// set it to the maximum valid batch index value for the current pipeline
		next_batch_index = max_batch_index;
	} else {
		auto batch_index =
		    pipeline.source->GetBatchIndex(context, source_chunk, *pipeline.source_state, *local_source_state);
		// we start with the base_batch_index as a valid starting value. Make sure that next batch is called below
		next_batch_index = pipeline.base_batch_index + batch_index + 1;
		if (next_batch_index >= max_batch_index) {
			throw InternalException("Pipeline batch index - invalid batch index %llu returned by source operator",
			                        batch_index);
		}
	}
	auto &partition_info = local_sink_state->partition_info;
	if (next_batch_index == partition_info.batch_index.GetIndex()) {
		// no changes, return
		return SinkNextBatchType::READY;
	}
	// batch index has changed - update it
	if (partition_info.batch_index.GetIndex() > next_batch_index) {
		throw InternalException(
		    "Pipeline batch index - gotten lower batch index %llu (down from previous batch index of %llu)",
		    next_batch_index, partition_info.batch_index.GetIndex());
	}
#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
	if (debug_blocked_next_batch_count < debug_blocked_target_count) {
		debug_blocked_next_batch_count++;

		auto &callback_state = interrupt_state;
		std::thread rewake_thread([callback_state] {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
			callback_state.Callback();
		});
		rewake_thread.detach();

		return SinkNextBatchType::BLOCKED;
	}
#endif
	auto current_batch = partition_info.batch_index.GetIndex();
	partition_info.batch_index = next_batch_index;
	OperatorSinkNextBatchInput next_batch_input {*pipeline.sink->sink_state, *local_sink_state, interrupt_state};
	// call NextBatch before updating min_batch_index to provide the opportunity to flush the previous batch
	auto next_batch_result = pipeline.sink->NextBatch(context, next_batch_input);

	if (next_batch_result == SinkNextBatchType::BLOCKED) {
		partition_info.batch_index = current_batch; // set batch_index back to what it was before
		return SinkNextBatchType::BLOCKED;
	}

	partition_info.min_batch_index = pipeline.UpdateBatchIndex(current_batch, next_batch_index);

	return SinkNextBatchType::READY;
}

PipelineExecuteResult PipelineExecutor::Execute(idx_t max_chunks) {
	D_ASSERT(pipeline.sink);

	for (idx_t i = 0; i < max_chunks; i++) {
		auto &source_chunk = pipeline.operators.empty() ? final_chunk : *source_chunks[chunk_counter];
		double start = getNow();
		if (context.client.interrupted) {
			throw InterruptException();
		}

		OperatorResultType result;
		if (exhausted_source && done_flushing && !remaining_sink_chunk && !next_batch_blocked &&
		    in_process_operators.empty()) {
			break;
		} else if (remaining_sink_chunk) {
			// The pipeline was interrupted by the Sink. We should retry sinking the final chunk.
			result = ExecutePushInternal(final_chunk);
			remaining_sink_chunk = false;
		} else if (!in_process_operators.empty() && !started_flushing) {
			// The pipeline was interrupted by the Sink when pushing a source chunk through the pipeline. We need to
			// re-push the same source chunk through the pipeline because there are in_process operators, meaning
			// that the result for the pipeline
			D_ASSERT(source_chunk.size() > 0);
			result = ExecutePushInternal(source_chunk);
		} else if (exhausted_source && !next_batch_blocked && !done_flushing) {
			// The source was exhausted, try flushing all operators
			auto flush_completed = TryFlushCachingOperators();
			if (flush_completed) {
				done_flushing = true;
				break;
			} else {
				return PipelineExecuteResult::INTERRUPTED;
			}
		} else if (!exhausted_source || next_batch_blocked) {
			SourceResultType source_result;
			if (!next_batch_blocked) {
				// "Regular" path: fetch a chunk from the source and push it through the pipeline

				source_chunk.Reset();
				source_result = FetchFromSource(source_chunk);
				if (source_result == SourceResultType::BLOCKED) {
					return PipelineExecuteResult::INTERRUPTED;
				}
				if (source_result == SourceResultType::FINISHED) {
					exhausted_source = true;
				}
			}

			if (requires_batch_index) {
				auto next_batch_result = NextBatch(source_chunk);
				next_batch_blocked = next_batch_result == SinkNextBatchType::BLOCKED;
				if (next_batch_blocked) {
					return PipelineExecuteResult::INTERRUPTED;
				}
			}

			if (exhausted_source && source_chunk.size() == 0) {
				// To ensure that we're not early-terminating the pipeline
				continue;
			}

			result = ExecutePushInternal(source_chunk);
		} else {
			throw InternalException("Unexpected state reached in pipeline executor");
		}

		// SINK INTERRUPT
		if (result == OperatorResultType::BLOCKED) {
			remaining_sink_chunk = true;
			return PipelineExecuteResult::INTERRUPTED;
		}

		if (result == OperatorResultType::FINISHED) {
			break;
		}
		double end = getNow();
		pipeline.total_time += end - start;
	}

	if ((!exhausted_source || !done_flushing) && !IsFinished()) {
		return PipelineExecuteResult::NOT_FINISHED;
	}

	return PushFinalize();
}

bool PipelineExecutor::RemainingSinkChunk() const {
	return remaining_sink_chunk;
}

PipelineExecuteResult PipelineExecutor::Execute() {
	return Execute(NumericLimits<idx_t>::Maximum());
}

OperatorResultType PipelineExecutor::ExecutePush(DataChunk &input) { // LCOV_EXCL_START
	return ExecutePushInternal(input);
} // LCOV_EXCL_STOP

void PipelineExecutor::FinishProcessing(int32_t operator_idx) {
	finished_processing_idx = operator_idx < 0 ? NumericLimits<int32_t>::Maximum() : operator_idx;
	in_process_operators = stack<idx_t>();

	if (pipeline.GetSource()) {
		auto guard = pipeline.source_state->Lock();
		pipeline.source_state->PreventBlocking(guard);
		pipeline.source_state->UnblockTasks(guard);
	}
	if (pipeline.GetSink()) {
		auto guard = pipeline.GetSink()->sink_state->Lock();
		pipeline.GetSink()->sink_state->PreventBlocking(guard);
		pipeline.GetSink()->sink_state->UnblockTasks(guard);
	}
}

bool PipelineExecutor::IsFinished() {
	return finished_processing_idx >= 0;
}

void PipelineExecutor::FlushQueuedChunks() {
	assert(pipeline.materialize_strategy_mode == 1);
	auto &last_op = pipeline.operators.empty() ? *pipeline.source : pipeline.operators.back().get();
	int origin_output_count = last_op.GetTypes().size();

	vector<LogicalType> types;
	std::unordered_map<int, int> mat_col_write_idx;
	for (auto &[rowid_col_idx, mat_map] : pipeline.materialize_maps) {
		for (auto &[col_idx, result_col_index] : mat_map.materialize_column_ids) {
			types.push_back(chunk_queue_ptr[0]->data[result_col_index].GetType());
			mat_col_write_idx[result_col_index] = types.size() - 1;
		}
	}
	DataChunk mat_chunk;
	mat_chunk.Initialize(Allocator::DefaultAllocator(), types, result_index.begin()->second);
	unordered_map<int, int> mat_result_write_back;
	unordered_map<int, bool> colid_keep_rowid;
	for (auto &[rowid_col_idx, mat_map] : pipeline.materialize_maps) {
		colid_keep_rowid[rowid_col_idx] = mat_map.keep_rowid;
		unordered_map<int64_t, int64_t> materialize_column_ids_new;
		unordered_map<int64_t, int32_t> fixed_len_strings_columns_new;
		for (auto &[col_idx, result_col_index] : mat_map.materialize_column_ids) {
			int write_col_idx = mat_col_write_idx[result_col_index];
			materialize_column_ids_new[col_idx] = write_col_idx;
			if (mat_map.fixed_len_strings_columns.find(result_col_index) != mat_map.fixed_len_strings_columns.end()) {
				fixed_len_strings_columns_new[write_col_idx] = mat_map.fixed_len_strings_columns[result_col_index];
			}
			mat_result_write_back[result_col_index] = write_col_idx;
		}
		if (mat_map.source_pipeline_id == pipeline.pipeline_id) {
			OperatorSourceInput source_input = {*pipeline.source_state,
			                                    *local_source_state,
			                                    interrupt_state,
			                                    true,
			                                    rowid_col_idx,
			                                    materialize_column_ids_new,
			                                    fixed_len_strings_columns_new,
			                                    true,
			                                    nullptr,
			                                    &inverted_indexnew[rowid_col_idx]};

			auto res = pipeline.source->GetData(context, mat_chunk, source_input);
		} else {
			auto &mat_source = pipeline.materialize_sources[mat_map.source_pipeline_id];

			OperatorSourceInput source_input = {*mat_source.materialize_source_state,
			                                    *mat_source.materialize_local_source_state,
			                                    interrupt_state,
			                                    true,
			                                    rowid_col_idx,
			                                    materialize_column_ids_new,
			                                    fixed_len_strings_columns_new,
			                                    true,
			                                    nullptr,
			                                    &inverted_indexnew[rowid_col_idx]};

			auto res = mat_source.materialize_source->GetData(context, mat_chunk, source_input);
		}
	}
	mat_chunk.SetCardinality(result_index.begin()->second);

	// slice the mat_chunk into these chunks
	int start = 0;
	for (auto &chunk : chunk_queue_ptr) {
		// chunk->disable_columns = last_op.output_disable_columns;
		for (auto &[chunk_col_idx, mat_col_idx] : mat_result_write_back) {
			chunk->data[chunk_col_idx].Slice(mat_chunk.data[mat_col_idx], start, start + chunk->size());
		}
		start += chunk->size();
	}
}

void PipelineExecutor::InplaceMaterializeChunk(DataChunk &chunk) {
	assert(pipeline.materialize_strategy_mode == 2 || pipeline.materialize_strategy_mode == 0);
	for (auto &[rowid_col_idx, mat_map] : pipeline.materialize_maps) {
		if (mat_map.source_pipeline_id == pipeline.pipeline_id) {
			OperatorSourceInput source_input = {*pipeline.source_state,
			                                    *local_source_state,
			                                    interrupt_state,
			                                    true,
			                                    rowid_col_idx,
			                                    mat_map.materialize_column_ids,
			                                    mat_map.fixed_len_strings_columns,
			                                    false};

			auto res = pipeline.source->GetData(context, chunk, source_input);
		} else {
			auto &mat_source = pipeline.materialize_sources[mat_map.source_pipeline_id];

			OperatorSourceInput source_input = {*mat_source.materialize_source_state,
			                                    *mat_source.materialize_local_source_state,
			                                    interrupt_state,
			                                    true,
			                                    rowid_col_idx,
			                                    mat_map.materialize_column_ids,
			                                    mat_map.fixed_len_strings_columns,
			                                    false};

			auto res = mat_source.materialize_source->GetData(context, chunk, source_input);
		}
	}
}

OperatorResultType PipelineExecutor::ExecutePushInternal(DataChunk &input, idx_t initial_idx) {
	D_ASSERT(pipeline.sink);
	if (input.size() == 0) { // LCOV_EXCL_START
		return OperatorResultType::NEED_MORE_INPUT;
	} // LCOV_EXCL_STOP

	// this loop will continuously push the input chunk through the pipeline as long as:
	// - the OperatorResultType for the Execute is HAVE_MORE_OUTPUT
	// - the Sink doesn't block

	while (true) {
		OperatorResultType result;
		// Note: if input is the final_chunk, we don't do any executing, the chunk just needs to be sinked
		auto &final_chunk_real = final_chunks.size() > 0 ? *final_chunks[chunk_counter] : final_chunk;
		if (&input != &final_chunk_real && !pipeline.operators.empty()) {
			final_chunk_real.Reset();
			result = Execute(input, final_chunk_real, initial_idx);
			if (result == OperatorResultType::FINISHED) {
				return OperatorResultType::FINISHED;
			}
		} else {
			result = OperatorResultType::NEED_MORE_INPUT;
		}
		auto &sink_chunk = pipeline.operators.empty() ? input : final_chunk_real;
		if (sink_chunk.size() > 0) {
			StartOperator(*pipeline.sink);
			D_ASSERT(pipeline.sink);
			D_ASSERT(pipeline.sink->sink_state);
			OperatorSinkInput sink_input {*pipeline.sink->sink_state, *local_sink_state, interrupt_state};

			SinkResultType sink_result = SinkResultType::NEED_MORE_INPUT;

			double materialize_start = getNow();
			if (pipeline.materialize_strategy_mode == 1) {
				chunk_queue_ptr[chunk_counter]->Reset();
				sink_chunk.Copy(*chunk_queue_ptr[chunk_counter]);
				// chunk_queue_ptr[chunk_counter]->Copy(sink_chunk);
				// chunk_queue.emplace_back(&sink_chunk);
				chunk_counter++;
				// build inverted index
				for (auto &[rowid_col_idx, mat_map] : pipeline.materialize_maps) {
					auto sel_vec = sink_chunk.data[rowid_col_idx];
					auto buffer = sink_chunk.data[rowid_col_idx].GetBuffer().get();

					SelectionVector *sel_index;
					bool use_sel_index =
					    buffer != nullptr && buffer->GetBufferType() == VectorBufferType::DICTIONARY_BUFFER;
					if (use_sel_index) {
						DictionaryBuffer *dict_buffer = static_cast<DictionaryBuffer *>(buffer);
						sel_index = &dict_buffer->GetSelVector();
					}

					switch (sel_vec.GetType().id()) {
					case LogicalTypeId::BIGINT: {
						D_ASSERT(sel_vec.GetType().id() == LogicalTypeId::BIGINT);

						int64_t *sel = reinterpret_cast<int64_t *>(sel_vec.GetData());
						int &index = result_index[rowid_col_idx];
						for (int64_t i = 0; i < sink_chunk.size(); i++) {
							// auto rowid = sel[i];
							auto rowid = use_sel_index ? sel[sel_index->get_index(i)] : sel[i];
							// std::cout << sel_index->get_index(i) << " " << rowid << std::endl;
							inverted_indexnew[rowid_col_idx].insert(rowid, index++);
						}
						break;
					}
					case LogicalTypeId::INTEGER: {
						D_ASSERT(sel_vec.GetType().id() == LogicalTypeId::INTEGER);

						int32_t *sel = reinterpret_cast<int32_t *>(sel_vec.GetData());
						int &index = result_index[rowid_col_idx];
						for (int64_t i = 0; i < sink_chunk.size(); i++) {
							// auto rowid = sel[i];
							int64_t rowid = use_sel_index ? sel[sel_index->get_index(i)] : sel[i];
							// std::cout << sel_index->get_index(i) << " " << rowid << std::endl;
							inverted_indexnew[rowid_col_idx].insert(rowid, index++);
						}
						break;
					}

					default:
						break;
					}
				}
			}
			double materialize_end = getNow();
			pipeline.map_building_time += materialize_end - materialize_start;

			//! materialize the current chunk queue and sink them all.
			if (pipeline.materialize_strategy_mode == 1 && chunk_counter >= pipeline.chunk_queue_threshold) {
				double materialize_start = getNow();
				// materialize the inverted index columns
				if (result_index.begin()->second > 0) {
					FlushQueuedChunks();
				}
				double materialize_end = getNow();
				pipeline.mat_operator_time += materialize_end - materialize_start;

				// sink all the chunks
				for (auto &chunk : chunk_queue_ptr) {
					OperatorSinkInput sink_input {*pipeline.sink->sink_state,
					                              *local_sink_state,
					                              interrupt_state,
					                              pipeline.materialize_flag,
					                              pipeline.final_materialize_column_types,
					                              pipeline.materialize_strategy_mode,
					                              false};
					if (pipeline.materialize_flag) {
						for (auto &[rowid, mat_map] : pipeline.materialize_maps) {
							sink_input.colid_keep_rowid[rowid] = mat_map.keep_rowid;
						}
					}
					double sink_start = getNow();
					sink_result = Sink(*chunk, sink_input);
					total_materialized_chunks++;
					total_materialized_rows += chunk->size();
					double sink_end = getNow();
					pipeline.incrementOperatorTime(sink_end - sink_start, pipeline.operators.size());
				}

				// chunk_queue.clear();
				chunk_counter = 0;
				for (auto it = inverted_indexnew.begin(); it != inverted_indexnew.end(); it++) {
					for (auto it2 = it->second.index_.begin(); it2 != it->second.index_.end(); it2++) {
						it2->clear();
					}
				}
				for (auto &[rowid_col_idx, mat_map] : pipeline.materialize_maps) {
					result_index[rowid_col_idx] = 0;
				}
			}

			//! TODO: Add materialize strategy 2

			if (pipeline.materialize_strategy_mode == 2) {
				// materialize the chunk in place
				double materialize_start = getNow();
				InplaceMaterializeChunk(sink_chunk);
				double materialize_end = getNow();
				pipeline.mat_operator_time += materialize_end - materialize_start;
			}

			if (!pipeline.materialize_flag || pipeline.materialize_strategy_mode != 1) {
				double sink_start = getNow();
				sink_result = Sink(sink_chunk, sink_input);
				total_materialized_chunks++;
				total_materialized_rows += sink_chunk.size();
				double sink_end = getNow();
				pipeline.incrementOperatorTime(sink_end - sink_start, pipeline.operators.size());
			}
			EndOperator(*pipeline.sink, nullptr);

			if (sink_result == SinkResultType::BLOCKED) {
				return OperatorResultType::BLOCKED;
			} else if (sink_result == SinkResultType::FINISHED) {
				FinishProcessing();
				return OperatorResultType::FINISHED;
			}
		}
		if (result == OperatorResultType::NEED_MORE_INPUT) {
			return OperatorResultType::NEED_MORE_INPUT;
		}
	}
}

PipelineExecuteResult PipelineExecutor::PushFinalize() {
	if (finalized) {
		throw InternalException("Calling PushFinalize on a pipeline that has been finalized already");
	}

	D_ASSERT(local_sink_state);
	if (chunk_counter > 0 && pipeline.materialize_strategy_mode == 1) {
		double materialize_start = getNow();

		// materialize the inverted index columns
		if (result_index.begin()->second > 0) {
			FlushQueuedChunks();
		}
		double materialize_end = getNow();
		pipeline.mat_operator_time += materialize_end - materialize_start;

		// sink all the chunks
		for (int i = 0; i < chunk_counter; i++) {
			auto &chunk = chunk_queue_ptr[i];
			// std::cout << "chunk size: " << chunk->size() << std::endl;
			OperatorSinkInput sink_input {*pipeline.sink->sink_state,
			                              *local_sink_state,
			                              interrupt_state,
			                              pipeline.materialize_flag,
			                              pipeline.final_materialize_column_types,
			                              pipeline.materialize_strategy_mode,
			                              false};
			if (pipeline.materialize_flag) {
				for (auto &[rowid, mat_map] : pipeline.materialize_maps) {
					sink_input.colid_keep_rowid[rowid] = mat_map.keep_rowid;
				}
			}
			double sink_start = getNow();
			auto sink_result = Sink(*chunk, sink_input);
			total_materialized_chunks++;
			total_materialized_rows += chunk->size();
			double sink_end = getNow();
			pipeline.incrementOperatorTime(sink_end - sink_start, pipeline.operators.size());
		}

		// chunk_queue.clear();
		chunk_counter = 0;
		for (auto it = inverted_indexnew.begin(); it != inverted_indexnew.end(); it++) {
			for (auto it2 = it->second.index_.begin(); it2 != it->second.index_.end(); it2++) {
				it2->clear();
			}
		}
		for (auto &[rowid_col_idx, mat_map] : pipeline.materialize_maps) {
			result_index[rowid_col_idx] = 0;
		}
	}

	OperatorSinkCombineInput combine_input {*pipeline.sink->sink_state, *local_sink_state, interrupt_state};

#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
	if (debug_blocked_combine_count < debug_blocked_target_count) {
		debug_blocked_combine_count++;

		auto &callback_state = combine_input.interrupt_state;
		std::thread rewake_thread([callback_state] {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
			callback_state.Callback();
		});
		rewake_thread.detach();

		return PipelineExecuteResult::INTERRUPTED;
	}
#endif
	auto result = pipeline.sink->Combine(context, combine_input);

	if (result == SinkCombineResultType::BLOCKED) {
		return PipelineExecuteResult::INTERRUPTED;
	}

	finalized = true;
	// flush all query profiler info
	for (idx_t i = 0; i < intermediate_states.size(); i++) {
		intermediate_states[i]->Finalize(pipeline.operators[i].get(), context);
	}
	pipeline.mat_lock.lock();
	if (pipeline.sink->type == PhysicalOperatorType::HASH_JOIN && pipeline.push_source && pipeline.pipeline_id != 0) {
		pipeline.thread_num--;
		if (pipeline.thread_num == 0) {
			pipeline.sink->sink_state->Cast<HashJoinGlobalSinkState>().SetSource(
			    pipeline.pipeline_id,
			    pipeline.source.get()
			        ->Cast<PhysicalTableScan>()
			        .bind_data->Cast<TableScanBindData>()
			        .table.GetDataTable()
			        ->GetRowGroupCollection(),
			    pipeline.source, move(pipeline.source_state), move(local_source_state));
			pipeline.sink->sink_state->Cast<HashJoinGlobalSinkState>().SetMaterializeSource(
			    std::move(pipeline.materialize_sources));
		}
	}

	bool print = pipeline.operators.size() > 0 || pipeline.sink->type != PhysicalOperatorType::CREATE_TABLE_AS;
	bool dump_statistic = false;
	if (dump_statistic) {
		if (pipeline.sink->type == PhysicalOperatorType::HASH_JOIN) {
			std::ofstream out(
			    "/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/payload_hash_table_build_time.txt",
			    std::ios::app);
			out << pipeline.operator_total_time[pipeline.operator_total_time.size() - 1] << std::endl;
		}
	}
	print = false;
	if (print) {
		std::cout << "----------------------------" << std::endl;
		for (int i = 0; i < pipeline.operator_total_time.size() - 1; i++) {
			std::cout << "Operator " << PhysicalOperatorToString(pipeline.operators[i].get().type)
			          << " time: " << pipeline.operator_total_time[i] << std::endl;
		}
		if (pipeline.sink) {
			std::cout << "Sink operator " << PhysicalOperatorToString(pipeline.sink.get()->type)
			          << " time: " << pipeline.operator_total_time[pipeline.operator_total_time.size() - 1]
			          << std::endl;
		}
		if (pipeline.materialize_flag) {
			if (pipeline.materialize_strategy_mode == 1) {
				std::cout << "Map building time: " << pipeline.map_building_time << std::endl;
				if (dump_statistic) {
					std::ofstream out(
					    "/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/release/payload_build_time.txt",
					    std::ios::app);
					int materialize_times = (total_materialized_chunks / pipeline.chunk_queue_threshold);
					if (total_materialized_chunks % pipeline.chunk_queue_threshold != 0) {
						materialize_times++;
					}
					int single_time_avg_rows = total_materialized_rows / materialize_times;
					out << materialize_times << " " << single_time_avg_rows << " " << total_materialized_chunks << " "
					    << total_materialized_rows << " " << pipeline.map_building_time << " "
					    << pipeline.mat_operator_time << std::endl;
					out.close();
				}
			}
			std::cout << "Materialize operator time: " << pipeline.mat_operator_time << std::endl;
			std::cout << "Total materialized chunks: " << total_materialized_chunks << std::endl;
			std::cout << "Total materialized rows: " << total_materialized_rows << std::endl;
		}
		std::cout << "----------------------------" << std::endl;
		std::cout << "Total time: " << pipeline.total_time << std::endl;
	}
	pipeline.mat_lock.unlock();
	pipeline.executor.Flush(thread);
	local_sink_state.reset();
	return PipelineExecuteResult::FINISHED;
}

void PipelineExecutor::GoToSource(idx_t &current_idx, idx_t initial_idx) {
	// we go back to the first operator (the source)
	current_idx = initial_idx;
	if (!in_process_operators.empty()) {
		// ... UNLESS there is an in process operator
		// if there is an in-process operator, we start executing at the latest one
		// for example, if we have a join operator that has tuples left, we first need to emit those tuples
		current_idx = in_process_operators.top();
		in_process_operators.pop();
	}
	D_ASSERT(current_idx >= initial_idx);
}

OperatorResultType PipelineExecutor::Execute(DataChunk &input, DataChunk &result, idx_t initial_idx) {
	if (input.size() == 0) { // LCOV_EXCL_START
		return OperatorResultType::NEED_MORE_INPUT;
	} // LCOV_EXCL_STOP
	D_ASSERT(!pipeline.operators.empty());

	idx_t current_idx;
	GoToSource(current_idx, initial_idx);
	if (current_idx == initial_idx) {
		current_idx++;
	}
	if (current_idx > pipeline.operators.size()) {
		result.Reference(input);
		return OperatorResultType::NEED_MORE_INPUT;
	}
	while (true) {
		if (context.client.interrupted) {
			throw InterruptException();
		}
		// now figure out where to put the chunk
		// if current_idx is the last possible index (>= operators.size()) we write to the result
		// otherwise we write to an intermediate chunk
		auto current_intermediate = current_idx;
		auto &current_chunk =
		    current_intermediate >= intermediate_chunks.size() ? result : *intermediate_chunks[current_intermediate];
		current_chunk.Reset();
		if (current_idx == initial_idx) {
			// we went back to the source: we need more input
			return OperatorResultType::NEED_MORE_INPUT;
		} else {
			auto &prev_chunk =
			    current_intermediate == initial_idx + 1 ? input : *intermediate_chunks[current_intermediate - 1];
			auto operator_idx = current_idx - 1;
			auto &current_operator = pipeline.operators[operator_idx].get();

			// if current_idx > source_idx, we pass the previous operators' output through the Execute of the
			// current operator
			StartOperator(current_operator);
			double op_start = getNow();
			auto op_result = current_operator.Execute(context, prev_chunk, current_chunk, *current_operator.op_state,
			                                          *intermediate_states[current_intermediate - 1]);
			double op_end = getNow();
			pipeline.incrementOperatorTime(op_end - op_start, operator_idx);

			//! if materialize here, we push the chunk into the chunk queue
			EndOperator(current_operator, &current_chunk);
			if (op_result == OperatorResultType::HAVE_MORE_OUTPUT) {
				// more data remains in this operator
				// push in-process marker
				in_process_operators.push(current_idx);
			} else if (op_result == OperatorResultType::FINISHED) {
				D_ASSERT(current_chunk.size() == 0);
				FinishProcessing(NumericCast<int32_t>(current_idx));
				return OperatorResultType::FINISHED;
			}
			// current_chunk.Verify();
		}

		if (current_chunk.size() == 0) {
			// no output from this operator!
			if (current_idx == initial_idx) {
				// if we got no output from the scan, we are done
				break;
			} else {
				// if we got no output from an intermediate op
				// we go back and try to pull data from the source again
				GoToSource(current_idx, initial_idx);
				continue;
			}
		} else {
			// we got output! continue to the next operator
			current_idx++;
			if (current_idx > pipeline.operators.size()) {
				// if we got output and are at the last operator, we are finished executing for this output chunk
				// return the data and push it into the chunk
				break;
			}
		}
	}
	return in_process_operators.empty() ? OperatorResultType::NEED_MORE_INPUT : OperatorResultType::HAVE_MORE_OUTPUT;
}

void PipelineExecutor::SetTaskForInterrupts(weak_ptr<Task> current_task) {
	interrupt_state = InterruptState(std::move(current_task));
}

SourceResultType PipelineExecutor::GetData(DataChunk &chunk, OperatorSourceInput &input) {
	//! Testing feature to enable async source on every operator
#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
	if (debug_blocked_source_count < debug_blocked_target_count) {
		debug_blocked_source_count++;

		auto &callback_state = input.interrupt_state;
		std::thread rewake_thread([callback_state] {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
			callback_state.Callback();
		});
		rewake_thread.detach();

		return SourceResultType::BLOCKED;
	}
#endif

	return pipeline.source->GetData(context, chunk, input);
}

SinkResultType PipelineExecutor::Sink(DataChunk &chunk, OperatorSinkInput &input) {
	//! Testing feature to enable async sink on every operator
#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
	if (debug_blocked_sink_count < debug_blocked_target_count) {
		debug_blocked_sink_count++;

		auto &callback_state = input.interrupt_state;
		std::thread rewake_thread([callback_state] {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
			callback_state.Callback();
		});
		rewake_thread.detach();

		return SinkResultType::BLOCKED;
	}
#endif
	return pipeline.sink->Sink(context, chunk, input);
}

SourceResultType PipelineExecutor::FetchFromSource(DataChunk &result) {
	StartOperator(*pipeline.source);

	OperatorSourceInput source_input = {*pipeline.source_state, *local_source_state, interrupt_state};
	auto res = GetData(result, source_input);
	// if (pipeline.pipeline_id == 2 &&
	//     result.data[1].GetBuffer().get()->GetBufferType() == VectorBufferType::DICTIONARY_BUFFER) {
	// 	auto buffer = result.data[1].GetBuffer().get();
	// 	DictionaryBuffer *dict_buffer = static_cast<DictionaryBuffer *>(buffer);
	// 	auto sel_index = &dict_buffer->GetSelVector();
	// 	auto val = reinterpret_cast<int32_t *>(result.data[1].GetData());
	// 	for (int i = 0; i < result.size(); i++) {
	// 		std::cout << sel_index->get_index(i) << " " << val[sel_index->get_index(i)] << std::endl;
	// 	}
	// }
	// Ensures Sinks only return empty results when Blocking or Finished
	D_ASSERT(res != SourceResultType::BLOCKED || result.size() == 0);

	EndOperator(*pipeline.source, &result);

	return res;
}

void PipelineExecutor::InitializeChunk(DataChunk &chunk) {
	auto &last_op = pipeline.operators.empty() ? *pipeline.source : pipeline.operators.back().get();
	auto types = last_op.GetTypes();

	chunk.Initialize(Allocator::DefaultAllocator(), types);
	chunk.disable_columns = last_op.disable_columns;
}

void PipelineExecutor::StartOperator(PhysicalOperator &op) {
	if (context.client.interrupted) {
		throw InterruptException();
	}
	context.thread.profiler.StartOperator(&op);
}

void PipelineExecutor::EndOperator(PhysicalOperator &op, optional_ptr<DataChunk> chunk) {
	context.thread.profiler.EndOperator(chunk);
	// TO DO: fix
	//  if (chunk) {
	//  	chunk->Verify();
	//  }
}

} // namespace duckdb
