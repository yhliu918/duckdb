#include "duckdb/parallel/pipeline_executor.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/execution/operator/helper/physical_materialized_collector.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/main/client_context.hpp"

#include <fstream>
#include <iostream>
#include <sys/time.h>
#include <thread>
#include <unistd.h>

#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
#include <chrono>
#include <thread>
#endif

namespace duckdb {
double getNow() {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}
void parse_materialize_config(Pipeline &pipeline_p, bool read_mat_info = false) {
	std::ifstream file("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/release/config/pipeline" +
	                       std::to_string(pipeline_p.pipeine_id),
	                   std::ios::in);
	if (file.is_open()) {
		// basic config
		int materialize_strategy_mode;
		bool push_source;

		file >> materialize_strategy_mode >> push_source;
		pipeline_p.push_source = push_source;

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
				int dependent_pipeline;
				int logical_type;
				int string_length = 0;
				int lines = 0;
				file >> dependent_pipeline >> keep_rowid >> rowid_col_idx >> lines;
				for (int j = 0; j < lines; j++) {
					file >> table_col_idx >> result_col_idx >> logical_type;
					col_map[table_col_idx] = result_col_idx;
					col_types[result_col_idx] = logical_type;
					if (LogicalTypeId(logical_type) == LogicalTypeId::VARCHAR) {
						file >> string_length;
						fixed_len_strings_columns[result_col_idx] = string_length;
					}
				}
				if (!col_map.empty()) {
					pipeline_p.SetMaterializeMap(materialize_strategy_mode, rowid_col_idx, dependent_pipeline,
					                             keep_rowid, col_map, col_types, fixed_len_strings_columns);
				}
			}
		}
		file.close();
	}
}

PipelineExecutor::PipelineExecutor(ClientContext &context_p, Pipeline &pipeline_p)
    : pipeline(pipeline_p), thread(context_p), context(context_p, thread, &pipeline_p) {
	D_ASSERT(pipeline.source_state);
	int num = 0;
	parse_materialize_config(pipeline, false);
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
			std::ifstream file("/home/yihao/duckdb/ht/duckdb/examples/embedded-c++/config_num", std::ios::in);
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
	pipeline.mat_lock.unlock();

	local_source_state = pipeline.source->GetLocalSourceState(context, *pipeline.source_state);

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
					parse_materialize_config(pipeline, true);
				}
			}
			pipeline.mat_lock.unlock();
		}
		auto &prev_operator = i == 0 ? *pipeline.source : pipeline.operators[i - 1].get();
		auto &current_operator = pipeline.operators[i].get();

		auto chunk = make_uniq<DataChunk>();
		chunk->Initialize(Allocator::Get(context.client), prev_operator.GetTypes());

		intermediate_chunks.push_back(std::move(chunk));

		auto op_state = current_operator.GetOperatorState(context);
		intermediate_states.push_back(std::move(op_state));

		if (current_operator.IsSink() && current_operator.sink_state->state == SinkFinalizeType::NO_OUTPUT_POSSIBLE) {
			// one of the operators has already figured out no output is possible
			// we can skip executing the pipeline
			FinishProcessing();
		}
	}

	if (pipeline.materialize_flag && pipeline.materialize_strategy_mode == 0) {
		auto &last_op = pipeline.operators.empty() ? *pipeline.source : pipeline.operators.back().get();
		auto types = last_op.GetTypes();
		auto chunk = make_uniq<DataChunk>();
		chunk->Initialize(Allocator::Get(context.client), types);
		intermediate_chunks.push_back(std::move(chunk));
	}

	if (pipeline.materialize_strategy_mode == 1) {
		int row_id_column_number = pipeline.materialize_maps.size();
		for (auto &[rowid_col_idx, mat_map] : pipeline.materialize_maps) {
			inverted_indexnew[rowid_col_idx].reserve(200);
			inverted_indexnew[rowid_col_idx].resize(200);
			for (auto &[colid, type] : mat_map.materialize_column_types) {
				pipeline.final_materialize_column_types[colid] = type;
			}
			result_index[rowid_col_idx] = 0;
		}
	}

	InitializeChunk(final_chunk);
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
	auto &source_chunk = pipeline.operators.empty() ? final_chunk : *intermediate_chunks[0];
	for (idx_t i = 0; i < max_chunks; i++) {
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
		if (&input != &final_chunk) {
			final_chunk.Reset();
			result = Execute(input, final_chunk, initial_idx);
			if (result == OperatorResultType::FINISHED) {
				return OperatorResultType::FINISHED;
			}
		} else {
			result = OperatorResultType::NEED_MORE_INPUT;
		}
		auto &sink_chunk = final_chunk;
		if (sink_chunk.size() > 0) {
			StartOperator(*pipeline.sink);
			D_ASSERT(pipeline.sink);
			D_ASSERT(pipeline.sink->sink_state);
			OperatorSinkInput sink_input {*pipeline.sink->sink_state,
			                              *local_sink_state,
			                              interrupt_state,
			                              pipeline.materialize_flag,
			                              pipeline.final_materialize_column_types,
			                              pipeline.materialize_strategy_mode,
			                              false};
			// todo: the materialized result collector sink
			if (pipeline.materialize_flag) {
				for (auto &[rowid, mat_map] : pipeline.materialize_maps) {
					sink_input.colid_keep_rowid[rowid] = mat_map.keep_rowid;
				}
			}
			double sink_start = getNow();
			auto sink_result = Sink(sink_chunk, sink_input);
			double sink_end = getNow();
			pipeline.incrementOperatorTime(sink_end - sink_start, pipeline.operators.size());

			// if (pipeline.materialize_strategy_mode == 1) {
			// 	auto sel_vec = sink_chunk.data[pipeline.rowid_col_idx];
			// 	int64_t *sel = reinterpret_cast<int64_t *>(sel_vec.GetData());
			// 	for (int64_t i = 0; i < sink_chunk.size(); i++) {
			// 		auto rowid = sel[i];
			// 		inverted_index[rowid / STANDARD_ROW_GROUPS_SIZE][rowid].emplace_back(result_index++);
			// 	}
			// }
			double materialize_start = getNow();

			if (pipeline.materialize_strategy_mode == 1) {
				for (auto &[rowid_col_idx, mat_map] : pipeline.materialize_maps) {
					auto sel_vec = sink_chunk.data[rowid_col_idx];
					int64_t *sel = reinterpret_cast<int64_t *>(sel_vec.GetData());
					int &index = result_index[rowid_col_idx];
					for (int64_t i = 0; i < sink_chunk.size(); i++) {
						auto rowid = sel[i];
						// inverted_indexnew[rowid / STANDARD_ROW_GROUPS_SIZE][rowid].emplace_back(result_index++);
						// inverted_index[rowid / STANDARD_ROW_GROUPS_SIZE][rowid].emplace_back(result_index++);
						std::cout << index << " " << rowid << std::endl;
						inverted_indexnew[rowid_col_idx][rowid / STANDARD_ROW_GROUPS_SIZE].emplace_back(
						    std::make_pair(rowid, index++));
					}
				}
			}

			double materialize_end = getNow();
			pipeline.map_building_time += materialize_end - materialize_start;

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

	double materialize_start = getNow();
	if (pipeline.materialize_strategy_mode == 1) {
		// materialize the inverted index columns and sink->Sink
		auto &last_op = pipeline.operators.empty() ? *pipeline.source : pipeline.operators.back().get();
		int origin_output_count = last_op.GetTypes().size();

		vector<LogicalType> types;
		for (auto &[col_id, type] : pipeline.final_materialize_column_types) {
			types.push_back(LogicalType(LogicalTypeId(type)));
		}
		DataChunk mat_chunk;
		mat_chunk.Initialize(Allocator::DefaultAllocator(), types, result_index.begin()->second);
		unordered_map<int, bool> colid_keep_rowid;
		for (auto &[rowid_col_idx, mat_map] : pipeline.materialize_maps) {
			colid_keep_rowid[rowid_col_idx] = mat_map.keep_rowid;
			unordered_map<int64_t, int64_t> materialize_column_ids_new;
			for (auto &[col_idx, result_col_index] : mat_map.materialize_column_ids) {
				materialize_column_ids_new[col_idx] = result_col_index - origin_output_count;
			}
			unordered_map<int64_t, int32_t> fixed_len_strings_columns_new;
			for (auto &[col_index, length] : mat_map.fixed_len_strings_columns) {
				fixed_len_strings_columns_new[col_index - origin_output_count] = length;
			}
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

			// std::cout << "cardinality " << result_index << std::endl;
			auto res = mat_source.materialize_source->GetData(context, mat_chunk, source_input);
		}
		mat_chunk.SetCardinality(result_index.begin()->second);

		// write the final result

		OperatorSinkInput sink_input {*pipeline.sink->sink_state,
		                              *local_sink_state,
		                              interrupt_state,
		                              pipeline.materialize_flag,
		                              pipeline.final_materialize_column_types,
		                              pipeline.materialize_strategy_mode,
		                              true,
		                              colid_keep_rowid};

		pipeline.sink->Sink(context, mat_chunk, sink_input);
	}
	double materialize_end = getNow();
	pipeline.mat_operator_time += materialize_end - materialize_start;

	// Run the combine for the sink
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
	if (pipeline.sink->type == PhysicalOperatorType::HASH_JOIN && pipeline.push_source) {
		pipeline.thread_num--;
		if (pipeline.thread_num == 0) {
			pipeline.sink->sink_state->Cast<HashJoinGlobalSinkState>().SetSource(
			    pipeline.pipeine_id,
			    pipeline.source.get()
			        ->Cast<PhysicalTableScan>()
			        .bind_data->Cast<TableScanBindData>()
			        .table.GetDataTable()
			        ->GetRowGroupCollection(),
			    pipeline.source, move(pipeline.source_state), move(local_source_state));
		}
	}

	bool print = pipeline.operators.size() > 0 || pipeline.sink->type != PhysicalOperatorType::CREATE_TABLE_AS;
	// print = false;
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
			}
			std::cout << "Materialize operator time: " << pipeline.mat_operator_time << std::endl;
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
			// FIX me: if we need to support materialize strategy 0
			//  if (pipeline.materialize_flag && operator_idx == pipeline.operators.size() - 1 &&
			//      pipeline.materialize_strategy_mode == 0) {
			//  	double mat_start = getNow();
			//  	OperatorSourceInput source_input = {*pipeline.materialize_source_state,
			//  	                                    *pipeline.materialize_local_source_state,
			//  	                                    interrupt_state,
			//  	                                    true,
			//  	                                    pipeline.rowid_col_idx,
			//  	                                    pipeline.materialize_column_ids,
			//  	                                    pipeline.fixed_len_strings_columns,
			//  	                                    false};
			//  	// std::cout << std::this_thread::get_id() << " " << pipeline.materialize_column_ids.size() << " "
			//  	//           << pipeline.materialize_column_types.size() << " "
			//  	//           << (pipeline.materialize_source_state != nullptr) << " "
			//  	//           << (pipeline.materialize_local_source_state != nullptr) << " " <<
			//  	//           current_chunk.ColumnCount()
			//  	//           << " " << result.ColumnCount() << std::endl;
			//  	// result.ReferenceColumns(current_chunk, {0, 1});
			//  	for (idx_t col_idx = 0; col_idx < current_chunk.ColumnCount(); col_idx++) {
			//  		auto &other_col = current_chunk.data[col_idx];
			//  		auto &this_col = result.data[col_idx];
			//  		D_ASSERT(other_col.GetType() == this_col.GetType());
			//  		this_col.Reference(other_col);
			//  	}
			//  	result.SetCardinality(current_chunk.size());
			//  	auto res = pipeline.materialize_source->GetData(context, result, source_input);
			//  	// std::cout << "get data!" << result.size() << std::endl;
			//  	double mat_end = getNow();
			//  	pipeline.mat_operator_time += mat_end - mat_start;
			//  }

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

	// Ensures Sinks only return empty results when Blocking or Finished
	D_ASSERT(res != SourceResultType::BLOCKED || result.size() == 0);

	EndOperator(*pipeline.source, &result);

	return res;
}

void PipelineExecutor::InitializeChunk(DataChunk &chunk) {
	auto &last_op = pipeline.operators.empty() ? *pipeline.source : pipeline.operators.back().get();
	auto types = last_op.GetTypes();
	if (pipeline.sink) {
		if (pipeline.sink->type == PhysicalOperatorType::RESULT_COLLECTOR && pipeline.operators.size() > 0 &&
		    pipeline.materialize_strategy_mode == 0) {
			pipeline.mat_lock.lock();
			for (auto &[col_id, type] : pipeline.final_materialize_column_types) {
				types.push_back(LogicalType(LogicalTypeId(type)));
			}
			pipeline.mat_lock.unlock();
		}
	}

	chunk.Initialize(Allocator::DefaultAllocator(), types);
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
