//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/pipeline.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parallel/executor_task.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"

namespace duckdb {

class Executor;
class Event;
class MetaPipeline;
class PipelineExecutor;
class Pipeline;

class PipelineTask : public ExecutorTask {
	static constexpr const idx_t PARTIAL_CHUNK_COUNT = 50;

public:
	explicit PipelineTask(Pipeline &pipeline_p, shared_ptr<Event> event_p);

	Pipeline &pipeline;
	unique_ptr<PipelineExecutor> pipeline_executor;

public:
	const PipelineExecutor &GetPipelineExecutor() const;
	bool TaskBlockedOnResult() const override;

public:
	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;
};

class PipelineBuildState {
public:
	//! How much to increment batch indexes when multiple pipelines share the same source
	constexpr static idx_t BATCH_INCREMENT = 10000000000000;

public:
	//! Duplicate eliminated join scan dependencies
	reference_map_t<const PhysicalOperator, reference<Pipeline>> delim_join_dependencies;
	//! Materialized CTE scan dependencies
	reference_map_t<const PhysicalOperator, reference<Pipeline>> cte_dependencies;

public:
	void SetPipelineSource(Pipeline &pipeline, PhysicalOperator &op);
	void SetPipelineSink(Pipeline &pipeline, optional_ptr<PhysicalOperator> op, idx_t sink_pipeline_count);
	void SetPipelineOperators(Pipeline &pipeline, vector<reference<PhysicalOperator>> operators);
	void AddPipelineOperator(Pipeline &pipeline, PhysicalOperator &op);
	shared_ptr<Pipeline> CreateChildPipeline(Executor &executor, Pipeline &pipeline, PhysicalOperator &op);

	optional_ptr<PhysicalOperator> GetPipelineSource(Pipeline &pipeline);
	optional_ptr<PhysicalOperator> GetPipelineSink(Pipeline &pipeline);
	vector<reference<PhysicalOperator>> GetPipelineOperators(Pipeline &pipeline);
};

//! The Pipeline class represents an execution pipeline starting at a
class Pipeline : public enable_shared_from_this<Pipeline> {
	friend class Executor;
	friend class PipelineExecutor;
	friend class PipelineEvent;
	friend class PipelineFinishEvent;
	friend class PipelineBuildState;
	friend class MetaPipeline;

public:
	explicit Pipeline(Executor &execution_context);

	Executor &executor;

public:
	ClientContext &GetClientContext();

	void AddDependency(shared_ptr<Pipeline> &pipeline);

	void Ready();
	void Reset();
	void ResetSink();
	void ResetSource(bool force);
	void ClearSource();
	void Schedule(shared_ptr<Event> &event);
	void PrepareFinalize();

	string ToString() const;
	void Print() const;
	void PrintDependencies() const;

	//! Returns query progress
	bool GetProgress(double &current_percentage, idx_t &estimated_cardinality);

	//! Returns a list of all operators (including source and sink) involved in this pipeline
	vector<reference<PhysicalOperator>> GetOperators();
	vector<const_reference<PhysicalOperator>> GetOperators() const;

	optional_ptr<PhysicalOperator> GetSink() {
		return sink;
	}

	optional_ptr<PhysicalOperator> GetSource() {
		return source;
	}

	//! Returns whether any of the operators in the pipeline care about preserving order
	bool IsOrderDependent() const;

	//! Registers a new batch index for a pipeline executor - returns the current minimum batch index
	idx_t RegisterNewBatchIndex();

	//! Updates the batch index of a pipeline (and returns the new minimum batch index)
	idx_t UpdateBatchIndex(idx_t old_index, idx_t new_index);

	void SetMaterializeSource(int pipe_id, shared_ptr<RowGroupCollection> table, optional_ptr<PhysicalOperator> op,
	                          unique_ptr<GlobalSourceState> state, unique_ptr<LocalSourceState> local_state) {
		materialize_sources[pipe_id] = MatSourceInfo(table, move(op), move(state), move(local_state));
	}

	struct MaterializeMap {
		MaterializeMap(int pipeid, bool keep_rowid, unordered_map<int64_t, int64_t> colid, map<int64_t, int8_t> types,
		               unordered_map<int64_t, int32_t> string_columns) {
			this->source_pipeline_id = pipeid;
			this->keep_rowid = keep_rowid;
			this->materialize_column_ids = move(colid);
			this->materialize_column_types = move(types);
			this->fixed_len_strings_columns = move(string_columns);
		}
		MaterializeMap() = default;
		MaterializeMap &operator=(MaterializeMap &&map) noexcept {
			if (this != &map) {
				this->source_pipeline_id = map.source_pipeline_id;
				this->keep_rowid = map.keep_rowid;
				this->materialize_column_ids = move(map.materialize_column_ids);
				this->materialize_column_types = move(map.materialize_column_types);
				this->fixed_len_strings_columns = move(map.fixed_len_strings_columns);
			}
			return *this;
		}
		MaterializeMap(MaterializeMap &map) {
			this->source_pipeline_id = map.source_pipeline_id;
			this->keep_rowid = map.keep_rowid;
			this->materialize_column_ids = move(map.materialize_column_ids);
			this->materialize_column_types = move(map.materialize_column_types);
			this->fixed_len_strings_columns = move(map.fixed_len_strings_columns);
		}
		int source_pipeline_id;
		bool keep_rowid;
		unordered_map<int64_t, int64_t> materialize_column_ids;
		map<int64_t, int8_t> materialize_column_types;
		unordered_map<int64_t, int32_t> fixed_len_strings_columns;
	};

	void SetMaterializeMap(int strategy, int col_idx, int pipeline_id, bool keep_rowid,
	                       unordered_map<int64_t, int64_t> colid, map<int64_t, int8_t> types,
	                       unordered_map<int64_t, int32_t> string_columns) {
		materialize_strategy_mode = strategy;
		materialize_flag = true;
		materialize_maps[col_idx] =
		    MaterializeMap(pipeline_id, keep_rowid, move(colid), move(types), move(string_columns));
	}

	bool materialize_flag = false;
	std::mutex mat_lock;
	std::vector<int> operator_state_set;
	int thread_num;
	void incrementOperatorTime(double time, int op_idx) {
		operator_total_time[op_idx] += time;
	}

	vector<double> operator_total_time;
	double mat_operator_time = 0;
	double map_building_time = 0;
	double total_time = 0;
	int pipeline_id = 0;
	int parent = 0;
	bool push_source = false;
	unordered_map<int, MaterializeMap> materialize_maps;   // rowid_col_idx -> MaterializeMap
	unordered_map<int, MatSourceInfo> materialize_sources; // pipelineid -> MatSourceInfo

	map<int64_t, int8_t> final_materialize_column_types; // output layout
	int chunk_queue_threshold = 0;

private:
	//! Whether or not the pipeline has been readied
	bool ready;
	//! Whether or not the pipeline has been initialized
	atomic<bool> initialized;
	//! The source of this pipeline
	optional_ptr<PhysicalOperator> source;

	int materialize_strategy_mode;

	//! The chain of intermediate operators
	vector<reference<PhysicalOperator>> operators;

	//! The sink (i.e. destination) for data; this is e.g. a hash table to-be-built
	optional_ptr<PhysicalOperator> sink;

	//! The global source state
	unique_ptr<GlobalSourceState> source_state;

	//! The parent pipelines (i.e. pipelines that are dependent on this pipeline to finish)
	vector<weak_ptr<Pipeline>> parents;
	//! The dependencies of this pipeline
	vector<weak_ptr<Pipeline>> dependencies;

	//! The base batch index of this pipeline
	idx_t base_batch_index = 0;
	//! Lock for accessing the set of batch indexes
	mutex batch_lock;
	//! The set of batch indexes that are currently being processed
	//! Despite batch indexes being unique - this is a multiset
	//! The reason is that when we start a new pipeline we insert the current minimum batch index as a placeholder
	//! Which leads to duplicate entries in the set of active batch indexes
	multiset<idx_t> batch_indexes;

private:
	void ScheduleSequentialTask(shared_ptr<Event> &event);
	bool LaunchScanTasks(shared_ptr<Event> &event, idx_t max_threads);

	bool ScheduleParallel(shared_ptr<Event> &event);
};

} // namespace duckdb
