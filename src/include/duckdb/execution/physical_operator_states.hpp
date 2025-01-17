//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/physical_operator_states.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"

namespace duckdb {
class Event;
class Executor;
class PhysicalOperator;
class Pipeline;
class PipelineBuildState;
class MetaPipeline;
class InterruptState;
class InvertedIndex;

struct SourcePartitionInfo {
	//! The current batch index
	//! This is only set in case RequiresBatchIndex() is true, and the source has support for it (SupportsBatchIndex())
	//! Otherwise this is left on INVALID_INDEX
	//! The batch index is a globally unique, increasing index that should be used to maintain insertion order
	//! //! in conjunction with parallelism
	optional_idx batch_index;
	//! The minimum batch index that any thread is currently actively reading
	optional_idx min_batch_index;
};

// LCOV_EXCL_START
class OperatorState {
public:
	virtual ~OperatorState() {
	}

	virtual void Finalize(const PhysicalOperator &op, ExecutionContext &context) {
	}

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class GlobalOperatorState {
public:
	virtual ~GlobalOperatorState() {
	}

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class GlobalSinkState : public StateWithBlockableTasks {
public:
	GlobalSinkState() : state(SinkFinalizeType::READY) {
	}
	virtual ~GlobalSinkState() {
	}

	SinkFinalizeType state;

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}

	virtual idx_t MaxThreads(idx_t source_max_threads) {
		return source_max_threads;
	}
};

class LocalSinkState {
public:
	virtual ~LocalSinkState() {
	}

	//! Source partition info
	SourcePartitionInfo partition_info;

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class GlobalSourceState : public StateWithBlockableTasks {
public:
	virtual ~GlobalSourceState() {
	}

	virtual idx_t MaxThreads() {
		return 1;
	}

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class LocalSourceState {
public:
	virtual ~LocalSourceState() {
	}

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct OperatorSinkInput {
	GlobalSinkState &global_state;
	LocalSinkState &local_state;
	InterruptState &interrupt_state;
	bool materialize_flag;
	map<int64_t, int8_t> materialize_column_types;
	int materialize_strategy_mode;
	bool final_materilaize;
	unordered_map<int, bool> colid_keep_rowid; // first: rowid_col_idx, second: keep_rowid
};

struct OperatorSourceInput {
	GlobalSourceState &global_state;
	LocalSourceState &local_state;
	InterruptState &interrupt_state;
	bool materialize_flag;
	int64_t rowid_column_id;
	unordered_map<int64_t, int64_t> materialize_col_id;
	unordered_map<int64_t, int32_t> fixed_len_strings_columns;
	bool use_inverted_index;
	std::unordered_map<int, std::unordered_map<int64_t, std::vector<int>>> *inverted_index;
	InvertedIndex *inverted_indexnew;
};

struct OperatorSinkCombineInput {
	GlobalSinkState &global_state;
	LocalSinkState &local_state;
	InterruptState &interrupt_state;
};

struct OperatorSinkFinalizeInput {
	GlobalSinkState &global_state;
	InterruptState &interrupt_state;
};

struct OperatorSinkNextBatchInput {
	GlobalSinkState &global_state;
	LocalSinkState &local_state;
	InterruptState &interrupt_state;
};
struct MatSourceInfo {
	MatSourceInfo() = default;
	MatSourceInfo(shared_ptr<RowGroupCollection> table, optional_ptr<PhysicalOperator> op,
	              unique_ptr<GlobalSourceState> state, unique_ptr<LocalSourceState> local_state) {
		this->table = table;
		this->materialize_source = op;
		this->materialize_source_state = move(state);
		this->materialize_local_source_state = move(local_state);
	}
	MatSourceInfo(MatSourceInfo &&info) noexcept {
		this->table = std::move(info.table);
		this->materialize_source = std::move(info.materialize_source);
		this->materialize_source_state = std::move(info.materialize_source_state);
		this->materialize_local_source_state = std::move(info.materialize_local_source_state);
	}
	MatSourceInfo &operator=(MatSourceInfo &&info) noexcept {
		if (this != &info) {
			this->table = std::move(info.table);
			this->materialize_source = std::move(info.materialize_source);
			this->materialize_source_state = std::move(info.materialize_source_state);
			this->materialize_local_source_state = std::move(info.materialize_local_source_state);
		}
		return *this;
	}
	shared_ptr<RowGroupCollection> table;
	optional_ptr<PhysicalOperator> materialize_source;
	unique_ptr<GlobalSourceState> materialize_source_state;
	unique_ptr<LocalSourceState> materialize_local_source_state;
};

// LCOV_EXCL_STOP

} // namespace duckdb
