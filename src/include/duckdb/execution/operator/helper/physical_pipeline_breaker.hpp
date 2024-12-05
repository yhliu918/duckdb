//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_pipeline_breaker.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>

#include "duckdb/execution/physical_operator.hpp"

#include "concurrentqueue.h"
#include "duckdb/common/thread.hpp"
#include "lightweightsemaphore.h"

namespace duckdb {

typedef duckdb_moodycamel::ConcurrentQueue<duckdb::DataChunk> concurrent_queue_t;
typedef duckdb_moodycamel::LightweightSemaphore lightweight_semaphore_t;

struct ConcurrentQueue {
	concurrent_queue_t q;
	lightweight_semaphore_t semaphore;

	void Enqueue(DataChunk &&chunk);
};

//! PhysicalPipelineBreaker represents a physical operator that is used to break up pipelines
class PhysicalPipelineBreaker : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::PIPELINE_BREAKER;

public:
	PhysicalPipelineBreaker(vector<LogicalType> types, unique_ptr<PhysicalOperator> join, idx_t estimated_cardinality);

	~PhysicalPipelineBreaker() override;

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;

	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return true;
	}

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

private:
	unique_ptr<ConcurrentQueue> chunk_queue;
};
}  // namespace duckdb