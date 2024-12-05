#include "duckdb/execution/operator/helper/physical_pipeline_breaker.hpp"

#include "../extension/jemalloc/include/jemalloc_extension.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

void ConcurrentQueue::Enqueue(DataChunk &&chunk) {
	if (q.enqueue(std::move(chunk))) {
		semaphore.signal();
	} else {
		throw InternalException("Could not enqueue datachunk!");
	}
}

PhysicalPipelineBreaker::PhysicalPipelineBreaker(vector<LogicalType> types, unique_ptr<PhysicalOperator> child_operator,
                                                 idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::PIPELINE_BREAKER, std::move(types), estimated_cardinality),
	  chunk_queue(make_uniq<ConcurrentQueue>()) {
	children.push_back(std::move(child_operator));
}

PhysicalPipelineBreaker::~PhysicalPipelineBreaker() {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PhysicalPipelineBreaker::Sink(ExecutionContext &context, DataChunk &chunk,
											 OperatorSinkInput &input) const {
	auto chunk_copy = chunk;
	chunk_queue->Enqueue(std::move(chunk_copy));
	return SinkResultType::NEED_MORE_INPUT;
}
SinkFinalizeType PhysicalPipelineBreaker::Finalize(Pipeline &pipeline, Event &event,
                                                   ClientContext &context,
                                                   OperatorSinkFinalizeInput &input) const {
	chunk_queue->semaphore.signal(96);
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PipelineBreakerGlobalSource : public GlobalSourceState {
	idx_t MaxThreads() override {
		return 96;
	}
};

unique_ptr<GlobalSourceState> PhysicalPipelineBreaker::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<PipelineBreakerGlobalSource>();
}

SourceResultType PhysicalPipelineBreaker::GetData(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSourceInput &input) const {
	chunk_queue->semaphore.wait();
	if (chunk_queue->q.try_dequeue(chunk)) {
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
	return SourceResultType::FINISHED;
}

// Build
void PhysicalPipelineBreaker::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();

	auto &state = meta_pipeline.GetState();

	// operator is a sink, build a pipeline
	sink_state.reset();
	D_ASSERT(children.size() == 1);

	// single operator: the operator becomes the data source of the current pipeline
	state.SetPipelineSource(current, *this);

	// we create a new pipeline starting from the child
	auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipelineWithoutDependency(current, *this);
	child_meta_pipeline.Build(*children[0]);
}
}  // namespace duckdb