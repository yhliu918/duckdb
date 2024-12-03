#include "duckdb/execution/operator/helper/physical_pipeline_breaker.hpp"

#include "../extension/jemalloc/include/jemalloc_extension.hpp"

#include <sys/time.h>

extern double first_probe_end;
extern double combine_end;
extern int debug_tag;
extern int probe_type;
static double getNow() {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}

namespace duckdb {

PhysicalPipelineBreaker::PhysicalPipelineBreaker(vector<LogicalType> types, unique_ptr<PhysicalOperator> child_operator,
                                                 idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::PIPELINE_BREAKER, std::move(types), estimated_cardinality) {
	children.push_back(std::move(child_operator));
}

PhysicalPipelineBreaker::~PhysicalPipelineBreaker() {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class PipelineBreakerGlobalSink : public GlobalSinkState {
public:
	std::mutex glock;
	shared_ptr<ColumnDataCollection> intermediate_table;
	ColumnDataParallelScanState scan_state;
	bool initialized = false;
};

class PipelineBreakerLocalSink : public LocalSinkState {
public:
	explicit PipelineBreakerLocalSink(vector<LogicalType> types, ClientContext &context) {
		intermediate_table = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
		intermediate_table->InitializeAppend(append_state);
	}

	unique_ptr<ColumnDataCollection> intermediate_table;
	ColumnDataAppendState append_state;
};

duckdb::SinkResultType PhysicalPipelineBreaker::Sink(duckdb::ExecutionContext &context, duckdb::DataChunk &chunk,
                                                     duckdb::OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<PipelineBreakerLocalSink>();

	lstate.intermediate_table->Append(lstate.append_state, chunk);

	if (debug_tag) {
		first_probe_end = getNow();
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalPipelineBreaker::Combine(ExecutionContext &context,
                                                       OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<PipelineBreakerGlobalSink>();
	auto &lstate = input.local_state.Cast<PipelineBreakerLocalSink>();

	if (lstate.intermediate_table->Count() == 0) {
		return SinkCombineResultType::FINISHED;
	}

	lock_guard<mutex> l(gstate.glock);
	if (!gstate.intermediate_table) {
		gstate.intermediate_table = std::move(lstate.intermediate_table);
	} else {
		gstate.intermediate_table->Combine(*lstate.intermediate_table);
	}

	if (debug_tag) {
		combine_end = getNow();
		probe_type = 1;
	}

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalPipelineBreaker::Finalize(duckdb::Pipeline &pipeline, duckdb::Event &event,
                                                   duckdb::ClientContext &context,
                                                   duckdb::OperatorSinkFinalizeInput &input) const {
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSinkState> PhysicalPipelineBreaker::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<PipelineBreakerGlobalSink>();
}

unique_ptr<LocalSinkState> PhysicalPipelineBreaker::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<PipelineBreakerLocalSink>(types, context.client);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PipelineBreakerGlobalSource : public GlobalSourceState {
public:
	explicit PipelineBreakerGlobalSource(const ColumnDataCollection &collection)
	    : max_threads(MaxValue<idx_t>(collection.ChunkCount(), 1)) {
		collection.InitializeScan(global_scan_state);
	}

	idx_t MaxThreads() override {
		return max_threads;
	}

public:
	ColumnDataParallelScanState global_scan_state;

	const idx_t max_threads;
};

class PipelineBreakerLocalSource : public LocalSourceState {
public:
	ColumnDataLocalScanState local_scan_state;
};

unique_ptr<GlobalSourceState> PhysicalPipelineBreaker::GetGlobalSourceState(ClientContext &context) const {
	auto &sink = sink_state->Cast<PipelineBreakerGlobalSink>();
	return make_uniq<PipelineBreakerGlobalSource>(*sink.intermediate_table);
}
unique_ptr<LocalSourceState> PhysicalPipelineBreaker::GetLocalSourceState(duckdb::ExecutionContext &context,
                                                                          GlobalSourceState &gstate) const {
	return make_uniq<PipelineBreakerLocalSource>();
}

SourceResultType PhysicalPipelineBreaker::GetData(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSourceInput &input) const {
	auto &sink = sink_state->Cast<PipelineBreakerGlobalSink>();
	auto &gstate = input.global_state.Cast<PipelineBreakerGlobalSource>();
	auto &lstate = input.local_state.Cast<PipelineBreakerLocalSource>();

	sink.intermediate_table->Scan(gstate.global_scan_state, lstate.local_scan_state, chunk);

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}
}  // namespace duckdb