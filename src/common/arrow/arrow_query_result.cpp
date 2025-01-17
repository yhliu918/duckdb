#include "duckdb/common/arrow/arrow_query_result.hpp"

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

ArrowQueryResult::ArrowQueryResult(StatementType statement_type, StatementProperties properties, vector<string> names_p,
                                   vector<LogicalType> types_p, ClientProperties client_properties, idx_t batch_size)
    : QueryResult(QueryResultType::ARROW_RESULT, statement_type, std::move(properties), std::move(types_p),
                  std::move(names_p), std::move(client_properties)),
      batch_size(batch_size) {
}

ArrowQueryResult::ArrowQueryResult(ErrorData error) : QueryResult(QueryResultType::ARROW_RESULT, std::move(error)) {
}

unique_ptr<DataChunk> ArrowQueryResult::Fetch() {
	throw NotImplementedException("Can't 'Fetch' from ArrowQueryResult");
}
unique_ptr<DataChunk> ArrowQueryResult::FetchRaw() {
	throw NotImplementedException("Can't 'FetchRaw' from ArrowQueryResult");
}

string ArrowQueryResult::ToString() {
	// FIXME: can't throw an exception here as it's used for verification
	return "";
}

int64_t ArrowQueryResult::GetRowNumber() {
	// FIXME: can't throw an exception here as it's used for verification
	return 0;
}

vector<unique_ptr<ArrowArrayWrapper>> ArrowQueryResult::ConsumeArrays() {
	if (HasError()) {
		throw InvalidInputException("Attempting to fetch ArrowArrays from an unsuccessful query result\n: Error %s",
		                            GetError());
	}
	return std::move(arrays);
}

vector<unique_ptr<ArrowArrayWrapper>> &ArrowQueryResult::Arrays() {
	if (HasError()) {
		throw InvalidInputException("Attempting to fetch ArrowArrays from an unsuccessful query result\n: Error %s",
		                            GetError());
	}
	return arrays;
}

void ArrowQueryResult::SetArrowData(vector<unique_ptr<ArrowArrayWrapper>> arrays) {
	D_ASSERT(this->arrays.empty());
	this->arrays = std::move(arrays);
}

idx_t ArrowQueryResult::BatchSize() const {
	return batch_size;
}

} // namespace duckdb
