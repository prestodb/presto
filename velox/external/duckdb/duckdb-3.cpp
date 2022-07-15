// See https://raw.githubusercontent.com/duckdb/duckdb/master/LICENSE for licensing information

#include "duckdb.hpp"
#include "duckdb-internal.hpp"
#ifndef DUCKDB_AMALGAMATION
#error header mismatch
#endif








namespace duckdb {

PhysicalPerfectHashAggregate::PhysicalPerfectHashAggregate(ClientContext &context, vector<LogicalType> types_p,
                                                           vector<unique_ptr<Expression>> aggregates_p,
                                                           vector<unique_ptr<Expression>> groups_p,
                                                           vector<unique_ptr<BaseStatistics>> group_stats,
                                                           vector<idx_t> required_bits_p, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::PERFECT_HASH_GROUP_BY, move(types_p), estimated_cardinality),
      groups(move(groups_p)), aggregates(move(aggregates_p)), required_bits(move(required_bits_p)) {
	D_ASSERT(groups.size() == group_stats.size());
	group_minima.reserve(group_stats.size());
	for (auto &stats : group_stats) {
		D_ASSERT(stats);
		auto &nstats = (NumericStatistics &)*stats;
		D_ASSERT(!nstats.min.IsNull());
		group_minima.push_back(move(nstats.min));
	}
	for (auto &expr : groups) {
		group_types.push_back(expr->return_type);
	}

	vector<BoundAggregateExpression *> bindings;
	vector<LogicalType> payload_types_filters;
	for (auto &expr : aggregates) {
		D_ASSERT(expr->expression_class == ExpressionClass::BOUND_AGGREGATE);
		D_ASSERT(expr->IsAggregate());
		auto &aggr = (BoundAggregateExpression &)*expr;
		bindings.push_back(&aggr);

		D_ASSERT(!aggr.distinct);
		D_ASSERT(aggr.function.combine);
		for (auto &child : aggr.children) {
			payload_types.push_back(child->return_type);
		}
		if (aggr.filter) {
			payload_types_filters.push_back(aggr.filter->return_type);
		}
	}
	for (const auto &pay_filters : payload_types_filters) {
		payload_types.push_back(pay_filters);
	}
	aggregate_objects = AggregateObject::CreateAggregateObjects(bindings);

	// filter_indexes must be pre-built, not lazily instantiated in parallel...
	idx_t aggregate_input_idx = 0;
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		aggregate_input_idx += aggr.children.size();
	}
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		if (aggr.filter) {
			auto &bound_ref_expr = (BoundReferenceExpression &)*aggr.filter;
			auto it = filter_indexes.find(aggr.filter.get());
			if (it == filter_indexes.end()) {
				filter_indexes[aggr.filter.get()] = bound_ref_expr.index;
				bound_ref_expr.index = aggregate_input_idx++;
			} else {
				++aggregate_input_idx;
			}
		}
	}
}

unique_ptr<PerfectAggregateHashTable> PhysicalPerfectHashAggregate::CreateHT(Allocator &allocator,
                                                                             ClientContext &context) const {
	return make_unique<PerfectAggregateHashTable>(allocator, BufferManager::GetBufferManager(context), group_types,
	                                              payload_types, aggregate_objects, group_minima, required_bits);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class PerfectHashAggregateGlobalState : public GlobalSinkState {
public:
	PerfectHashAggregateGlobalState(const PhysicalPerfectHashAggregate &op, ClientContext &context)
	    : ht(op.CreateHT(Allocator::Get(context), context)) {
	}

	//! The lock for updating the global aggregate state
	mutex lock;
	//! The global aggregate hash table
	unique_ptr<PerfectAggregateHashTable> ht;
};

class PerfectHashAggregateLocalState : public LocalSinkState {
public:
	PerfectHashAggregateLocalState(const PhysicalPerfectHashAggregate &op, ExecutionContext &context)
	    : ht(op.CreateHT(Allocator::Get(context.client), context.client)) {
		group_chunk.InitializeEmpty(op.group_types);
		if (!op.payload_types.empty()) {
			aggregate_input_chunk.InitializeEmpty(op.payload_types);
		}
	}

	//! The local aggregate hash table
	unique_ptr<PerfectAggregateHashTable> ht;
	DataChunk group_chunk;
	DataChunk aggregate_input_chunk;
};

unique_ptr<GlobalSinkState> PhysicalPerfectHashAggregate::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<PerfectHashAggregateGlobalState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalPerfectHashAggregate::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<PerfectHashAggregateLocalState>(*this, context);
}

SinkResultType PhysicalPerfectHashAggregate::Sink(ExecutionContext &context, GlobalSinkState &state,
                                                  LocalSinkState &lstate_p, DataChunk &input) const {
	auto &lstate = (PerfectHashAggregateLocalState &)lstate_p;
	DataChunk &group_chunk = lstate.group_chunk;
	DataChunk &aggregate_input_chunk = lstate.aggregate_input_chunk;

	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		auto &group = groups[group_idx];
		D_ASSERT(group->type == ExpressionType::BOUND_REF);
		auto &bound_ref_expr = (BoundReferenceExpression &)*group;
		group_chunk.data[group_idx].Reference(input.data[bound_ref_expr.index]);
	}
	idx_t aggregate_input_idx = 0;
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		for (auto &child_expr : aggr.children) {
			D_ASSERT(child_expr->type == ExpressionType::BOUND_REF);
			auto &bound_ref_expr = (BoundReferenceExpression &)*child_expr;
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(input.data[bound_ref_expr.index]);
		}
	}
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		if (aggr.filter) {
			auto it = filter_indexes.find(aggr.filter.get());
			D_ASSERT(it != filter_indexes.end());
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(input.data[it->second]);
		}
	}

	group_chunk.SetCardinality(input.size());

	aggregate_input_chunk.SetCardinality(input.size());

	group_chunk.Verify();
	aggregate_input_chunk.Verify();
	D_ASSERT(aggregate_input_chunk.ColumnCount() == 0 || group_chunk.size() == aggregate_input_chunk.size());

	lstate.ht->AddChunk(group_chunk, aggregate_input_chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
void PhysicalPerfectHashAggregate::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                           LocalSinkState &lstate_p) const {
	auto &lstate = (PerfectHashAggregateLocalState &)lstate_p;
	auto &gstate = (PerfectHashAggregateGlobalState &)gstate_p;

	lock_guard<mutex> l(gstate.lock);
	gstate.ht->Combine(*lstate.ht);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PerfectHashAggregateState : public GlobalSourceState {
public:
	PerfectHashAggregateState() : ht_scan_position(0) {
	}

	//! The current position to scan the HT for output tuples
	idx_t ht_scan_position;
};

unique_ptr<GlobalSourceState> PhysicalPerfectHashAggregate::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<PerfectHashAggregateState>();
}

void PhysicalPerfectHashAggregate::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                           LocalSourceState &lstate) const {
	auto &state = (PerfectHashAggregateState &)gstate_p;
	auto &gstate = (PerfectHashAggregateGlobalState &)*sink_state;

	gstate.ht->Scan(state.ht_scan_position, chunk);
}

string PhysicalPerfectHashAggregate::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < groups.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += groups[i]->GetName();
	}
	for (idx_t i = 0; i < aggregates.size(); i++) {
		if (i > 0 || !groups.empty()) {
			result += "\n";
		}
		result += aggregates[i]->GetName();
		auto &aggregate = (BoundAggregateExpression &)*aggregates[i];
		if (aggregate.filter) {
			result += " Filter: " + aggregate.filter->GetName();
		}
	}
	return result;
}

} // namespace duckdb









namespace duckdb {

PhysicalSimpleAggregate::PhysicalSimpleAggregate(vector<LogicalType> types, vector<unique_ptr<Expression>> expressions,
                                                 idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::SIMPLE_AGGREGATE, move(types), estimated_cardinality),
      aggregates(move(expressions)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
struct AggregateState {
	explicit AggregateState(const vector<unique_ptr<Expression>> &aggregate_expressions) {
		for (auto &aggregate : aggregate_expressions) {
			D_ASSERT(aggregate->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
			auto &aggr = (BoundAggregateExpression &)*aggregate;
			auto state = unique_ptr<data_t[]>(new data_t[aggr.function.state_size()]);
			aggr.function.initialize(state.get());
			aggregates.push_back(move(state));
			destructors.push_back(aggr.function.destructor);
		}
	}
	~AggregateState() {
		D_ASSERT(destructors.size() == aggregates.size());
		for (idx_t i = 0; i < destructors.size(); i++) {
			if (!destructors[i]) {
				continue;
			}
			Vector state_vector(Value::POINTER((uintptr_t)aggregates[i].get()));
			state_vector.SetVectorType(VectorType::FLAT_VECTOR);

			destructors[i](state_vector, 1);
		}
	}

	void Move(AggregateState &other) {
		other.aggregates = move(aggregates);
		other.destructors = move(destructors);
	}

	//! The aggregate values
	vector<unique_ptr<data_t[]>> aggregates;
	// The destructors
	vector<aggregate_destructor_t> destructors;
};

class SimpleAggregateGlobalState : public GlobalSinkState {
public:
	explicit SimpleAggregateGlobalState(const vector<unique_ptr<Expression>> &aggregates)
	    : state(aggregates), finished(false) {
	}

	//! The lock for updating the global aggregate state
	mutex lock;
	//! The global aggregate state
	AggregateState state;
	//! Whether or not the aggregate is finished
	bool finished;
};

class SimpleAggregateLocalState : public LocalSinkState {
public:
	SimpleAggregateLocalState(Allocator &allocator, const vector<unique_ptr<Expression>> &aggregates,
	                          const vector<LogicalType> &child_types)
	    : state(aggregates), child_executor(allocator) {
		vector<LogicalType> payload_types;
		vector<AggregateObject> aggregate_objects;
		for (auto &aggregate : aggregates) {
			D_ASSERT(aggregate->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
			auto &aggr = (BoundAggregateExpression &)*aggregate;
			// initialize the payload chunk
			if (!aggr.children.empty()) {
				for (auto &child : aggr.children) {
					payload_types.push_back(child->return_type);
					child_executor.AddExpression(*child);
				}
			}
			aggregate_objects.emplace_back(&aggr);
		}
		if (!payload_types.empty()) { // for select count(*) from t; there is no payload at all
			payload_chunk.Initialize(allocator, payload_types);
		}
		filter_set.Initialize(allocator, aggregate_objects, child_types);
	}
	void Reset() {
		payload_chunk.Reset();
	}

	//! The local aggregate state
	AggregateState state;
	//! The executor
	ExpressionExecutor child_executor;
	//! The payload chunk
	DataChunk payload_chunk;
	//! Aggregate filter data set
	AggregateFilterDataSet filter_set;
};

unique_ptr<GlobalSinkState> PhysicalSimpleAggregate::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<SimpleAggregateGlobalState>(aggregates);
}

unique_ptr<LocalSinkState> PhysicalSimpleAggregate::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<SimpleAggregateLocalState>(Allocator::Get(context.client), aggregates, children[0]->GetTypes());
}

SinkResultType PhysicalSimpleAggregate::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                             DataChunk &input) const {
	auto &sink = (SimpleAggregateLocalState &)lstate;
	// perform the aggregation inside the local state
	idx_t payload_idx = 0, payload_expr_idx = 0;
	sink.Reset();

	DataChunk &payload_chunk = sink.payload_chunk;
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggregate = (BoundAggregateExpression &)*aggregates[aggr_idx];
		idx_t payload_cnt = 0;
		// resolve the filter (if any)
		if (aggregate.filter) {
			auto &filtered_data = sink.filter_set.GetFilterData(aggr_idx);
			auto count = filtered_data.ApplyFilter(input);

			sink.child_executor.SetChunk(filtered_data.filtered_payload);
			payload_chunk.SetCardinality(count);
		} else {
			sink.child_executor.SetChunk(input);
			payload_chunk.SetCardinality(input);
		}
		// resolve the child expressions of the aggregate (if any)
		if (!aggregate.children.empty()) {
			for (idx_t i = 0; i < aggregate.children.size(); ++i) {
				sink.child_executor.ExecuteExpression(payload_expr_idx, payload_chunk.data[payload_idx + payload_cnt]);
				payload_expr_idx++;
				payload_cnt++;
			}
		}

		AggregateInputData aggr_input_data(aggregate.bind_info.get());
		aggregate.function.simple_update(payload_cnt == 0 ? nullptr : &payload_chunk.data[payload_idx], aggr_input_data,
		                                 payload_cnt, sink.state.aggregates[aggr_idx].get(), payload_chunk.size());
		payload_idx += payload_cnt;
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
void PhysicalSimpleAggregate::Combine(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate) const {
	auto &gstate = (SimpleAggregateGlobalState &)state;
	auto &source = (SimpleAggregateLocalState &)lstate;
	D_ASSERT(!gstate.finished);

	// finalize: combine the local state into the global state
	// all aggregates are combinable: we might be doing a parallel aggregate
	// use the combine method to combine the partial aggregates
	lock_guard<mutex> glock(gstate.lock);
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggregate = (BoundAggregateExpression &)*aggregates[aggr_idx];
		Vector source_state(Value::POINTER((uintptr_t)source.state.aggregates[aggr_idx].get()));
		Vector dest_state(Value::POINTER((uintptr_t)gstate.state.aggregates[aggr_idx].get()));

		AggregateInputData aggr_input_data(aggregate.bind_info.get());
		aggregate.function.combine(source_state, dest_state, aggr_input_data, 1);
	}

	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(this, &source.child_executor, "child_executor", 0);
	client_profiler.Flush(context.thread.profiler);
}

SinkFinalizeType PhysicalSimpleAggregate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                   GlobalSinkState &gstate_p) const {
	auto &gstate = (SimpleAggregateGlobalState &)gstate_p;

	D_ASSERT(!gstate.finished);
	gstate.finished = true;
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class SimpleAggregateState : public GlobalSourceState {
public:
	SimpleAggregateState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalSimpleAggregate::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<SimpleAggregateState>();
}

void PhysicalSimpleAggregate::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                      LocalSourceState &lstate) const {
	auto &gstate = (SimpleAggregateGlobalState &)*sink_state;
	auto &state = (SimpleAggregateState &)gstate_p;
	D_ASSERT(gstate.finished);
	if (state.finished) {
		return;
	}

	// initialize the result chunk with the aggregate values
	chunk.SetCardinality(1);
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggregate = (BoundAggregateExpression &)*aggregates[aggr_idx];

		Vector state_vector(Value::POINTER((uintptr_t)gstate.state.aggregates[aggr_idx].get()));
		AggregateInputData aggr_input_data(aggregate.bind_info.get());
		aggregate.function.finalize(state_vector, aggr_input_data, chunk.data[aggr_idx], 1, 0);
	}
	state.finished = true;
}

string PhysicalSimpleAggregate::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = (BoundAggregateExpression &)*aggregates[i];
		if (i > 0) {
			result += "\n";
		}
		result += aggregates[i]->GetName();
		if (aggregate.filter) {
			result += " Filter: " + aggregate.filter->GetName();
		}
	}
	return result;
}

} // namespace duckdb







namespace duckdb {

PhysicalStreamingWindow::PhysicalStreamingWindow(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                                                 idx_t estimated_cardinality, PhysicalOperatorType type)
    : PhysicalOperator(type, move(types), estimated_cardinality), select_list(move(select_list)) {
}

class StreamingWindowGlobalState : public GlobalOperatorState {
public:
	StreamingWindowGlobalState() : row_number(1) {
	}

	//! The next row number.
	std::atomic<int64_t> row_number;
};

class StreamingWindowState : public OperatorState {
public:
	StreamingWindowState() : initialized(false) {
	}

	void Initialize(DataChunk &input, const vector<unique_ptr<Expression>> &expressions) {
		for (idx_t expr_idx = 0; expr_idx < expressions.size(); expr_idx++) {
			auto &expr = *expressions[expr_idx];
			switch (expr.GetExpressionType()) {
			case ExpressionType::WINDOW_FIRST_VALUE: {
				auto &wexpr = (BoundWindowExpression &)expr;
				auto &ref = (BoundReferenceExpression &)*wexpr.children[0];
				const_vectors.push_back(make_unique<Vector>(input.data[ref.index].GetValue(0)));
				break;
			}
			case ExpressionType::WINDOW_PERCENT_RANK: {
				const_vectors.push_back(make_unique<Vector>(Value((double)0)));
				break;
			}
			case ExpressionType::WINDOW_RANK:
			case ExpressionType::WINDOW_RANK_DENSE: {
				const_vectors.push_back(make_unique<Vector>(Value((int64_t)1)));
				break;
			}
			default:
				const_vectors.push_back(nullptr);
			}
		}
		initialized = true;
	}

public:
	bool initialized;
	vector<unique_ptr<Vector>> const_vectors;
};

unique_ptr<GlobalOperatorState> PhysicalStreamingWindow::GetGlobalOperatorState(ClientContext &context) const {
	return make_unique<StreamingWindowGlobalState>();
}

unique_ptr<OperatorState> PhysicalStreamingWindow::GetOperatorState(ExecutionContext &context) const {
	return make_unique<StreamingWindowState>();
}

OperatorResultType PhysicalStreamingWindow::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                    GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &gstate = (StreamingWindowGlobalState &)gstate_p;
	auto &state = (StreamingWindowState &)state_p;
	if (!state.initialized) {
		state.Initialize(input, select_list);
	}
	// Put payload columns in place
	for (idx_t col_idx = 0; col_idx < input.data.size(); col_idx++) {
		chunk.data[col_idx].Reference(input.data[col_idx]);
	}
	// Compute window function
	const idx_t count = input.size();
	for (idx_t expr_idx = 0; expr_idx < select_list.size(); expr_idx++) {
		idx_t col_idx = input.data.size() + expr_idx;
		auto &expr = *select_list[expr_idx];
		switch (expr.GetExpressionType()) {
		case ExpressionType::WINDOW_FIRST_VALUE:
		case ExpressionType::WINDOW_PERCENT_RANK:
		case ExpressionType::WINDOW_RANK:
		case ExpressionType::WINDOW_RANK_DENSE: {
			// Reference constant vector
			chunk.data[col_idx].Reference(*state.const_vectors[expr_idx]);
			break;
		}
		case ExpressionType::WINDOW_ROW_NUMBER: {
			// Set row numbers
			auto rdata = FlatVector::GetData<int64_t>(chunk.data[col_idx]);
			for (idx_t i = 0; i < count; i++) {
				rdata[i] = gstate.row_number + i;
			}
			break;
		}
		default:
			throw NotImplementedException("%s for StreamingWindow", ExpressionTypeToString(expr.GetExpressionType()));
		}
	}
	gstate.row_number += count;
	chunk.SetCardinality(count);
	return OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalStreamingWindow::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < select_list.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += select_list[i]->GetName();
	}
	return result;
}

} // namespace duckdb















#include <algorithm>
#include <cmath>
#include <numeric>

namespace duckdb {

using counts_t = std::vector<size_t>;

//	Global sink state
class WindowGlobalState : public GlobalSinkState {
public:
	WindowGlobalState(Allocator &allocator, const PhysicalWindow &op_p, ClientContext &context)
	    : op(op_p), buffer_manager(BufferManager::GetBufferManager(context)), chunks(allocator),
	      over_collection(allocator), hash_collection(allocator), mode(DBConfig::GetConfig(context).window_mode) {
	}
	const PhysicalWindow &op;
	BufferManager &buffer_manager;
	mutex lock;
	ChunkCollection chunks;
	ChunkCollection over_collection;
	ChunkCollection hash_collection;
	counts_t counts;
	WindowAggregationMode mode;
};

//	Per-thread sink state
class WindowLocalState : public LocalSinkState {
public:
	explicit WindowLocalState(Allocator &allocator, const PhysicalWindow &op_p, const unsigned partition_bits = 10)
	    : op(op_p), chunks(allocator), over_collection(allocator), hash_collection(allocator),
	      partition_count(size_t(1) << partition_bits) {
	}

	const PhysicalWindow &op;
	ChunkCollection chunks;
	ChunkCollection over_collection;
	ChunkCollection hash_collection;
	const size_t partition_count;
	counts_t counts;
};

// Per-thread read state
class WindowOperatorState : public LocalSourceState {
public:
	WindowOperatorState(Allocator &allocator, const PhysicalWindow &op, ExecutionContext &context)
	    : buffer_manager(BufferManager::GetBufferManager(context.client)), chunks(allocator),
	      window_results(allocator) {
		auto &gstate = (WindowGlobalState &)*op.sink_state;
		// initialize thread-local operator state
		partitions = gstate.counts.size();
		next_part = 0;
		position = 0;
	}

	BufferManager &buffer_manager;
	//! The number of partitions to process (0 if there is no partitioning)
	size_t partitions;
	//! The output read position.
	size_t next_part;
	//! The generated input chunks
	ChunkCollection chunks;
	//! The generated output chunks
	ChunkCollection window_results;
	//! The read cursor
	idx_t position;

	unique_ptr<GlobalSortState> global_sort_state;
};

// this implements a sorted window functions variant
PhysicalWindow::PhysicalWindow(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                               idx_t estimated_cardinality, PhysicalOperatorType type)
    : PhysicalOperator(type, move(types), estimated_cardinality), select_list(move(select_list)) {
}

template <typename INPUT_TYPE>
struct ChunkIterator {

	ChunkIterator(ChunkCollection &collection, const idx_t col_idx)
	    : collection(collection), col_idx(col_idx), chunk_begin(0), chunk_end(0), ch_idx(0), data(nullptr),
	      validity(nullptr) {
		Update(0);
	}

	inline void Update(idx_t r) {
		if (r >= chunk_end) {
			ch_idx = collection.LocateChunk(r);
			auto &ch = collection.GetChunk(ch_idx);
			chunk_begin = ch_idx * STANDARD_VECTOR_SIZE;
			chunk_end = chunk_begin + ch.size();
			auto &vector = ch.data[col_idx];
			data = FlatVector::GetData<INPUT_TYPE>(vector);
			validity = &FlatVector::Validity(vector);
		}
	}

	inline bool IsValid(idx_t r) {
		return validity->RowIsValid(r - chunk_begin);
	}

	inline INPUT_TYPE GetValue(idx_t r) {
		return data[r - chunk_begin];
	}

private:
	ChunkCollection &collection;
	idx_t col_idx;
	idx_t chunk_begin;
	idx_t chunk_end;
	idx_t ch_idx;
	const INPUT_TYPE *data;
	ValidityMask *validity;
};

template <typename INPUT_TYPE>
static void MaskTypedColumn(ValidityMask &mask, ChunkCollection &over_collection, const idx_t c) {
	ChunkIterator<INPUT_TYPE> ci(over_collection, c);

	//	Record the first value
	idx_t r = 0;
	auto prev_valid = ci.IsValid(r);
	auto prev = ci.GetValue(r);

	//	Process complete blocks
	const auto count = over_collection.Count();
	const auto entry_count = mask.EntryCount(count);
	for (idx_t entry_idx = 0; entry_idx < entry_count; ++entry_idx) {
		auto validity_entry = mask.GetValidityEntry(entry_idx);

		//	Skip the block if it is all boundaries.
		idx_t next = MinValue<idx_t>(r + ValidityMask::BITS_PER_VALUE, count);
		if (ValidityMask::AllValid(validity_entry)) {
			r = next;
			continue;
		}

		//	Scan the rows in the complete block
		idx_t start = r;
		for (; r < next; ++r) {
			//	Update the chunk for this row
			ci.Update(r);

			auto curr_valid = ci.IsValid(r);
			auto curr = ci.GetValue(r);
			if (!ValidityMask::RowIsValid(validity_entry, r - start)) {
				if (curr_valid != prev_valid || (curr_valid && !Equals::Operation(curr, prev))) {
					mask.SetValidUnsafe(r);
				}
			}
			prev_valid = curr_valid;
			prev = curr;
		}
	}
}

static void MaskColumn(ValidityMask &mask, ChunkCollection &over_collection, const idx_t c) {
	auto &vector = over_collection.GetChunk(0).data[c];
	switch (vector.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		MaskTypedColumn<int8_t>(mask, over_collection, c);
		break;
	case PhysicalType::INT16:
		MaskTypedColumn<int16_t>(mask, over_collection, c);
		break;
	case PhysicalType::INT32:
		MaskTypedColumn<int32_t>(mask, over_collection, c);
		break;
	case PhysicalType::INT64:
		MaskTypedColumn<int64_t>(mask, over_collection, c);
		break;
	case PhysicalType::UINT8:
		MaskTypedColumn<uint8_t>(mask, over_collection, c);
		break;
	case PhysicalType::UINT16:
		MaskTypedColumn<uint16_t>(mask, over_collection, c);
		break;
	case PhysicalType::UINT32:
		MaskTypedColumn<uint32_t>(mask, over_collection, c);
		break;
	case PhysicalType::UINT64:
		MaskTypedColumn<uint64_t>(mask, over_collection, c);
		break;
	case PhysicalType::INT128:
		MaskTypedColumn<hugeint_t>(mask, over_collection, c);
		break;
	case PhysicalType::FLOAT:
		MaskTypedColumn<float>(mask, over_collection, c);
		break;
	case PhysicalType::DOUBLE:
		MaskTypedColumn<double>(mask, over_collection, c);
		break;
	case PhysicalType::VARCHAR:
		MaskTypedColumn<string_t>(mask, over_collection, c);
		break;
	case PhysicalType::INTERVAL:
		MaskTypedColumn<interval_t>(mask, over_collection, c);
		break;
	default:
		throw NotImplementedException("Type for comparison");
		break;
	}
}

static idx_t FindNextStart(const ValidityMask &mask, idx_t l, const idx_t r, idx_t &n) {
	if (mask.AllValid()) {
		auto start = MinValue(l + n - 1, r);
		n -= MinValue(n, r - l);
		return start;
	}

	while (l < r) {
		//	If l is aligned with the start of a block, and the block is blank, then skip forward one block.
		idx_t entry_idx;
		idx_t shift;
		mask.GetEntryIndex(l, entry_idx, shift);

		const auto block = mask.GetValidityEntry(entry_idx);
		if (mask.NoneValid(block) && !shift) {
			l += ValidityMask::BITS_PER_VALUE;
			continue;
		}

		// Loop over the block
		for (; shift < ValidityMask::BITS_PER_VALUE && l < r; ++shift, ++l) {
			if (mask.RowIsValid(block, shift) && --n == 0) {
				return MinValue(l, r);
			}
		}
	}

	//	Didn't find a start so return the end of the range
	return r;
}

static idx_t FindPrevStart(const ValidityMask &mask, const idx_t l, idx_t r, idx_t &n) {
	if (mask.AllValid()) {
		auto start = (r <= l + n) ? l : r - n;
		n -= r - start;
		return start;
	}

	while (l < r) {
		// If r is aligned with the start of a block, and the previous block is blank,
		// then skip backwards one block.
		idx_t entry_idx;
		idx_t shift;
		mask.GetEntryIndex(r - 1, entry_idx, shift);

		const auto block = mask.GetValidityEntry(entry_idx);
		if (mask.NoneValid(block) && (shift + 1 == ValidityMask::BITS_PER_VALUE)) {
			// r is nonzero (> l) and word aligned, so this will not underflow.
			r -= ValidityMask::BITS_PER_VALUE;
			continue;
		}

		// Loop backwards over the block
		// shift is probing r-1 >= l >= 0
		for (++shift; shift-- > 0; --r) {
			if (mask.RowIsValid(block, shift) && --n == 0) {
				return MaxValue(l, r - 1);
			}
		}
	}

	//	Didn't find a start so return the start of the range
	return l;
}

static void MaterializeExpressions(Expression **exprs, idx_t expr_count, ChunkCollection &input,
                                   ChunkCollection &output, bool scalar = false) {
	if (expr_count == 0) {
		return;
	}

	auto &allocator = input.GetAllocator();
	vector<LogicalType> types;
	ExpressionExecutor executor(allocator);
	for (idx_t expr_idx = 0; expr_idx < expr_count; ++expr_idx) {
		types.push_back(exprs[expr_idx]->return_type);
		executor.AddExpression(*exprs[expr_idx]);
	}

	for (idx_t i = 0; i < input.ChunkCount(); i++) {
		DataChunk chunk;
		chunk.Initialize(allocator, types);

		executor.Execute(input.GetChunk(i), chunk);

		chunk.Verify();
		output.Append(chunk);

		if (scalar) {
			break;
		}
	}
}

static void MaterializeExpression(Expression *expr, ChunkCollection &input, ChunkCollection &output,
                                  bool scalar = false) {
	MaterializeExpressions(&expr, 1, input, output, scalar);
}

static void SortCollectionForPartition(WindowOperatorState &state, BoundWindowExpression *wexpr, ChunkCollection &input,
                                       ChunkCollection &over, ChunkCollection *hashes, const hash_t hash_bin,
                                       const hash_t hash_mask) {
	if (input.Count() == 0) {
		return;
	}
	auto &allocator = input.GetAllocator();

	vector<BoundOrderByNode> orders;
	// we sort by both 1) partition by expression list and 2) order by expressions
	for (idx_t prt_idx = 0; prt_idx < wexpr->partitions.size(); prt_idx++) {
		if (wexpr->partitions_stats.empty() || !wexpr->partitions_stats[prt_idx]) {
			orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, wexpr->partitions[prt_idx]->Copy(),
			                    nullptr);
		} else {
			orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, wexpr->partitions[prt_idx]->Copy(),
			                    wexpr->partitions_stats[prt_idx]->Copy());
		}
	}
	for (const auto &order : wexpr->orders) {
		orders.push_back(order.Copy());
	}

	// fuse input and sort collection into one
	// (sorting columns are not decoded, and we need them later)
	auto payload_types = input.Types();
	payload_types.insert(payload_types.end(), over.Types().begin(), over.Types().end());
	DataChunk payload_chunk;
	payload_chunk.InitializeEmpty(payload_types);

	// initialise partitioning memory
	// to minimise copying, we fill up a chunk and then sink it.
	SelectionVector sel;
	DataChunk over_partition;
	DataChunk payload_partition;
	if (hashes) {
		sel.Initialize(STANDARD_VECTOR_SIZE);
		over_partition.Initialize(allocator, over.Types());
		payload_partition.Initialize(allocator, payload_types);
	}

	// initialize row layout for sorting
	RowLayout payload_layout;
	payload_layout.Initialize(payload_types);

	// initialize sorting states
	state.global_sort_state = make_unique<GlobalSortState>(state.buffer_manager, orders, payload_layout);
	auto &global_sort_state = *state.global_sort_state;
	LocalSortState local_sort_state;
	local_sort_state.Initialize(global_sort_state, state.buffer_manager);

	// sink collection chunks into row format
	const idx_t chunk_count = over.ChunkCount();
	for (idx_t i = 0; i < chunk_count; i++) {
		auto &input_chunk = *input.Chunks()[i];
		for (idx_t col_idx = 0; col_idx < input_chunk.ColumnCount(); ++col_idx) {
			payload_chunk.data[col_idx].Reference(input_chunk.data[col_idx]);
		}
		auto &over_chunk = *over.Chunks()[i];
		for (idx_t col_idx = 0; col_idx < over_chunk.ColumnCount(); ++col_idx) {
			payload_chunk.data[input_chunk.ColumnCount() + col_idx].Reference(over_chunk.data[col_idx]);
		}
		payload_chunk.SetCardinality(input_chunk);

		// Extract the hash partition, if any
		if (hashes) {
			auto &hash_chunk = *hashes->Chunks()[i];
			auto hash_size = hash_chunk.size();
			auto hash_data = FlatVector::GetData<hash_t>(hash_chunk.data[0]);
			idx_t bin_size = 0;
			for (idx_t i = 0; i < hash_size; ++i) {
				if ((hash_data[i] & hash_mask) == hash_bin) {
					sel.set_index(bin_size++, i);
				}
			}

			// Flush the partition chunks if we would overflow
			if (over_partition.size() + bin_size > STANDARD_VECTOR_SIZE) {
				local_sort_state.SinkChunk(over_partition, payload_partition);
				over_partition.Reset();
				payload_partition.Reset();
			}

			// Copy the data for each collection.
			if (bin_size) {
				over_partition.Append(over_chunk, false, &sel, bin_size);
				payload_partition.Append(payload_chunk, false, &sel, bin_size);
			}
		} else {
			local_sort_state.SinkChunk(over_chunk, payload_chunk);
		}
	}

	// Flush any ragged partition chunks
	if (over_partition.size() > 0) {
		local_sort_state.SinkChunk(over_partition, payload_partition);
		over_partition.Reset();
		payload_partition.Reset();
	}

	// If there are no hashes, release the input to save memory.
	if (!hashes) {
		over.Reset();
		input.Reset();
	}

	// add local state to global state, which sorts the data
	global_sort_state.AddLocalState(local_sort_state);
	// Prepare for merge phase (in this case we never have a merge phase, but this call is still needed)
	global_sort_state.PrepareMergePhase();
}

static void ScanSortedPartition(WindowOperatorState &state, ChunkCollection &input,
                                const vector<LogicalType> &input_types, ChunkCollection &over,
                                const vector<LogicalType> &over_types) {
	auto &global_sort_state = *state.global_sort_state;

	auto payload_types = input_types;
	payload_types.insert(payload_types.end(), over_types.begin(), over_types.end());
	auto &allocator = input.GetAllocator();

	// scan the sorted row data
	PayloadScanner scanner(*global_sort_state.sorted_blocks[0]->payload_data, global_sort_state);
	for (;;) {
		DataChunk payload_chunk;
		payload_chunk.Initialize(allocator, payload_types);
		payload_chunk.SetCardinality(0);
		scanner.Scan(payload_chunk);
		if (payload_chunk.size() == 0) {
			break;
		}

		// split into two
		DataChunk over_chunk;
		payload_chunk.Split(over_chunk, input_types.size());

		// append back to collection
		input.Append(payload_chunk);
		over.Append(over_chunk);
	}
}

static void HashChunk(Allocator &allocator, counts_t &counts, DataChunk &hash_chunk, DataChunk &sort_chunk,
                      const idx_t partition_cols) {
	const vector<LogicalType> hash_types(1, LogicalType::HASH);
	hash_chunk.Initialize(allocator, hash_types);
	hash_chunk.SetCardinality(sort_chunk);
	auto &hash_vector = hash_chunk.data[0];

	const auto count = sort_chunk.size();
	VectorOperations::Hash(sort_chunk.data[0], hash_vector, count);
	for (idx_t prt_idx = 1; prt_idx < partition_cols; ++prt_idx) {
		VectorOperations::CombineHash(hash_vector, sort_chunk.data[prt_idx], count);
	}

	const auto partition_mask = hash_t(counts.size() - 1);
	auto hashes = FlatVector::GetData<hash_t>(hash_vector);
	if (hash_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		const auto bin = (hashes[0] & partition_mask);
		counts[bin] += count;
	} else {
		for (idx_t i = 0; i < count; ++i) {
			const auto bin = (hashes[i] & partition_mask);
			++counts[bin];
		}
	}
}

static void MaterializeOverForWindow(Allocator &allocator, BoundWindowExpression *wexpr, DataChunk &input_chunk,
                                     DataChunk &over_chunk) {
	vector<LogicalType> over_types;
	ExpressionExecutor executor(allocator);

	// we sort by both 1) partition by expression list and 2) order by expressions
	for (idx_t prt_idx = 0; prt_idx < wexpr->partitions.size(); prt_idx++) {
		auto &pexpr = wexpr->partitions[prt_idx];
		over_types.push_back(pexpr->return_type);
		executor.AddExpression(*pexpr);
	}

	for (idx_t ord_idx = 0; ord_idx < wexpr->orders.size(); ord_idx++) {
		auto &oexpr = wexpr->orders[ord_idx].expression;
		over_types.push_back(oexpr->return_type);
		executor.AddExpression(*oexpr);
	}

	D_ASSERT(!over_types.empty());

	over_chunk.Initialize(allocator, over_types);
	executor.Execute(input_chunk, over_chunk);

	over_chunk.Verify();
}

static inline bool BoundaryNeedsPeer(const WindowBoundary &boundary) {
	switch (boundary) {
	case WindowBoundary::CURRENT_ROW_RANGE:
	case WindowBoundary::EXPR_PRECEDING_RANGE:
	case WindowBoundary::EXPR_FOLLOWING_RANGE:
		return true;
	default:
		return false;
	}
}

struct WindowBoundariesState {
	static inline bool IsScalar(const unique_ptr<Expression> &expr) {
		return expr ? expr->IsScalar() : true;
	}

	explicit WindowBoundariesState(BoundWindowExpression *wexpr)
	    : type(wexpr->type), start_boundary(wexpr->start), end_boundary(wexpr->end),
	      partition_count(wexpr->partitions.size()), order_count(wexpr->orders.size()),
	      range_sense(wexpr->orders.empty() ? OrderType::INVALID : wexpr->orders[0].type),
	      scalar_start(IsScalar(wexpr->start_expr)), scalar_end(IsScalar(wexpr->end_expr)),
	      has_preceding_range(wexpr->start == WindowBoundary::EXPR_PRECEDING_RANGE ||
	                          wexpr->end == WindowBoundary::EXPR_PRECEDING_RANGE),
	      has_following_range(wexpr->start == WindowBoundary::EXPR_FOLLOWING_RANGE ||
	                          wexpr->end == WindowBoundary::EXPR_FOLLOWING_RANGE),
	      needs_peer(BoundaryNeedsPeer(wexpr->end) || wexpr->type == ExpressionType::WINDOW_CUME_DIST) {
	}

	// Cached lookups
	const ExpressionType type;
	const WindowBoundary start_boundary;
	const WindowBoundary end_boundary;
	const idx_t partition_count;
	const idx_t order_count;
	const OrderType range_sense;
	const bool scalar_start;
	const bool scalar_end;
	const bool has_preceding_range;
	const bool has_following_range;
	const bool needs_peer;

	idx_t partition_start = 0;
	idx_t partition_end = 0;
	idx_t peer_start = 0;
	idx_t peer_end = 0;
	idx_t valid_start = 0;
	idx_t valid_end = 0;
	int64_t window_start = -1;
	int64_t window_end = -1;
	bool is_same_partition = false;
	bool is_peer = false;
};

static bool WindowNeedsRank(BoundWindowExpression *wexpr) {
	return wexpr->type == ExpressionType::WINDOW_PERCENT_RANK || wexpr->type == ExpressionType::WINDOW_RANK ||
	       wexpr->type == ExpressionType::WINDOW_RANK_DENSE || wexpr->type == ExpressionType::WINDOW_CUME_DIST;
}

template <typename T>
static T GetCell(ChunkCollection &collection, idx_t column, idx_t index) {
	D_ASSERT(collection.ColumnCount() > column);
	auto &chunk = collection.GetChunkForRow(index);
	auto &source = chunk.data[column];
	const auto source_offset = index % STANDARD_VECTOR_SIZE;
	const auto data = FlatVector::GetData<T>(source);
	return data[source_offset];
}

static bool CellIsNull(ChunkCollection &collection, idx_t column, idx_t index) {
	D_ASSERT(collection.ColumnCount() > column);
	auto &chunk = collection.GetChunkForRow(index);
	auto &source = chunk.data[column];
	const auto source_offset = index % STANDARD_VECTOR_SIZE;
	return FlatVector::IsNull(source, source_offset);
}

template <typename T>
struct ChunkCollectionIterator {
	using iterator = ChunkCollectionIterator<T>;
	using iterator_category = std::forward_iterator_tag;
	using difference_type = std::ptrdiff_t;
	using value_type = T;
	using reference = T;
	using pointer = idx_t;

	ChunkCollectionIterator(ChunkCollection &coll_p, idx_t col_no_p, pointer pos_p = 0)
	    : coll(&coll_p), col_no(col_no_p), pos(pos_p) {
	}

	inline reference operator*() const {
		return GetCell<T>(*coll, col_no, pos);
	}
	inline explicit operator pointer() const {
		return pos;
	}

	inline iterator &operator++() {
		++pos;
		return *this;
	}
	inline iterator operator++(int) {
		auto result = *this;
		++(*this);
		return result;
	}

	friend inline bool operator==(const iterator &a, const iterator &b) {
		return a.pos == b.pos;
	}
	friend inline bool operator!=(const iterator &a, const iterator &b) {
		return a.pos != b.pos;
	}

private:
	ChunkCollection *coll;
	idx_t col_no;
	pointer pos;
};

template <typename T, typename OP>
struct OperationCompare : public std::function<bool(T, T)> {
	inline bool operator()(const T &lhs, const T &val) const {
		return OP::template Operation(lhs, val);
	}
};

template <typename T, typename OP, bool FROM>
static idx_t FindTypedRangeBound(ChunkCollection &over, const idx_t order_col, const idx_t order_begin,
                                 const idx_t order_end, ChunkCollection &boundary, const idx_t boundary_row) {
	D_ASSERT(!CellIsNull(boundary, 0, boundary_row));
	const auto val = GetCell<T>(boundary, 0, boundary_row);

	OperationCompare<T, OP> comp;
	ChunkCollectionIterator<T> begin(over, order_col, order_begin);
	ChunkCollectionIterator<T> end(over, order_col, order_end);
	if (FROM) {
		return idx_t(std::lower_bound(begin, end, val, comp));
	} else {
		return idx_t(std::upper_bound(begin, end, val, comp));
	}
}

template <typename OP, bool FROM>
static idx_t FindRangeBound(ChunkCollection &over, const idx_t order_col, const idx_t order_begin,
                            const idx_t order_end, ChunkCollection &boundary, const idx_t expr_idx) {
	const auto &over_types = over.Types();
	D_ASSERT(over_types.size() > order_col);
	D_ASSERT(boundary.Types().size() == 1);
	D_ASSERT(boundary.Types()[0] == over_types[order_col]);

	switch (over_types[order_col].InternalType()) {
	case PhysicalType::INT8:
		return FindTypedRangeBound<int8_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::INT16:
		return FindTypedRangeBound<int16_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::INT32:
		return FindTypedRangeBound<int32_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::INT64:
		return FindTypedRangeBound<int64_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::UINT8:
		return FindTypedRangeBound<uint8_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::UINT16:
		return FindTypedRangeBound<uint16_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::UINT32:
		return FindTypedRangeBound<uint32_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::UINT64:
		return FindTypedRangeBound<uint64_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::INT128:
		return FindTypedRangeBound<hugeint_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::FLOAT:
		return FindTypedRangeBound<float, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::DOUBLE:
		return FindTypedRangeBound<double, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::INTERVAL:
		return FindTypedRangeBound<interval_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	default:
		throw InternalException("Unsupported column type for RANGE");
	}
}

template <bool FROM>
static idx_t FindOrderedRangeBound(ChunkCollection &over, const idx_t order_col, const OrderType range_sense,
                                   const idx_t order_begin, const idx_t order_end, ChunkCollection &boundary,
                                   const idx_t expr_idx) {
	switch (range_sense) {
	case OrderType::ASCENDING:
		return FindRangeBound<LessThan, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case OrderType::DESCENDING:
		return FindRangeBound<GreaterThan, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	default:
		throw InternalException("Unsupported ORDER BY sense for RANGE");
	}
}

static void UpdateWindowBoundaries(WindowBoundariesState &bounds, const idx_t input_size, const idx_t row_idx,
                                   ChunkCollection &over_collection, ChunkCollection &boundary_start_collection,
                                   ChunkCollection &boundary_end_collection, const ValidityMask &partition_mask,
                                   const ValidityMask &order_mask) {

	// RANGE sorting parameters
	const auto order_col = bounds.partition_count;

	if (bounds.partition_count + bounds.order_count > 0) {

		// determine partition and peer group boundaries to ultimately figure out window size
		bounds.is_same_partition = !partition_mask.RowIsValidUnsafe(row_idx);
		bounds.is_peer = !order_mask.RowIsValidUnsafe(row_idx);

		// when the partition changes, recompute the boundaries
		if (!bounds.is_same_partition) {
			bounds.partition_start = row_idx;
			bounds.peer_start = row_idx;

			// find end of partition
			bounds.partition_end = input_size;
			if (bounds.partition_count) {
				idx_t n = 1;
				bounds.partition_end = FindNextStart(partition_mask, bounds.partition_start + 1, input_size, n);
			}

			// Find valid ordering values for the new partition
			// so we can exclude NULLs from RANGE expression computations
			bounds.valid_start = bounds.partition_start;
			bounds.valid_end = bounds.partition_end;

			if ((bounds.valid_start < bounds.valid_end) && bounds.has_preceding_range) {
				// Exclude any leading NULLs
				if (CellIsNull(over_collection, order_col, bounds.valid_start)) {
					idx_t n = 1;
					bounds.valid_start = FindNextStart(order_mask, bounds.valid_start + 1, bounds.valid_end, n);
				}
			}

			if ((bounds.valid_start < bounds.valid_end) && bounds.has_following_range) {
				// Exclude any trailing NULLs
				if (CellIsNull(over_collection, order_col, bounds.valid_end - 1)) {
					idx_t n = 1;
					bounds.valid_end = FindPrevStart(order_mask, bounds.valid_start, bounds.valid_end, n);
				}
			}

		} else if (!bounds.is_peer) {
			bounds.peer_start = row_idx;
		}

		if (bounds.needs_peer) {
			bounds.peer_end = bounds.partition_end;
			if (bounds.order_count) {
				idx_t n = 1;
				bounds.peer_end = FindNextStart(order_mask, bounds.peer_start + 1, bounds.partition_end, n);
			}
		}

	} else {
		bounds.is_same_partition = false;
		bounds.is_peer = true;
		bounds.partition_end = input_size;
		bounds.peer_end = bounds.partition_end;
	}

	// determine window boundaries depending on the type of expression
	bounds.window_start = -1;
	bounds.window_end = -1;

	switch (bounds.start_boundary) {
	case WindowBoundary::UNBOUNDED_PRECEDING:
		bounds.window_start = bounds.partition_start;
		break;
	case WindowBoundary::CURRENT_ROW_ROWS:
		bounds.window_start = row_idx;
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		bounds.window_start = bounds.peer_start;
		break;
	case WindowBoundary::EXPR_PRECEDING_ROWS: {
		bounds.window_start =
		    (int64_t)row_idx - GetCell<int64_t>(boundary_start_collection, 0, bounds.scalar_start ? 0 : row_idx);
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_ROWS: {
		bounds.window_start =
		    row_idx + GetCell<int64_t>(boundary_start_collection, 0, bounds.scalar_start ? 0 : row_idx);
		break;
	}
	case WindowBoundary::EXPR_PRECEDING_RANGE: {
		const auto expr_idx = bounds.scalar_start ? 0 : row_idx;
		if (CellIsNull(boundary_start_collection, 0, expr_idx)) {
			bounds.window_start = bounds.peer_start;
		} else {
			bounds.window_start =
			    FindOrderedRangeBound<true>(over_collection, order_col, bounds.range_sense, bounds.valid_start, row_idx,
			                                boundary_start_collection, expr_idx);
		}
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_RANGE: {
		const auto expr_idx = bounds.scalar_start ? 0 : row_idx;
		if (CellIsNull(boundary_start_collection, 0, expr_idx)) {
			bounds.window_start = bounds.peer_start;
		} else {
			bounds.window_start = FindOrderedRangeBound<true>(over_collection, order_col, bounds.range_sense, row_idx,
			                                                  bounds.valid_end, boundary_start_collection, expr_idx);
		}
		break;
	}
	default:
		throw InternalException("Unsupported window start boundary");
	}

	switch (bounds.end_boundary) {
	case WindowBoundary::CURRENT_ROW_ROWS:
		bounds.window_end = row_idx + 1;
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		bounds.window_end = bounds.peer_end;
		break;
	case WindowBoundary::UNBOUNDED_FOLLOWING:
		bounds.window_end = bounds.partition_end;
		break;
	case WindowBoundary::EXPR_PRECEDING_ROWS:
		bounds.window_end =
		    (int64_t)row_idx - GetCell<int64_t>(boundary_end_collection, 0, bounds.scalar_end ? 0 : row_idx) + 1;
		break;
	case WindowBoundary::EXPR_FOLLOWING_ROWS:
		bounds.window_end = row_idx + GetCell<int64_t>(boundary_end_collection, 0, bounds.scalar_end ? 0 : row_idx) + 1;
		break;
	case WindowBoundary::EXPR_PRECEDING_RANGE: {
		const auto expr_idx = bounds.scalar_end ? 0 : row_idx;
		if (CellIsNull(boundary_end_collection, 0, expr_idx)) {
			bounds.window_end = bounds.peer_end;
		} else {
			bounds.window_end =
			    FindOrderedRangeBound<false>(over_collection, order_col, bounds.range_sense, bounds.valid_start,
			                                 row_idx, boundary_end_collection, expr_idx);
		}
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_RANGE: {
		const auto expr_idx = bounds.scalar_end ? 0 : row_idx;
		if (CellIsNull(boundary_end_collection, 0, expr_idx)) {
			bounds.window_end = bounds.peer_end;
		} else {
			bounds.window_end = FindOrderedRangeBound<false>(over_collection, order_col, bounds.range_sense, row_idx,
			                                                 bounds.valid_end, boundary_end_collection, expr_idx);
		}
		break;
	}
	default:
		throw InternalException("Unsupported window end boundary");
	}

	// clamp windows to partitions if they should exceed
	if (bounds.window_start < (int64_t)bounds.partition_start) {
		bounds.window_start = bounds.partition_start;
	}
	if (bounds.window_start > (int64_t)bounds.partition_end) {
		bounds.window_start = bounds.partition_end;
	}
	if (bounds.window_end < (int64_t)bounds.partition_start) {
		bounds.window_end = bounds.partition_start;
	}
	if (bounds.window_end > (int64_t)bounds.partition_end) {
		bounds.window_end = bounds.partition_end;
	}

	if (bounds.window_start < 0 || bounds.window_end < 0) {
		throw InternalException("Failed to compute window boundaries");
	}
}

static void ComputeWindowExpression(BoundWindowExpression *wexpr, ChunkCollection &input, ChunkCollection &output,
                                    ChunkCollection &over, const ValidityMask &partition_mask,
                                    const ValidityMask &order_mask, WindowAggregationMode mode) {

	// TODO we could evaluate those expressions in parallel
	auto &allocator = input.GetAllocator();
	// evaluate inner expressions of window functions, could be more complex
	ChunkCollection payload_collection(allocator);
	vector<Expression *> exprs;
	for (auto &child : wexpr->children) {
		exprs.push_back(child.get());
	}
	// TODO: child may be a scalar, don't need to materialize the whole collection then
	MaterializeExpressions(exprs.data(), exprs.size(), input, payload_collection);

	ChunkCollection leadlag_offset_collection(allocator);
	ChunkCollection leadlag_default_collection(allocator);
	if (wexpr->type == ExpressionType::WINDOW_LEAD || wexpr->type == ExpressionType::WINDOW_LAG) {
		if (wexpr->offset_expr) {
			MaterializeExpression(wexpr->offset_expr.get(), input, leadlag_offset_collection,
			                      wexpr->offset_expr->IsScalar());
		}
		if (wexpr->default_expr) {
			MaterializeExpression(wexpr->default_expr.get(), input, leadlag_default_collection,
			                      wexpr->default_expr->IsScalar());
		}
	}

	// evaluate the FILTER clause and stuff it into a large mask for compactness and reuse
	ValidityMask filter_mask;
	vector<validity_t> filter_bits;
	if (wexpr->filter_expr) {
		// 	Start with all invalid and set the ones that pass
		filter_bits.resize(ValidityMask::ValidityMaskSize(input.Count()), 0);
		filter_mask.Initialize(filter_bits.data());
		ExpressionExecutor filter_execution(allocator, *wexpr->filter_expr);
		SelectionVector true_sel(STANDARD_VECTOR_SIZE);
		idx_t base_idx = 0;
		for (auto &chunk : input.Chunks()) {
			const auto filtered = filter_execution.SelectExpression(*chunk, true_sel);
			for (idx_t f = 0; f < filtered; ++f) {
				filter_mask.SetValid(base_idx + true_sel[f]);
			}
			base_idx += chunk->size();
		}
	}

	// evaluate boundaries if present. Parser has checked boundary types.
	ChunkCollection boundary_start_collection(allocator);
	if (wexpr->start_expr) {
		MaterializeExpression(wexpr->start_expr.get(), input, boundary_start_collection, wexpr->start_expr->IsScalar());
	}

	ChunkCollection boundary_end_collection(allocator);
	if (wexpr->end_expr) {
		MaterializeExpression(wexpr->end_expr.get(), input, boundary_end_collection, wexpr->end_expr->IsScalar());
	}

	// Set up a validity mask for IGNORE NULLS
	ValidityMask ignore_nulls;
	if (wexpr->ignore_nulls) {
		switch (wexpr->type) {
		case ExpressionType::WINDOW_LEAD:
		case ExpressionType::WINDOW_LAG:
		case ExpressionType::WINDOW_FIRST_VALUE:
		case ExpressionType::WINDOW_LAST_VALUE:
		case ExpressionType::WINDOW_NTH_VALUE: {
			idx_t pos = 0;
			for (auto &chunk : payload_collection.Chunks()) {
				const auto count = chunk->size();
				VectorData vdata;
				chunk->data[0].Orrify(count, vdata);
				if (!vdata.validity.AllValid()) {
					//	Lazily materialise the contents when we find the first NULL
					if (ignore_nulls.AllValid()) {
						ignore_nulls.Initialize(payload_collection.Count());
					}
					// Write to the current position
					// Chunks in a collection are full, so we don't have to worry about raggedness
					auto dst = ignore_nulls.GetData() + ignore_nulls.EntryCount(pos);
					auto src = vdata.validity.GetData();
					for (auto entry_count = vdata.validity.EntryCount(count); entry_count-- > 0;) {
						*dst++ = *src++;
					}
				}
				pos += count;
			}
			break;
		}
		default:
			break;
		}
	}

	// build a segment tree for frame-adhering aggregates
	// see http://www.vldb.org/pvldb/vol8/p1058-leis.pdf
	unique_ptr<WindowSegmentTree> segment_tree = nullptr;

	if (wexpr->aggregate) {
		segment_tree = make_unique<WindowSegmentTree>(*(wexpr->aggregate), wexpr->bind_info.get(), wexpr->return_type,
		                                              &payload_collection, filter_mask, mode);
	}

	WindowBoundariesState bounds(wexpr);
	uint64_t dense_rank = 1, rank_equal = 0, rank = 1;

	// this is the main loop, go through all sorted rows and compute window function result
	const vector<LogicalType> output_types(1, wexpr->return_type);
	DataChunk output_chunk;
	output_chunk.Initialize(allocator, output_types);
	for (idx_t row_idx = 0; row_idx < input.Count(); row_idx++) {
		// Grow the chunk if necessary.
		const auto output_offset = row_idx % STANDARD_VECTOR_SIZE;
		if (output_offset == 0) {
			output.Append(output_chunk);
			output_chunk.Reset();
			output_chunk.SetCardinality(MinValue(idx_t(STANDARD_VECTOR_SIZE), input.Count() - row_idx));
		}
		auto &result = output_chunk.data[0];

		// special case, OVER (), aggregate over everything
		UpdateWindowBoundaries(bounds, input.Count(), row_idx, over, boundary_start_collection, boundary_end_collection,
		                       partition_mask, order_mask);
		if (WindowNeedsRank(wexpr)) {
			if (!bounds.is_same_partition || row_idx == 0) { // special case for first row, need to init
				dense_rank = 1;
				rank = 1;
				rank_equal = 0;
			} else if (!bounds.is_peer) {
				dense_rank++;
				rank += rank_equal;
				rank_equal = 0;
			}
			rank_equal++;
		}

		// if no values are read for window, result is NULL
		if (bounds.window_start >= bounds.window_end) {
			FlatVector::SetNull(result, output_offset, true);
			continue;
		}

		switch (wexpr->type) {
		case ExpressionType::WINDOW_AGGREGATE: {
			segment_tree->Compute(result, output_offset, bounds.window_start, bounds.window_end);
			break;
		}
		case ExpressionType::WINDOW_ROW_NUMBER: {
			auto rdata = FlatVector::GetData<int64_t>(result);
			rdata[output_offset] = row_idx - bounds.partition_start + 1;
			break;
		}
		case ExpressionType::WINDOW_RANK_DENSE: {
			auto rdata = FlatVector::GetData<int64_t>(result);
			rdata[output_offset] = dense_rank;
			break;
		}
		case ExpressionType::WINDOW_RANK: {
			auto rdata = FlatVector::GetData<int64_t>(result);
			rdata[output_offset] = rank;
			break;
		}
		case ExpressionType::WINDOW_PERCENT_RANK: {
			int64_t denom = (int64_t)bounds.partition_end - bounds.partition_start - 1;
			double percent_rank = denom > 0 ? ((double)rank - 1) / denom : 0;
			auto rdata = FlatVector::GetData<double>(result);
			rdata[output_offset] = percent_rank;
			break;
		}
		case ExpressionType::WINDOW_CUME_DIST: {
			int64_t denom = (int64_t)bounds.partition_end - bounds.partition_start;
			double cume_dist = denom > 0 ? ((double)(bounds.peer_end - bounds.partition_start)) / denom : 0;
			auto rdata = FlatVector::GetData<double>(result);
			rdata[output_offset] = cume_dist;
			break;
		}
		case ExpressionType::WINDOW_NTILE: {
			D_ASSERT(payload_collection.ColumnCount() == 1);
			auto n_param = GetCell<int64_t>(payload_collection, 0, row_idx);
			// With thanks from SQLite's ntileValueFunc()
			int64_t n_total = bounds.partition_end - bounds.partition_start;
			if (n_param > n_total) {
				// more groups allowed than we have values
				// map every entry to a unique group
				n_param = n_total;
			}
			int64_t n_size = (n_total / n_param);
			// find the row idx within the group
			D_ASSERT(row_idx >= bounds.partition_start);
			int64_t adjusted_row_idx = row_idx - bounds.partition_start;
			// now compute the ntile
			int64_t n_large = n_total - n_param * n_size;
			int64_t i_small = n_large * (n_size + 1);
			int64_t result_ntile;

			D_ASSERT((n_large * (n_size + 1) + (n_param - n_large) * n_size) == n_total);

			if (adjusted_row_idx < i_small) {
				result_ntile = 1 + adjusted_row_idx / (n_size + 1);
			} else {
				result_ntile = 1 + n_large + (adjusted_row_idx - i_small) / n_size;
			}
			// result has to be between [1, NTILE]
			D_ASSERT(result_ntile >= 1 && result_ntile <= n_param);
			auto rdata = FlatVector::GetData<int64_t>(result);
			rdata[output_offset] = result_ntile;
			break;
		}
		case ExpressionType::WINDOW_LEAD:
		case ExpressionType::WINDOW_LAG: {
			int64_t offset = 1;
			if (wexpr->offset_expr) {
				offset = GetCell<int64_t>(leadlag_offset_collection, 0, wexpr->offset_expr->IsScalar() ? 0 : row_idx);
			}
			int64_t val_idx = (int64_t)row_idx;
			if (wexpr->type == ExpressionType::WINDOW_LEAD) {
				val_idx += offset;
			} else {
				val_idx -= offset;
			}

			idx_t delta = 0;
			if (val_idx < (int64_t)row_idx) {
				// Count backwards
				delta = idx_t(row_idx - val_idx);
				val_idx = FindPrevStart(ignore_nulls, bounds.partition_start, row_idx, delta);
			} else if (val_idx > (int64_t)row_idx) {
				delta = idx_t(val_idx - row_idx);
				val_idx = FindNextStart(ignore_nulls, row_idx + 1, bounds.partition_end, delta);
			}
			// else offset is zero, so don't move.

			if (!delta) {
				payload_collection.CopyCell(0, val_idx, result, output_offset);
			} else if (wexpr->default_expr) {
				const auto source_row = wexpr->default_expr->IsScalar() ? 0 : row_idx;
				leadlag_default_collection.CopyCell(0, source_row, result, output_offset);
			} else {
				FlatVector::SetNull(result, output_offset, true);
			}
			break;
		}
		case ExpressionType::WINDOW_FIRST_VALUE: {
			idx_t n = 1;
			const auto first_idx = FindNextStart(ignore_nulls, bounds.window_start, bounds.window_end, n);
			payload_collection.CopyCell(0, first_idx, result, output_offset);
			break;
		}
		case ExpressionType::WINDOW_LAST_VALUE: {
			idx_t n = 1;
			payload_collection.CopyCell(0, FindPrevStart(ignore_nulls, bounds.window_start, bounds.window_end, n),
			                            result, output_offset);
			break;
		}
		case ExpressionType::WINDOW_NTH_VALUE: {
			D_ASSERT(payload_collection.ColumnCount() == 2);
			// Returns value evaluated at the row that is the n'th row of the window frame (counting from 1);
			// returns NULL if there is no such row.
			if (CellIsNull(payload_collection, 1, row_idx)) {
				FlatVector::SetNull(result, output_offset, true);
			} else {
				auto n_param = GetCell<int64_t>(payload_collection, 1, row_idx);
				if (n_param < 1) {
					FlatVector::SetNull(result, output_offset, true);
				} else {
					auto n = idx_t(n_param);
					const auto nth_index = FindNextStart(ignore_nulls, bounds.window_start, bounds.window_end, n);
					if (!n) {
						payload_collection.CopyCell(0, nth_index, result, output_offset);
					} else {
						FlatVector::SetNull(result, output_offset, true);
					}
				}
			}
			break;
		}
		default:
			throw InternalException("Window aggregate type %s", ExpressionTypeToString(wexpr->type));
		}
	}

	// Push the last chunk
	output.Append(output_chunk);
}

using WindowExpressions = vector<BoundWindowExpression *>;

static void ComputeWindowExpressions(WindowExpressions &window_exprs, ChunkCollection &input,
                                     ChunkCollection &window_results, ChunkCollection &over,
                                     WindowAggregationMode mode) {
	//	Idempotency
	if (input.Count() == 0) {
		return;
	}
	//	Pick out a function for the OVER clause
	auto over_expr = window_exprs[0];

	//	Set bits for the start of each partition
	vector<validity_t> partition_bits(ValidityMask::EntryCount(input.Count()), 0);
	ValidityMask partition_mask(partition_bits.data());
	partition_mask.SetValid(0);

	for (idx_t c = 0; c < over_expr->partitions.size(); ++c) {
		MaskColumn(partition_mask, over, c);
	}

	//	Set bits for the start of each peer group.
	//	Partitions also break peer groups, so start with the partition bits.
	const auto sort_col_count = over_expr->partitions.size() + over_expr->orders.size();
	ValidityMask order_mask(partition_mask, input.Count());
	for (idx_t c = over_expr->partitions.size(); c < sort_col_count; ++c) {
		MaskColumn(order_mask, over, c);
	}

	//	Compute the functions columnwise
	for (idx_t expr_idx = 0; expr_idx < window_exprs.size(); ++expr_idx) {
		ChunkCollection output(input.GetAllocator());
		ComputeWindowExpression(window_exprs[expr_idx], input, output, over, partition_mask, order_mask, mode);
		window_results.Fuse(output);
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
static void GeneratePartition(Allocator &allocator, WindowOperatorState &state, WindowGlobalState &gstate,
                              const idx_t hash_bin) {
	auto &op = (PhysicalWindow &)gstate.op;
	WindowExpressions window_exprs;
	for (idx_t expr_idx = 0; expr_idx < op.select_list.size(); ++expr_idx) {
		D_ASSERT(op.select_list[expr_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
		auto wexpr = reinterpret_cast<BoundWindowExpression *>(op.select_list[expr_idx].get());
		window_exprs.emplace_back(wexpr);
	}

	//	Get rid of any stale data
	state.chunks.Reset();
	state.window_results.Reset();
	state.position = 0;
	state.global_sort_state = nullptr;

	//	Pick out a function for the OVER clause
	auto over_expr = window_exprs[0];

	// There are three types of partitions:
	// 1. No partition (no sorting)
	// 2. One partition (sorting, but no hashing)
	// 3. Multiple partitions (sorting and hashing)
	const auto input_types = gstate.chunks.Types();
	const auto over_types = gstate.over_collection.Types();

	if (gstate.counts.empty() && hash_bin == 0) {
		ChunkCollection &input = gstate.chunks;
		ChunkCollection output(allocator);
		ChunkCollection &over = gstate.over_collection;

		const auto has_sorting = over_expr->partitions.size() + over_expr->orders.size();
		if (has_sorting && input.Count() > 0) {
			// 2. One partition
			SortCollectionForPartition(state, over_expr, input, over, nullptr, 0, 0);

			// Overwrite the collections with the sorted data
			ScanSortedPartition(state, input, input_types, over, over_types);
		}

		ComputeWindowExpressions(window_exprs, input, output, over, gstate.mode);
		state.chunks.Merge(input);
		state.window_results.Merge(output);

	} else if (hash_bin < gstate.counts.size() && gstate.counts[hash_bin] > 0) {
		// 3. Multiple partitions
		const auto hash_mask = hash_t(gstate.counts.size() - 1);
		SortCollectionForPartition(state, over_expr, gstate.chunks, gstate.over_collection, &gstate.hash_collection,
		                           hash_bin, hash_mask);

		// Scan the sorted data into new Collections
		ChunkCollection input(allocator);
		ChunkCollection output(allocator);
		ChunkCollection over(allocator);
		ScanSortedPartition(state, input, input_types, over, over_types);

		ComputeWindowExpressions(window_exprs, input, output, over, gstate.mode);
		state.chunks.Merge(input);
		state.window_results.Merge(output);
	}
}

static void Scan(WindowOperatorState &state, DataChunk &chunk) {
	ChunkCollection &big_data = state.chunks;
	ChunkCollection &window_results = state.window_results;

	if (state.position >= big_data.Count()) {
		return;
	}

	// just return what was computed before, appending the result cols of the window expressions at the end
	auto &proj_ch = big_data.GetChunkForRow(state.position);
	auto &wind_ch = window_results.GetChunkForRow(state.position);

	idx_t out_idx = 0;
	D_ASSERT(proj_ch.size() == wind_ch.size());
	chunk.SetCardinality(proj_ch);
	for (idx_t col_idx = 0; col_idx < proj_ch.ColumnCount(); col_idx++) {
		chunk.data[out_idx++].Reference(proj_ch.data[col_idx]);
	}
	for (idx_t col_idx = 0; col_idx < wind_ch.ColumnCount(); col_idx++) {
		chunk.data[out_idx++].Reference(wind_ch.data[col_idx]);
	}
	chunk.Verify();

	state.position += STANDARD_VECTOR_SIZE;
}

SinkResultType PhysicalWindow::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate_p,
                                    DataChunk &input) const {
	auto &lstate = (WindowLocalState &)lstate_p;
	lstate.chunks.Append(input);

	// Compute the over columns and the hash values for this block (if any)
	const auto over_idx = 0;
	auto over_expr = reinterpret_cast<BoundWindowExpression *>(select_list[over_idx].get());

	auto &allocator = Allocator::Get(context.client);
	const auto sort_col_count = over_expr->partitions.size() + over_expr->orders.size();
	if (sort_col_count > 0) {
		DataChunk over_chunk;
		MaterializeOverForWindow(allocator, over_expr, input, over_chunk);

		if (!over_expr->partitions.empty()) {
			if (lstate.counts.empty()) {
				lstate.counts.resize(lstate.partition_count, 0);
			}

			DataChunk hash_chunk;
			HashChunk(allocator, lstate.counts, hash_chunk, over_chunk, over_expr->partitions.size());
			lstate.hash_collection.Append(hash_chunk);
			D_ASSERT(lstate.chunks.Count() == lstate.hash_collection.Count());
		}

		lstate.over_collection.Append(over_chunk);
		D_ASSERT(lstate.chunks.Count() == lstate.over_collection.Count());
	}
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalWindow::Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const {
	auto &lstate = (WindowLocalState &)lstate_p;
	if (lstate.chunks.Count() == 0) {
		return;
	}
	auto &gstate = (WindowGlobalState &)gstate_p;
	lock_guard<mutex> glock(gstate.lock);
	gstate.chunks.Merge(lstate.chunks);
	gstate.over_collection.Merge(lstate.over_collection);
	gstate.hash_collection.Merge(lstate.hash_collection);
	if (gstate.counts.empty()) {
		gstate.counts = lstate.counts;
	} else {
		D_ASSERT(gstate.counts.size() == lstate.counts.size());
		for (idx_t i = 0; i < gstate.counts.size(); ++i) {
			gstate.counts[i] += lstate.counts[i];
		}
	}
}

unique_ptr<LocalSinkState> PhysicalWindow::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<WindowLocalState>(Allocator::Get(context.client), *this);
}

unique_ptr<GlobalSinkState> PhysicalWindow::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<WindowGlobalState>(Allocator::Get(context), *this, context);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class WindowGlobalSourceState : public GlobalSourceState {
public:
	explicit WindowGlobalSourceState(const PhysicalWindow &op) : op(op), next_part(0) {
	}

	const PhysicalWindow &op;
	//! The output read position.
	atomic<idx_t> next_part;

public:
	idx_t MaxThreads() override {
		auto &state = (WindowGlobalState &)*op.sink_state;

		// If there is only one partition, we have to process it on one thread.
		if (state.counts.empty()) {
			return 1;
		}

		idx_t max_threads = 0;
		for (const auto count : state.counts) {
			if (count > 0) {
				max_threads++;
			}
		}

		return max_threads;
	}
};

unique_ptr<LocalSourceState> PhysicalWindow::GetLocalSourceState(ExecutionContext &context,
                                                                 GlobalSourceState &gstate) const {
	return make_unique<WindowOperatorState>(Allocator::Get(context.client), *this, context);
}

unique_ptr<GlobalSourceState> PhysicalWindow::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<WindowGlobalSourceState>(*this);
}

void PhysicalWindow::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                             LocalSourceState &lstate_p) const {
	auto &state = (WindowOperatorState &)lstate_p;
	auto &global_source = (WindowGlobalSourceState &)gstate_p;
	auto &gstate = (WindowGlobalState &)*sink_state;

	do {
		if (state.position >= state.chunks.Count()) {
			auto hash_bin = global_source.next_part++;
			for (; hash_bin < state.partitions; hash_bin = global_source.next_part++) {
				if (gstate.counts[hash_bin] > 0) {
					break;
				}
			}
			GeneratePartition(Allocator::Get(context.client), state, gstate, hash_bin);
		}
		Scan(state, chunk);
		if (chunk.size() != 0) {
			return;
		} else {
			break;
		}
	} while (true);
	D_ASSERT(chunk.size() == 0);
}

string PhysicalWindow::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < select_list.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += select_list[i]->GetName();
	}
	return result;
}

} // namespace duckdb




namespace duckdb {

PhysicalFilter::PhysicalFilter(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                               idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::FILTER, move(types), estimated_cardinality) {
	D_ASSERT(select_list.size() > 0);
	if (select_list.size() > 1) {
		// create a big AND out of the expressions
		auto conjunction = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		for (auto &expr : select_list) {
			conjunction->children.push_back(move(expr));
		}
		expression = move(conjunction);
	} else {
		expression = move(select_list[0]);
	}
}

class FilterState : public OperatorState {
public:
	explicit FilterState(ExecutionContext &context, Expression &expr)
	    : executor(Allocator::Get(context.client), expr), sel(STANDARD_VECTOR_SIZE) {
	}

	ExpressionExecutor executor;
	SelectionVector sel;

public:
	void Finalize(PhysicalOperator *op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, &executor, "filter", 0);
	}
};

unique_ptr<OperatorState> PhysicalFilter::GetOperatorState(ExecutionContext &context) const {
	return make_unique<FilterState>(context, *expression);
}

OperatorResultType PhysicalFilter::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                           GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = (FilterState &)state_p;
	idx_t result_count = state.executor.SelectExpression(input, state.sel);
	if (result_count == input.size()) {
		// nothing was filtered: skip adding any selection vectors
		chunk.Reference(input);
	} else {
		chunk.Slice(input, state.sel, result_count);
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalFilter::ParamsToString() const {
	return expression->GetName();
}

} // namespace duckdb





namespace duckdb {

PhysicalBatchCollector::PhysicalBatchCollector(PreparedStatementData &data) : PhysicalResultCollector(data) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class BatchCollectorGlobalState : public GlobalSinkState {
public:
	explicit BatchCollectorGlobalState(Allocator &allocator) : data(allocator) {
	}

	mutex glock;
	BatchedChunkCollection data;
	unique_ptr<MaterializedQueryResult> result;
};

class BatchCollectorLocalState : public LocalSinkState {
public:
	explicit BatchCollectorLocalState(Allocator &allocator) : data(allocator) {
	}

	BatchedChunkCollection data;
};

SinkResultType PhysicalBatchCollector::Sink(ExecutionContext &context, GlobalSinkState &gstate,
                                            LocalSinkState &lstate_p, DataChunk &input) const {
	auto &state = (BatchCollectorLocalState &)lstate_p;
	state.data.Append(input, state.batch_index);
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalBatchCollector::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                     LocalSinkState &lstate_p) const {
	auto &gstate = (BatchCollectorGlobalState &)gstate_p;
	auto &state = (BatchCollectorLocalState &)lstate_p;

	lock_guard<mutex> lock(gstate.glock);
	gstate.data.Merge(state.data);
}

SinkFinalizeType PhysicalBatchCollector::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                  GlobalSinkState &gstate_p) const {
	auto &gstate = (BatchCollectorGlobalState &)gstate_p;
	auto result =
	    make_unique<MaterializedQueryResult>(statement_type, properties, types, names, context.shared_from_this());
	DataChunk output;
	output.Initialize(BufferAllocator::Get(context), types);

	BatchedChunkScanState state;
	gstate.data.InitializeScan(state);
	while (true) {
		output.Reset();
		gstate.data.Scan(state, output);
		if (output.size() == 0) {
			break;
		}
		result->collection.Append(output);
	}

	gstate.result = move(result);
	return SinkFinalizeType::READY;
}

unique_ptr<LocalSinkState> PhysicalBatchCollector::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<BatchCollectorLocalState>(Allocator::DefaultAllocator());
}

unique_ptr<GlobalSinkState> PhysicalBatchCollector::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<BatchCollectorGlobalState>(Allocator::DefaultAllocator());
}

unique_ptr<QueryResult> PhysicalBatchCollector::GetResult(GlobalSinkState &state) {
	auto &gstate = (BatchCollectorGlobalState &)state;
	D_ASSERT(gstate.result);
	return move(gstate.result);
}

} // namespace duckdb


namespace duckdb {

PhysicalExecute::PhysicalExecute(PhysicalOperator *plan)
    : PhysicalOperator(PhysicalOperatorType::EXECUTE, plan->types, -1), plan(plan) {
}

vector<PhysicalOperator *> PhysicalExecute::GetChildren() const {
	return {plan};
}

void PhysicalExecute::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
	// EXECUTE statement: build pipeline on child
	plan->BuildPipelines(executor, current, state);
}

} // namespace duckdb




namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class ExplainAnalyzeStateGlobalState : public GlobalSinkState {
public:
	string analyzed_plan;
};

SinkResultType PhysicalExplainAnalyze::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                            DataChunk &input) const {
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalExplainAnalyze::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                  GlobalSinkState &gstate_p) const {
	auto &gstate = (ExplainAnalyzeStateGlobalState &)gstate_p;
	auto &profiler = QueryProfiler::Get(context);
	gstate.analyzed_plan = profiler.ToString();
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSinkState> PhysicalExplainAnalyze::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<ExplainAnalyzeStateGlobalState>();
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class ExplainAnalyzeState : public GlobalSourceState {
public:
	ExplainAnalyzeState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalExplainAnalyze::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<ExplainAnalyzeState>();
}

void PhysicalExplainAnalyze::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &source_state,
                                     LocalSourceState &lstate) const {
	auto &state = (ExplainAnalyzeState &)source_state;
	auto &gstate = (ExplainAnalyzeStateGlobalState &)*sink_state;
	if (state.finished) {
		return;
	}
	chunk.SetValue(0, 0, Value("analyzed_plan"));
	chunk.SetValue(1, 0, Value(gstate.analyzed_plan));
	chunk.SetCardinality(1);

	state.finished = true;
}

} // namespace duckdb









namespace duckdb {

PhysicalLimit::PhysicalLimit(vector<LogicalType> types, idx_t limit, idx_t offset,
                             unique_ptr<Expression> limit_expression, unique_ptr<Expression> offset_expression,
                             idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::LIMIT, move(types), estimated_cardinality), limit_value(limit),
      offset_value(offset), limit_expression(move(limit_expression)), offset_expression(move(offset_expression)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class LimitGlobalState : public GlobalSinkState {
public:
	explicit LimitGlobalState(Allocator &allocator, const PhysicalLimit &op) : data(allocator) {
		limit = 0;
		offset = 0;
	}

	mutex glock;
	idx_t limit;
	idx_t offset;
	BatchedChunkCollection data;
};

class LimitLocalState : public LocalSinkState {
public:
	explicit LimitLocalState(Allocator &allocator, const PhysicalLimit &op) : current_offset(0), data(allocator) {
		this->limit = op.limit_expression ? DConstants::INVALID_INDEX : op.limit_value;
		this->offset = op.offset_expression ? DConstants::INVALID_INDEX : op.offset_value;
	}

	idx_t current_offset;
	idx_t limit;
	idx_t offset;
	BatchedChunkCollection data;
};

unique_ptr<GlobalSinkState> PhysicalLimit::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<LimitGlobalState>(Allocator::Get(context), *this);
}

unique_ptr<LocalSinkState> PhysicalLimit::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<LimitLocalState>(Allocator::Get(context.client), *this);
}

bool PhysicalLimit::ComputeOffset(ExecutionContext &context, DataChunk &input, idx_t &limit, idx_t &offset,
                                  idx_t current_offset, idx_t &max_element, Expression *limit_expression,
                                  Expression *offset_expression) {
	if (limit != DConstants::INVALID_INDEX && offset != DConstants::INVALID_INDEX) {
		max_element = limit + offset;
		if ((limit == 0 || current_offset >= max_element) && !(limit_expression || offset_expression)) {
			return false;
		}
	}

	// get the next chunk from the child
	if (limit == DConstants::INVALID_INDEX) {
		limit = 1ULL << 62ULL;
		Value val = GetDelimiter(context, input, limit_expression);
		if (!val.IsNull()) {
			limit = val.GetValue<idx_t>();
		}
		if (limit > 1ULL << 62ULL) {
			throw BinderException("Max value %lld for LIMIT/OFFSET is %lld", limit, 1ULL << 62ULL);
		}
	}
	if (offset == DConstants::INVALID_INDEX) {
		offset = 0;
		Value val = GetDelimiter(context, input, offset_expression);
		if (!val.IsNull()) {
			offset = val.GetValue<idx_t>();
		}
		if (offset > 1ULL << 62ULL) {
			throw BinderException("Max value %lld for LIMIT/OFFSET is %lld", offset, 1ULL << 62ULL);
		}
	}
	max_element = limit + offset;
	if (limit == 0 || current_offset >= max_element) {
		return false;
	}
	return true;
}

SinkResultType PhysicalLimit::Sink(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate,
                                   DataChunk &input) const {

	D_ASSERT(input.size() > 0);
	auto &state = (LimitLocalState &)lstate;
	auto &limit = state.limit;
	auto &offset = state.offset;

	idx_t max_element;
	if (!ComputeOffset(context, input, limit, offset, state.current_offset, max_element, limit_expression.get(),
	                   offset_expression.get())) {
		return SinkResultType::FINISHED;
	}
	state.data.Append(input, lstate.batch_index);
	state.current_offset += input.size();
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalLimit::Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const {
	auto &gstate = (LimitGlobalState &)gstate_p;
	auto &state = (LimitLocalState &)lstate_p;

	lock_guard<mutex> lock(gstate.glock);
	gstate.limit = state.limit;
	gstate.offset = state.offset;
	gstate.data.Merge(state.data);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class LimitSourceState : public GlobalSourceState {
public:
	LimitSourceState() {
		initialized = false;
		current_offset = 0;
	}

	bool initialized;
	idx_t current_offset;
	BatchedChunkScanState scan_state;
};

unique_ptr<GlobalSourceState> PhysicalLimit::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<LimitSourceState>();
}

void PhysicalLimit::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                            LocalSourceState &lstate) const {
	auto &gstate = (LimitGlobalState &)*sink_state;
	auto &state = (LimitSourceState &)gstate_p;
	while (state.current_offset < gstate.limit + gstate.offset) {
		if (!state.initialized) {
			gstate.data.InitializeScan(state.scan_state);
			state.initialized = true;
		}
		gstate.data.Scan(state.scan_state, chunk);
		if (chunk.size() == 0) {
			break;
		}
		if (HandleOffset(chunk, state.current_offset, gstate.offset, gstate.limit)) {
			break;
		}
	}
}

bool PhysicalLimit::HandleOffset(DataChunk &input, idx_t &current_offset, idx_t offset, idx_t limit) {
	idx_t max_element = limit + offset;
	if (limit == DConstants::INVALID_INDEX) {
		max_element = DConstants::INVALID_INDEX;
	}
	idx_t input_size = input.size();
	if (current_offset < offset) {
		// we are not yet at the offset point
		if (current_offset + input.size() > offset) {
			// however we will reach it in this chunk
			// we have to copy part of the chunk with an offset
			idx_t start_position = offset - current_offset;
			auto chunk_count = MinValue<idx_t>(limit, input.size() - start_position);
			SelectionVector sel(STANDARD_VECTOR_SIZE);
			for (idx_t i = 0; i < chunk_count; i++) {
				sel.set_index(i, start_position + i);
			}
			// set up a slice of the input chunks
			input.Slice(input, sel, chunk_count);
		} else {
			current_offset += input_size;
			return false;
		}
	} else {
		// have to copy either the entire chunk or part of it
		idx_t chunk_count;
		if (current_offset + input.size() >= max_element) {
			// have to limit the count of the chunk
			chunk_count = max_element - current_offset;
		} else {
			// we copy the entire chunk
			chunk_count = input.size();
		}
		// instead of copying we just change the pointer in the current chunk
		input.Reference(input);
		input.SetCardinality(chunk_count);
	}

	current_offset += input_size;
	return true;
}

Value PhysicalLimit::GetDelimiter(ExecutionContext &context, DataChunk &input, Expression *expr) {
	DataChunk limit_chunk;
	vector<LogicalType> types {expr->return_type};
	auto &allocator = Allocator::Get(context.client);
	limit_chunk.Initialize(allocator, types);
	ExpressionExecutor limit_executor(allocator, expr);
	auto input_size = input.size();
	input.SetCardinality(1);
	limit_executor.Execute(input, limit_chunk);
	input.SetCardinality(input_size);
	auto limit_value = limit_chunk.GetValue(0, 0);
	return limit_value;
}

} // namespace duckdb








namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class LimitPercentGlobalState : public GlobalSinkState {
public:
	explicit LimitPercentGlobalState(Allocator &allocator, const PhysicalLimitPercent &op)
	    : current_offset(0), data(allocator) {
		if (!op.limit_expression) {
			this->limit_percent = op.limit_percent;
			is_limit_percent_delimited = true;
		} else {
			this->limit_percent = 100.0;
		}

		if (!op.offset_expression) {
			this->offset = op.offset_value;
			is_offset_delimited = true;
		} else {
			this->offset = 0;
		}
	}

	idx_t current_offset;
	double limit_percent;
	idx_t offset;
	ChunkCollection data;

	bool is_limit_percent_delimited = false;
	bool is_offset_delimited = false;
};

unique_ptr<GlobalSinkState> PhysicalLimitPercent::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<LimitPercentGlobalState>(Allocator::Get(context), *this);
}

SinkResultType PhysicalLimitPercent::Sink(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate,
                                          DataChunk &input) const {
	D_ASSERT(input.size() > 0);
	auto &state = (LimitPercentGlobalState &)gstate;
	auto &limit_percent = state.limit_percent;
	auto &offset = state.offset;

	// get the next chunk from the child
	if (!state.is_limit_percent_delimited) {
		Value val = PhysicalLimit::GetDelimiter(context, input, limit_expression.get());
		if (!val.IsNull()) {
			limit_percent = val.GetValue<double>();
		}
		if (limit_percent < 0.0) {
			throw BinderException("Percentage value(%f) can't be negative", limit_percent);
		}
		state.is_limit_percent_delimited = true;
	}
	if (!state.is_offset_delimited) {
		Value val = PhysicalLimit::GetDelimiter(context, input, offset_expression.get());
		if (!val.IsNull()) {
			offset = val.GetValue<idx_t>();
		}
		if (offset > 1ULL << 62ULL) {
			throw BinderException("Max value %lld for LIMIT/OFFSET is %lld", offset, 1ULL << 62ULL);
		}
		state.is_offset_delimited = true;
	}

	if (!PhysicalLimit::HandleOffset(input, state.current_offset, offset, DConstants::INVALID_INDEX)) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	state.data.Append(input);
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class LimitPercentOperatorState : public GlobalSourceState {
public:
	LimitPercentOperatorState() : chunk_idx(0), limit(DConstants::INVALID_INDEX), current_offset(0) {
	}

	idx_t chunk_idx;
	idx_t limit;
	idx_t current_offset;
};

unique_ptr<GlobalSourceState> PhysicalLimitPercent::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<LimitPercentOperatorState>();
}

void PhysicalLimitPercent::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                   LocalSourceState &lstate) const {
	auto &gstate = (LimitPercentGlobalState &)*sink_state;
	auto &state = (LimitPercentOperatorState &)gstate_p;
	auto &limit_percent = gstate.limit_percent;
	auto &offset = gstate.offset;
	auto &limit = state.limit;
	auto &current_offset = state.current_offset;

	if (gstate.is_limit_percent_delimited && limit == DConstants::INVALID_INDEX) {
		idx_t count = gstate.data.Count();
		if (count > 0) {
			count += offset;
		}
		limit = MinValue((idx_t)(limit_percent / 100 * count), count);
		if (limit == 0) {
			return;
		}
	}

	if (current_offset >= limit || state.chunk_idx >= gstate.data.ChunkCount()) {
		return;
	}

	DataChunk &input = gstate.data.GetChunk(state.chunk_idx);
	state.chunk_idx++;
	if (PhysicalLimit::HandleOffset(input, current_offset, 0, limit)) {
		chunk.Reference(input);
	}
}

} // namespace duckdb



namespace duckdb {

void PhysicalLoad::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                           LocalSourceState &lstate) const {
	auto &db = DatabaseInstance::GetDatabase(context.client);
	if (info->load_type == LoadType::INSTALL || info->load_type == LoadType::FORCE_INSTALL) {
		ExtensionHelper::InstallExtension(db, info->filename, info->load_type == LoadType::FORCE_INSTALL);
	} else {
		ExtensionHelper::LoadExternalExtension(db, info->filename);
	}
}

} // namespace duckdb





namespace duckdb {

PhysicalMaterializedCollector::PhysicalMaterializedCollector(PreparedStatementData &data, bool parallel)
    : PhysicalResultCollector(data), parallel(parallel) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class MaterializedCollectorGlobalState : public GlobalSinkState {
public:
	mutex glock;
	unique_ptr<MaterializedQueryResult> result;
};

SinkResultType PhysicalMaterializedCollector::Sink(ExecutionContext &context, GlobalSinkState &gstate_p,
                                                   LocalSinkState &lstate, DataChunk &input) const {
	auto &gstate = (MaterializedCollectorGlobalState &)gstate_p;
	lock_guard<mutex> lock(gstate.glock);
	gstate.result->collection.Append(input);
	return SinkResultType::NEED_MORE_INPUT;
}

unique_ptr<GlobalSinkState> PhysicalMaterializedCollector::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_unique<MaterializedCollectorGlobalState>();
	state->result =
	    make_unique<MaterializedQueryResult>(statement_type, properties, types, names, context.shared_from_this());
	return move(state);
}

unique_ptr<QueryResult> PhysicalMaterializedCollector::GetResult(GlobalSinkState &state) {
	auto &gstate = (MaterializedCollectorGlobalState &)state;
	D_ASSERT(gstate.result);
	return move(gstate.result);
}

bool PhysicalMaterializedCollector::ParallelSink() const {
	return parallel;
}

} // namespace duckdb


namespace duckdb {

void PhysicalPragma::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                             LocalSourceState &lstate) const {
	auto &client = context.client;
	FunctionParameters parameters {info.parameters, info.named_parameters};
	function.function(client, parameters);
}

} // namespace duckdb



namespace duckdb {

void PhysicalPrepare::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                              LocalSourceState &lstate) const {
	auto &client = context.client;

	// store the prepared statement in the context
	ClientData::Get(client).prepared_statements[name] = prepared;
}

} // namespace duckdb



namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class SampleGlobalSinkState : public GlobalSinkState {
public:
	explicit SampleGlobalSinkState(Allocator &allocator, SampleOptions &options) {
		if (options.is_percentage) {
			auto percentage = options.sample_size.GetValue<double>();
			if (percentage == 0) {
				return;
			}
			sample = make_unique<ReservoirSamplePercentage>(allocator, percentage, options.seed);
		} else {
			auto size = options.sample_size.GetValue<int64_t>();
			if (size == 0) {
				return;
			}
			sample = make_unique<ReservoirSample>(allocator, size, options.seed);
		}
	}

	//! The lock for updating the global aggregate state
	mutex lock;
	//! The reservoir sample
	unique_ptr<BlockingSample> sample;
};

unique_ptr<GlobalSinkState> PhysicalReservoirSample::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<SampleGlobalSinkState>(Allocator::Get(context), *options);
}

SinkResultType PhysicalReservoirSample::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                             DataChunk &input) const {
	auto &gstate = (SampleGlobalSinkState &)state;
	if (!gstate.sample) {
		return SinkResultType::FINISHED;
	}
	// we implement reservoir sampling without replacement and exponential jumps here
	// the algorithm is adopted from the paper Weighted random sampling with a reservoir by Pavlos S. Efraimidis et al.
	// note that the original algorithm is about weighted sampling; this is a simplified approach for uniform sampling
	lock_guard<mutex> glock(gstate.lock);
	gstate.sample->AddToReservoir(input);
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
void PhysicalReservoirSample::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                      LocalSourceState &lstate) const {
	auto &sink = (SampleGlobalSinkState &)*this->sink_state;
	if (!sink.sample) {
		return;
	}
	auto sample_chunk = sink.sample->GetChunk();
	if (!sample_chunk) {
		return;
	}
	chunk.Move(*sample_chunk);
}

string PhysicalReservoirSample::ParamsToString() const {
	return options->sample_size.ToString() + (options->is_percentage ? "%" : " rows");
}

} // namespace duckdb







namespace duckdb {

PhysicalResultCollector::PhysicalResultCollector(PreparedStatementData &data)
    : PhysicalOperator(PhysicalOperatorType::RESULT_COLLECTOR, {LogicalType::BOOLEAN}, 0),
      statement_type(data.statement_type), properties(data.properties), plan(data.plan.get()), names(data.names) {
	this->types = data.types;
}

unique_ptr<PhysicalResultCollector> PhysicalResultCollector::GetResultCollector(ClientContext &context,
                                                                                PreparedStatementData &data) {
	auto &config = DBConfig::GetConfig(context);
	bool use_materialized_collector = !config.preserve_insertion_order || !data.plan->AllSourcesSupportBatchIndex();
	if (use_materialized_collector) {
		// parallel materialized collector only if we don't care about maintaining insertion order
		return make_unique_base<PhysicalResultCollector, PhysicalMaterializedCollector>(
		    data, !config.preserve_insertion_order);
	} else {
		// we care about maintaining insertion order and the sources all support batch indexes
		// use a batch collector
		return make_unique_base<PhysicalResultCollector, PhysicalBatchCollector>(data);
	}
}

vector<PhysicalOperator *> PhysicalResultCollector::GetChildren() const {
	return {plan};
}

void PhysicalResultCollector::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
	// operator is a sink, build a pipeline
	sink_state.reset();

	// single operator:
	// the operator becomes the data source of the current pipeline
	state.SetPipelineSource(current, this);
	// we create a new pipeline starting from the child
	D_ASSERT(children.size() == 0);
	D_ASSERT(plan);

	BuildChildPipeline(executor, current, state, plan);
}

} // namespace duckdb






namespace duckdb {

void PhysicalSet::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                          LocalSourceState &lstate) const {
	auto option = DBConfig::GetOptionByName(name);
	if (!option) {
		// check if this is an extra extension variable
		auto &config = DBConfig::GetConfig(context.client);
		auto entry = config.extension_parameters.find(name);
		if (entry == config.extension_parameters.end()) {
			// it is not!
			// get a list of all options
			vector<string> potential_names;
			for (idx_t i = 0, option_count = DBConfig::GetOptionCount(); i < option_count; i++) {
				potential_names.emplace_back(DBConfig::GetOptionByIndex(i)->name);
			}
			for (auto &entry : config.extension_parameters) {
				potential_names.push_back(entry.first);
			}

			throw CatalogException("unrecognized configuration parameter \"%s\"\n%s", name,
			                       StringUtil::CandidatesErrorMessage(potential_names, name, "Did you mean"));
		}
		//! it is!
		auto &extension_option = entry->second;
		auto &target_type = extension_option.type;
		Value target_value = value.CastAs(target_type);
		if (extension_option.set_function) {
			extension_option.set_function(context.client, scope, target_value);
		}
		if (scope == SetScope::GLOBAL) {
			config.set_variables[name] = move(target_value);
		} else {
			auto &client_config = ClientConfig::GetConfig(context.client);
			client_config.set_variables[name] = move(target_value);
		}
		return;
	}
	SetScope variable_scope = scope;
	if (variable_scope == SetScope::AUTOMATIC) {
		if (option->set_local) {
			variable_scope = SetScope::SESSION;
		} else {
			D_ASSERT(option->set_global);
			variable_scope = SetScope::GLOBAL;
		}
	}

	Value input = value.CastAs(option->parameter_type);
	switch (variable_scope) {
	case SetScope::GLOBAL: {
		if (!option->set_global) {
			throw CatalogException("option \"%s\" cannot be set globally", name);
		}
		auto &db = DatabaseInstance::GetDatabase(context.client);
		auto &config = DBConfig::GetConfig(context.client);
		option->set_global(&db, config, input);
		break;
	}
	case SetScope::SESSION:
		if (!option->set_local) {
			throw CatalogException("option \"%s\" cannot be set locally", name);
		}
		option->set_local(context.client, input);
		break;
	default:
		throw InternalException("Unsupported SetScope for variable");
	}
}

} // namespace duckdb



namespace duckdb {

PhysicalStreamingLimit::PhysicalStreamingLimit(vector<LogicalType> types, idx_t limit, idx_t offset,
                                               unique_ptr<Expression> limit_expression,
                                               unique_ptr<Expression> offset_expression, idx_t estimated_cardinality,
                                               bool parallel)
    : PhysicalOperator(PhysicalOperatorType::STREAMING_LIMIT, move(types), estimated_cardinality), limit_value(limit),
      offset_value(offset), limit_expression(move(limit_expression)), offset_expression(move(offset_expression)),
      parallel(parallel) {
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class StreamingLimitOperatorState : public OperatorState {
public:
	explicit StreamingLimitOperatorState(const PhysicalStreamingLimit &op) {
		this->limit = op.limit_expression ? DConstants::INVALID_INDEX : op.limit_value;
		this->offset = op.offset_expression ? DConstants::INVALID_INDEX : op.offset_value;
	}

	idx_t limit;
	idx_t offset;
};

class StreamingLimitGlobalState : public GlobalOperatorState {
public:
	StreamingLimitGlobalState() : current_offset(0) {
	}

	std::atomic<idx_t> current_offset;
};

unique_ptr<OperatorState> PhysicalStreamingLimit::GetOperatorState(ExecutionContext &context) const {
	return make_unique<StreamingLimitOperatorState>(*this);
}

unique_ptr<GlobalOperatorState> PhysicalStreamingLimit::GetGlobalOperatorState(ClientContext &context) const {
	return make_unique<StreamingLimitGlobalState>();
}

OperatorResultType PhysicalStreamingLimit::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                   GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &gstate = (StreamingLimitGlobalState &)gstate_p;
	auto &state = (StreamingLimitOperatorState &)state_p;
	auto &limit = state.limit;
	auto &offset = state.offset;
	idx_t current_offset = gstate.current_offset.fetch_add(input.size());
	idx_t max_element;
	if (!PhysicalLimit::ComputeOffset(context, input, limit, offset, current_offset, max_element,
	                                  limit_expression.get(), offset_expression.get())) {
		return OperatorResultType::FINISHED;
	}
	if (PhysicalLimit::HandleOffset(input, current_offset, offset, limit)) {
		chunk.Reference(input);
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

bool PhysicalStreamingLimit::IsOrderDependent() const {
	return !parallel;
}

bool PhysicalStreamingLimit::ParallelOperator() const {
	return parallel;
}

} // namespace duckdb




namespace duckdb {

PhysicalStreamingSample::PhysicalStreamingSample(vector<LogicalType> types, SampleMethod method, double percentage,
                                                 int64_t seed, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::STREAMING_SAMPLE, move(types), estimated_cardinality), method(method),
      percentage(percentage / 100), seed(seed) {
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class StreamingSampleOperatorState : public OperatorState {
public:
	explicit StreamingSampleOperatorState(int64_t seed) : random(seed) {
	}

	RandomEngine random;
};

void PhysicalStreamingSample::SystemSample(DataChunk &input, DataChunk &result, OperatorState &state_p) const {
	// system sampling: we throw one dice per chunk
	auto &state = (StreamingSampleOperatorState &)state_p;
	double rand = state.random.NextRandom();
	if (rand <= percentage) {
		// rand is smaller than sample_size: output chunk
		result.Reference(input);
	}
}

void PhysicalStreamingSample::BernoulliSample(DataChunk &input, DataChunk &result, OperatorState &state_p) const {
	// bernoulli sampling: we throw one dice per tuple
	// then slice the result chunk
	auto &state = (StreamingSampleOperatorState &)state_p;
	idx_t result_count = 0;
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < input.size(); i++) {
		double rand = state.random.NextRandom();
		if (rand <= percentage) {
			sel.set_index(result_count++, i);
		}
	}
	if (result_count > 0) {
		result.Slice(input, sel, result_count);
	}
}

unique_ptr<OperatorState> PhysicalStreamingSample::GetOperatorState(ExecutionContext &context) const {
	return make_unique<StreamingSampleOperatorState>(seed);
}

OperatorResultType PhysicalStreamingSample::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                    GlobalOperatorState &gstate, OperatorState &state) const {
	switch (method) {
	case SampleMethod::BERNOULLI_SAMPLE:
		BernoulliSample(input, chunk, state);
		break;
	case SampleMethod::SYSTEM_SAMPLE:
		SystemSample(input, chunk, state);
		break;
	default:
		throw InternalException("Unsupported sample method for streaming sample");
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalStreamingSample::ParamsToString() const {
	return SampleMethodToString(method) + ": " + to_string(100 * percentage) + "%";
}

} // namespace duckdb



namespace duckdb {

void PhysicalTransaction::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                  LocalSourceState &lstate) const {
	auto &client = context.client;

	switch (info->type) {
	case TransactionType::BEGIN_TRANSACTION: {
		if (client.transaction.IsAutoCommit()) {
			// start the active transaction
			// if autocommit is active, we have already called
			// BeginTransaction by setting autocommit to false we
			// prevent it from being closed after this query, hence
			// preserving the transaction context for the next query
			client.transaction.SetAutoCommit(false);
		} else {
			throw TransactionException("cannot start a transaction within a transaction");
		}
		break;
	}
	case TransactionType::COMMIT: {
		if (client.transaction.IsAutoCommit()) {
			throw TransactionException("cannot commit - no transaction is active");
		} else {
			// explicitly commit the current transaction
			client.transaction.Commit();
		}
		break;
	}
	case TransactionType::ROLLBACK: {
		if (client.transaction.IsAutoCommit()) {
			throw TransactionException("cannot rollback - no transaction is active");
		} else {
			// explicitly rollback the current transaction
			client.transaction.Rollback();
		}
		break;
	}
	default:
		throw NotImplementedException("Unrecognized transaction type!");
	}
}

} // namespace duckdb


namespace duckdb {

void PhysicalVacuum::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                             LocalSourceState &lstate) const {
	// NOP
}

} // namespace duckdb





namespace duckdb {

PerfectHashJoinExecutor::PerfectHashJoinExecutor(const PhysicalHashJoin &join_p, JoinHashTable &ht_p,
                                                 PerfectHashJoinStats perfect_join_stats)
    : join(join_p), ht(ht_p), perfect_join_statistics(move(perfect_join_stats)) {
}

bool PerfectHashJoinExecutor::CanDoPerfectHashJoin() {
	return perfect_join_statistics.is_build_small;
}

//===--------------------------------------------------------------------===//
// Build
//===--------------------------------------------------------------------===//
bool PerfectHashJoinExecutor::BuildPerfectHashTable(LogicalType &key_type) {
	// First, allocate memory for each build column
	auto build_size = perfect_join_statistics.build_range + 1;
	for (const auto &type : ht.build_types) {
		perfect_hash_table.emplace_back(type, build_size);
	}
	// and for duplicate_checking
	bitmap_build_idx = unique_ptr<bool[]>(new bool[build_size]);
	memset(bitmap_build_idx.get(), 0, sizeof(bool) * build_size); // set false

	// Now fill columns with build data
	JoinHTScanState join_ht_state;
	return FullScanHashTable(join_ht_state, key_type);
}

bool PerfectHashJoinExecutor::FullScanHashTable(JoinHTScanState &state, LogicalType &key_type) {
	Vector tuples_addresses(LogicalType::POINTER, ht.Count());              // allocate space for all the tuples
	auto key_locations = FlatVector::GetData<data_ptr_t>(tuples_addresses); // get a pointer to vector data
	// TODO: In a parallel finalize: One should exclusively lock and each thread should do one part of the code below.
	// Go through all the blocks and fill the keys addresses
	auto keys_count = ht.FillWithHTOffsets(key_locations, state);
	// Scan the build keys in the hash table
	Vector build_vector(key_type, keys_count);
	RowOperations::FullScanColumn(ht.layout, tuples_addresses, build_vector, keys_count, 0);
	// Now fill the selection vector using the build keys and create a sequential vector
	// todo: add check for fast pass when probe is part of build domain
	SelectionVector sel_build(keys_count + 1);
	SelectionVector sel_tuples(keys_count + 1);
	bool success = FillSelectionVectorSwitchBuild(build_vector, sel_build, sel_tuples, keys_count);
	// early out
	if (!success) {
		return false;
	}
	if (unique_keys == perfect_join_statistics.build_range + 1 && !ht.has_null) {
		perfect_join_statistics.is_build_dense = true;
	}
	keys_count = unique_keys; // do not consider keys out of the range
	// Full scan the remaining build columns and fill the perfect hash table
	for (idx_t i = 0; i < ht.build_types.size(); i++) {
		auto build_size = perfect_join_statistics.build_range + 1;
		auto &vector = perfect_hash_table[i];
		D_ASSERT(vector.GetType() == ht.build_types[i]);
		const auto col_no = ht.condition_types.size() + i;
		const auto col_offset = ht.layout.GetOffsets()[col_no];
		RowOperations::Gather(tuples_addresses, sel_tuples, vector, sel_build, keys_count, col_offset, col_no,
		                      build_size);
	}
	return true;
}

bool PerfectHashJoinExecutor::FillSelectionVectorSwitchBuild(Vector &source, SelectionVector &sel_vec,
                                                             SelectionVector &seq_sel_vec, idx_t count) {
	switch (source.GetType().InternalType()) {
	case PhysicalType::INT8:
		return TemplatedFillSelectionVectorBuild<int8_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::INT16:
		return TemplatedFillSelectionVectorBuild<int16_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::INT32:
		return TemplatedFillSelectionVectorBuild<int32_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::INT64:
		return TemplatedFillSelectionVectorBuild<int64_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::UINT8:
		return TemplatedFillSelectionVectorBuild<uint8_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::UINT16:
		return TemplatedFillSelectionVectorBuild<uint16_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::UINT32:
		return TemplatedFillSelectionVectorBuild<uint32_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::UINT64:
		return TemplatedFillSelectionVectorBuild<uint64_t>(source, sel_vec, seq_sel_vec, count);
	default:
		throw NotImplementedException("Type not supported for perfect hash join");
	}
}

template <typename T>
bool PerfectHashJoinExecutor::TemplatedFillSelectionVectorBuild(Vector &source, SelectionVector &sel_vec,
                                                                SelectionVector &seq_sel_vec, idx_t count) {
	if (perfect_join_statistics.build_min.IsNull() || perfect_join_statistics.build_max.IsNull()) {
		return false;
	}
	auto min_value = perfect_join_statistics.build_min.GetValueUnsafe<T>();
	auto max_value = perfect_join_statistics.build_max.GetValueUnsafe<T>();
	VectorData vector_data;
	source.Orrify(count, vector_data);
	auto data = reinterpret_cast<T *>(vector_data.data);
	// generate the selection vector
	for (idx_t i = 0, sel_idx = 0; i < count; ++i) {
		auto data_idx = vector_data.sel->get_index(i);
		auto input_value = data[data_idx];
		// add index to selection vector if value in the range
		if (min_value <= input_value && input_value <= max_value) {
			auto idx = (idx_t)(input_value - min_value); // subtract min value to get the idx position
			sel_vec.set_index(sel_idx, idx);
			if (bitmap_build_idx[idx]) {
				return false;
			} else {
				bitmap_build_idx[idx] = true;
				unique_keys++;
			}
			seq_sel_vec.set_index(sel_idx++, i);
		}
	}
	return true;
}

//===--------------------------------------------------------------------===//
// Probe
//===--------------------------------------------------------------------===//
class PerfectHashJoinState : public OperatorState {
public:
	PerfectHashJoinState(Allocator &allocator, const PhysicalHashJoin &join) : probe_executor(allocator) {
		join_keys.Initialize(allocator, join.condition_types);
		for (auto &cond : join.conditions) {
			probe_executor.AddExpression(*cond.left);
		}
		build_sel_vec.Initialize(STANDARD_VECTOR_SIZE);
		probe_sel_vec.Initialize(STANDARD_VECTOR_SIZE);
		seq_sel_vec.Initialize(STANDARD_VECTOR_SIZE);
	}

	DataChunk join_keys;
	ExpressionExecutor probe_executor;
	SelectionVector build_sel_vec;
	SelectionVector probe_sel_vec;
	SelectionVector seq_sel_vec;
};

unique_ptr<OperatorState> PerfectHashJoinExecutor::GetOperatorState(ExecutionContext &context) {
	auto state = make_unique<PerfectHashJoinState>(Allocator::Get(context.client), join);
	return move(state);
}

OperatorResultType PerfectHashJoinExecutor::ProbePerfectHashTable(ExecutionContext &context, DataChunk &input,
                                                                  DataChunk &result, OperatorState &state_p) {
	auto &state = (PerfectHashJoinState &)state_p;
	// keeps track of how many probe keys have a match
	idx_t probe_sel_count = 0;

	// fetch the join keys from the chunk
	state.join_keys.Reset();
	state.probe_executor.Execute(input, state.join_keys);
	// select the keys that are in the min-max range
	auto &keys_vec = state.join_keys.data[0];
	auto keys_count = state.join_keys.size();
	// todo: add check for fast pass when probe is part of build domain
	FillSelectionVectorSwitchProbe(keys_vec, state.build_sel_vec, state.probe_sel_vec, keys_count, probe_sel_count);

	// If build is dense and probe is in build's domain, just reference probe
	if (perfect_join_statistics.is_build_dense && keys_count == probe_sel_count) {
		result.Reference(input);
	} else {
		// otherwise, filter it out the values that do not match
		result.Slice(input, state.probe_sel_vec, probe_sel_count, 0);
	}
	// on the build side, we need to fetch the data and build dictionary vectors with the sel_vec
	for (idx_t i = 0; i < ht.build_types.size(); i++) {
		auto &result_vector = result.data[input.ColumnCount() + i];
		D_ASSERT(result_vector.GetType() == ht.build_types[i]);
		auto &build_vec = perfect_hash_table[i];
		result_vector.Reference(build_vec);
		result_vector.Slice(state.build_sel_vec, probe_sel_count);
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

void PerfectHashJoinExecutor::FillSelectionVectorSwitchProbe(Vector &source, SelectionVector &build_sel_vec,
                                                             SelectionVector &probe_sel_vec, idx_t count,
                                                             idx_t &probe_sel_count) {
	switch (source.GetType().InternalType()) {
	case PhysicalType::INT8:
		TemplatedFillSelectionVectorProbe<int8_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::INT16:
		TemplatedFillSelectionVectorProbe<int16_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::INT32:
		TemplatedFillSelectionVectorProbe<int32_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::INT64:
		TemplatedFillSelectionVectorProbe<int64_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::UINT8:
		TemplatedFillSelectionVectorProbe<uint8_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::UINT16:
		TemplatedFillSelectionVectorProbe<uint16_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::UINT32:
		TemplatedFillSelectionVectorProbe<uint32_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::UINT64:
		TemplatedFillSelectionVectorProbe<uint64_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	default:
		throw NotImplementedException("Type not supported");
	}
}

template <typename T>
void PerfectHashJoinExecutor::TemplatedFillSelectionVectorProbe(Vector &source, SelectionVector &build_sel_vec,
                                                                SelectionVector &probe_sel_vec, idx_t count,
                                                                idx_t &probe_sel_count) {
	auto min_value = perfect_join_statistics.build_min.GetValueUnsafe<T>();
	auto max_value = perfect_join_statistics.build_max.GetValueUnsafe<T>();

	VectorData vector_data;
	source.Orrify(count, vector_data);
	auto data = reinterpret_cast<T *>(vector_data.data);
	auto validity_mask = &vector_data.validity;
	// build selection vector for non-dense build
	if (validity_mask->AllValid()) {
		for (idx_t i = 0, sel_idx = 0; i < count; ++i) {
			// retrieve value from vector
			auto data_idx = vector_data.sel->get_index(i);
			auto input_value = data[data_idx];
			// add index to selection vector if value in the range
			if (min_value <= input_value && input_value <= max_value) {
				auto idx = (idx_t)(input_value - min_value); // subtract min value to get the idx position
				                                             // check for matches in the build
				if (bitmap_build_idx[idx]) {
					build_sel_vec.set_index(sel_idx, idx);
					probe_sel_vec.set_index(sel_idx++, i);
					probe_sel_count++;
				}
			}
		}
	} else {
		for (idx_t i = 0, sel_idx = 0; i < count; ++i) {
			// retrieve value from vector
			auto data_idx = vector_data.sel->get_index(i);
			if (!validity_mask->RowIsValid(data_idx)) {
				continue;
			}
			auto input_value = data[data_idx];
			// add index to selection vector if value in the range
			if (min_value <= input_value && input_value <= max_value) {
				auto idx = (idx_t)(input_value - min_value); // subtract min value to get the idx position
				                                             // check for matches in the build
				if (bitmap_build_idx[idx]) {
					build_sel_vec.set_index(sel_idx, idx);
					probe_sel_vec.set_index(sel_idx++, i);
					probe_sel_count++;
				}
			}
		}
	}
}

} // namespace duckdb






namespace duckdb {

PhysicalBlockwiseNLJoin::PhysicalBlockwiseNLJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                                 unique_ptr<PhysicalOperator> right, unique_ptr<Expression> condition,
                                                 JoinType join_type, idx_t estimated_cardinality)
    : PhysicalJoin(op, PhysicalOperatorType::BLOCKWISE_NL_JOIN, join_type, estimated_cardinality),
      condition(move(condition)) {
	children.push_back(move(left));
	children.push_back(move(right));
	// MARK and SINGLE joins not handled
	D_ASSERT(join_type != JoinType::MARK);
	D_ASSERT(join_type != JoinType::SINGLE);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class BlockwiseNLJoinLocalState : public LocalSinkState {
public:
	BlockwiseNLJoinLocalState() {
	}
};

class BlockwiseNLJoinGlobalState : public GlobalSinkState {
public:
	explicit BlockwiseNLJoinGlobalState(Allocator &allocator) : right_chunks(allocator) {
	}

	mutex lock;
	ChunkCollection right_chunks;
	//! Whether or not a tuple on the RHS has found a match, only used for FULL OUTER joins
	unique_ptr<bool[]> rhs_found_match;
};

unique_ptr<GlobalSinkState> PhysicalBlockwiseNLJoin::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<BlockwiseNLJoinGlobalState>(Allocator::Get(context));
}

unique_ptr<LocalSinkState> PhysicalBlockwiseNLJoin::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<BlockwiseNLJoinLocalState>();
}

SinkResultType PhysicalBlockwiseNLJoin::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                             DataChunk &input) const {
	auto &gstate = (BlockwiseNLJoinGlobalState &)state;
	lock_guard<mutex> nl_lock(gstate.lock);
	gstate.right_chunks.Append(input);
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PhysicalBlockwiseNLJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                   GlobalSinkState &gstate_p) const {
	auto &gstate = (BlockwiseNLJoinGlobalState &)gstate_p;
	if (IsRightOuterJoin(join_type)) {
		gstate.rhs_found_match = unique_ptr<bool[]>(new bool[gstate.right_chunks.Count()]);
		memset(gstate.rhs_found_match.get(), 0, sizeof(bool) * gstate.right_chunks.Count());
	}
	if (gstate.right_chunks.Count() == 0 && EmptyResultIfRHSIsEmpty()) {
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class BlockwiseNLJoinState : public OperatorState {
public:
	explicit BlockwiseNLJoinState(ExecutionContext &context, const PhysicalBlockwiseNLJoin &op)
	    : left_position(0), right_position(0), executor(Allocator::Get(context.client), *op.condition) {
		if (IsLeftOuterJoin(op.join_type)) {
			left_found_match = unique_ptr<bool[]>(new bool[STANDARD_VECTOR_SIZE]);
			memset(left_found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
		}
	}

	//! Whether or not a tuple on the LHS has found a match, only used for LEFT OUTER and FULL OUTER joins
	unique_ptr<bool[]> left_found_match;
	idx_t left_position;
	idx_t right_position;
	ExpressionExecutor executor;
};

unique_ptr<OperatorState> PhysicalBlockwiseNLJoin::GetOperatorState(ExecutionContext &context) const {
	return make_unique<BlockwiseNLJoinState>(context, *this);
}

OperatorResultType PhysicalBlockwiseNLJoin::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                    GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	D_ASSERT(input.size() > 0);
	auto &state = (BlockwiseNLJoinState &)state_p;
	auto &gstate = (BlockwiseNLJoinGlobalState &)*sink_state;

	if (gstate.right_chunks.Count() == 0) {
		// empty RHS
		if (!EmptyResultIfRHSIsEmpty()) {
			PhysicalComparisonJoin::ConstructEmptyJoinResult(join_type, false, input, chunk);
			return OperatorResultType::NEED_MORE_INPUT;
		} else {
			return OperatorResultType::FINISHED;
		}
	}

	// now perform the actual join
	// we construct a combined DataChunk by referencing the LHS and the RHS
	// every step that we do not have output results we shift the vectors of the RHS one up or down
	// this creates a new "alignment" between the tuples, exhausting all possible O(n^2) combinations
	// while allowing us to use vectorized execution for every step
	idx_t result_count = 0;
	do {
		if (state.left_position >= input.size()) {
			// exhausted LHS, have to pull new LHS chunk
			if (state.left_found_match) {
				// left join: before we move to the next chunk, see if we need to output any vectors that didn't
				// have a match found
				PhysicalJoin::ConstructLeftJoinResult(input, chunk, state.left_found_match.get());
				memset(state.left_found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
			}
			state.left_position = 0;
			state.right_position = 0;
			return OperatorResultType::NEED_MORE_INPUT;
		}
		auto &lchunk = input;
		auto &rchunk = gstate.right_chunks.GetChunk(state.right_position);

		// fill in the current element of the LHS into the chunk
		D_ASSERT(chunk.ColumnCount() == lchunk.ColumnCount() + rchunk.ColumnCount());
		for (idx_t i = 0; i < lchunk.ColumnCount(); i++) {
			ConstantVector::Reference(chunk.data[i], lchunk.data[i], state.left_position, lchunk.size());
		}
		// for the RHS we just reference the entire vector
		for (idx_t i = 0; i < rchunk.ColumnCount(); i++) {
			chunk.data[lchunk.ColumnCount() + i].Reference(rchunk.data[i]);
		}
		chunk.SetCardinality(rchunk.size());

		// now perform the computation
		SelectionVector match_sel(STANDARD_VECTOR_SIZE);
		result_count = state.executor.SelectExpression(chunk, match_sel);
		if (result_count > 0) {
			// found a match!
			// set the match flags in the LHS
			if (state.left_found_match) {
				state.left_found_match[state.left_position] = true;
			}
			// set the match flags in the RHS
			if (gstate.rhs_found_match) {
				for (idx_t i = 0; i < result_count; i++) {
					auto idx = match_sel.get_index(i);
					gstate.rhs_found_match[state.right_position * STANDARD_VECTOR_SIZE + idx] = true;
				}
			}
			chunk.Slice(match_sel, result_count);
		} else {
			// no result: reset the chunk
			chunk.Reset();
		}
		// move to the next tuple on the LHS
		state.left_position++;
		if (state.left_position >= input.size()) {
			// exhausted the current chunk, move to the next RHS chunk
			state.right_position++;
			if (state.right_position < gstate.right_chunks.ChunkCount()) {
				// we still have chunks left! start over on the LHS
				state.left_position = 0;
			}
		}
	} while (result_count == 0);
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

string PhysicalBlockwiseNLJoin::ParamsToString() const {
	string extra_info = JoinTypeToString(join_type) + "\n";
	extra_info += condition->GetName();
	return extra_info;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class BlockwiseNLJoinScanState : public GlobalSourceState {
public:
	explicit BlockwiseNLJoinScanState(const PhysicalBlockwiseNLJoin &op) : op(op), right_outer_position(0) {
	}

	mutex lock;
	const PhysicalBlockwiseNLJoin &op;
	//! The position in the RHS in the final scan of the FULL OUTER JOIN
	idx_t right_outer_position;

public:
	idx_t MaxThreads() override {
		auto &sink = (BlockwiseNLJoinGlobalState &)*op.sink_state;
		return sink.right_chunks.Count() / (STANDARD_VECTOR_SIZE * 10);
	}
};

unique_ptr<GlobalSourceState> PhysicalBlockwiseNLJoin::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<BlockwiseNLJoinScanState>(*this);
}

void PhysicalBlockwiseNLJoin::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                      LocalSourceState &lstate) const {
	D_ASSERT(IsRightOuterJoin(join_type));
	// check if we need to scan any unmatched tuples from the RHS for the full/right outer join
	auto &sink = (BlockwiseNLJoinGlobalState &)*sink_state;
	auto &state = (BlockwiseNLJoinScanState &)gstate;

	// if the LHS is exhausted in a FULL/RIGHT OUTER JOIN, we scan the found_match for any chunks we
	// still need to output
	lock_guard<mutex> l(state.lock);
	PhysicalComparisonJoin::ConstructFullOuterJoinResult(sink.rhs_found_match.get(), sink.right_chunks, chunk,
	                                                     state.right_outer_position);
}

} // namespace duckdb



namespace duckdb {

PhysicalComparisonJoin::PhysicalComparisonJoin(LogicalOperator &op, PhysicalOperatorType type,
                                               vector<JoinCondition> conditions_p, JoinType join_type,
                                               idx_t estimated_cardinality)
    : PhysicalJoin(op, type, join_type, estimated_cardinality) {
	conditions.resize(conditions_p.size());
	// we reorder conditions so the ones with COMPARE_EQUAL occur first
	idx_t equal_position = 0;
	idx_t other_position = conditions_p.size() - 1;
	for (idx_t i = 0; i < conditions_p.size(); i++) {
		if (conditions_p[i].comparison == ExpressionType::COMPARE_EQUAL ||
		    conditions_p[i].comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			// COMPARE_EQUAL and COMPARE_NOT_DISTINCT_FROM, move to the start
			conditions[equal_position++] = std::move(conditions_p[i]);
		} else {
			// other expression, move to the end
			conditions[other_position--] = std::move(conditions_p[i]);
		}
	}
}

string PhysicalComparisonJoin::ParamsToString() const {
	string extra_info = JoinTypeToString(join_type) + "\n";
	for (auto &it : conditions) {
		string op = ExpressionTypeToOperator(it.comparison);
		extra_info += it.left->GetName() + " " + op + " " + it.right->GetName() + "\n";
	}
	return extra_info;
}

void PhysicalComparisonJoin::ConstructEmptyJoinResult(JoinType join_type, bool has_null, DataChunk &input,
                                                      DataChunk &result) {
	// empty hash table, special case
	if (join_type == JoinType::ANTI) {
		// anti join with empty hash table, NOP join
		// return the input
		D_ASSERT(input.ColumnCount() == result.ColumnCount());
		result.Reference(input);
	} else if (join_type == JoinType::MARK) {
		// MARK join with empty hash table
		D_ASSERT(join_type == JoinType::MARK);
		D_ASSERT(result.ColumnCount() == input.ColumnCount() + 1);
		auto &result_vector = result.data.back();
		D_ASSERT(result_vector.GetType() == LogicalType::BOOLEAN);
		// for every data vector, we just reference the child chunk
		result.SetCardinality(input);
		for (idx_t i = 0; i < input.ColumnCount(); i++) {
			result.data[i].Reference(input.data[i]);
		}
		// for the MARK vector:
		// if the HT has no NULL values (i.e. empty result set), return a vector that has false for every input
		// entry if the HT has NULL values (i.e. result set had values, but all were NULL), return a vector that
		// has NULL for every input entry
		if (!has_null) {
			auto bool_result = FlatVector::GetData<bool>(result_vector);
			for (idx_t i = 0; i < result.size(); i++) {
				bool_result[i] = false;
			}
		} else {
			FlatVector::Validity(result_vector).SetAllInvalid(result.size());
		}
	} else if (join_type == JoinType::LEFT || join_type == JoinType::OUTER || join_type == JoinType::SINGLE) {
		// LEFT/FULL OUTER/SINGLE join and build side is empty
		// for the LHS we reference the data
		result.SetCardinality(input.size());
		for (idx_t i = 0; i < input.ColumnCount(); i++) {
			result.data[i].Reference(input.data[i]);
		}
		// for the RHS
		for (idx_t k = input.ColumnCount(); k < result.ColumnCount(); k++) {
			result.data[k].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result.data[k], true);
		}
	}
}

void PhysicalComparisonJoin::ConstructFullOuterJoinResult(bool *found_match, ChunkCollection &input, DataChunk &result,
                                                          idx_t &scan_position) {
	// fill in NULL values for the LHS
	SelectionVector rsel(STANDARD_VECTOR_SIZE);
	while (scan_position < input.Count()) {
		auto &rhs_chunk = input.GetChunk(scan_position / STANDARD_VECTOR_SIZE);
		idx_t result_count = 0;
		// figure out which tuples didn't find a match in the RHS
		for (idx_t i = 0; i < rhs_chunk.size(); i++) {
			if (!found_match[scan_position + i]) {
				rsel.set_index(result_count++, i);
			}
		}
		scan_position += STANDARD_VECTOR_SIZE;
		if (result_count > 0) {
			// if there were any tuples that didn't find a match, output them
			idx_t left_column_count = result.ColumnCount() - input.ColumnCount();
			for (idx_t i = 0; i < left_column_count; i++) {
				result.data[i].SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(result.data[i], true);
			}
			for (idx_t col_idx = 0; col_idx < rhs_chunk.ColumnCount(); col_idx++) {
				result.data[left_column_count + col_idx].Slice(rhs_chunk.data[col_idx], rsel, result_count);
			}
			result.SetCardinality(result_count);
			return;
		}
	}
}

} // namespace duckdb





namespace duckdb {

PhysicalCrossProduct::PhysicalCrossProduct(vector<LogicalType> types, unique_ptr<PhysicalOperator> left,
                                           unique_ptr<PhysicalOperator> right, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::CROSS_PRODUCT, move(types), estimated_cardinality) {
	children.push_back(move(left));
	children.push_back(move(right));
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class CrossProductGlobalState : public GlobalSinkState {
public:
	explicit CrossProductGlobalState(ClientContext &context) : rhs_materialized(BufferAllocator::Get(context)) {
	}

	ChunkCollection rhs_materialized;
	mutex rhs_lock;
};

unique_ptr<GlobalSinkState> PhysicalCrossProduct::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<CrossProductGlobalState>(context);
}

SinkResultType PhysicalCrossProduct::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate_p,
                                          DataChunk &input) const {
	auto &sink = (CrossProductGlobalState &)state;
	lock_guard<mutex> client_guard(sink.rhs_lock);
	sink.rhs_materialized.Append(input);
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class CrossProductOperatorState : public OperatorState {
public:
	CrossProductOperatorState() : right_position(0) {
	}

	idx_t right_position;
};

unique_ptr<OperatorState> PhysicalCrossProduct::GetOperatorState(ExecutionContext &context) const {
	return make_unique<CrossProductOperatorState>();
}

OperatorResultType PhysicalCrossProduct::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                 GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = (CrossProductOperatorState &)state_p;
	auto &sink = (CrossProductGlobalState &)*sink_state;
	auto &right_collection = sink.rhs_materialized;

	if (sink.rhs_materialized.Count() == 0) {
		// no RHS: empty result
		return OperatorResultType::FINISHED;
	}
	if (state.right_position >= right_collection.Count()) {
		// ran out of entries on the RHS
		// reset the RHS and move to the next chunk on the LHS
		state.right_position = 0;
		return OperatorResultType::NEED_MORE_INPUT;
	}

	auto &left_chunk = input;
	// now match the current vector of the left relation with the current row
	// from the right relation
	chunk.SetCardinality(left_chunk.size());
	// create a reference to the vectors of the left column
	for (idx_t i = 0; i < left_chunk.ColumnCount(); i++) {
		chunk.data[i].Reference(left_chunk.data[i]);
	}
	// duplicate the values on the right side
	auto &right_chunk = right_collection.GetChunkForRow(state.right_position);
	auto row_in_chunk = state.right_position % STANDARD_VECTOR_SIZE;
	for (idx_t i = 0; i < right_collection.ColumnCount(); i++) {
		ConstantVector::Reference(chunk.data[left_chunk.ColumnCount() + i], right_chunk.data[i], row_in_chunk,
		                          right_chunk.size());
	}

	// for the next iteration, move to the next position on the right side
	state.right_position++;
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalCrossProduct::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
	PhysicalJoin::BuildJoinPipelines(executor, current, state, *this);
}

vector<const PhysicalOperator *> PhysicalCrossProduct::GetSources() const {
	return children[0]->GetSources();
}

} // namespace duckdb









namespace duckdb {

PhysicalDelimJoin::PhysicalDelimJoin(vector<LogicalType> types, unique_ptr<PhysicalOperator> original_join,
                                     vector<PhysicalOperator *> delim_scans, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::DELIM_JOIN, move(types), estimated_cardinality), join(move(original_join)),
      delim_scans(move(delim_scans)) {
	D_ASSERT(join->children.size() == 2);
	// now for the original join
	// we take its left child, this is the side that we will duplicate eliminate
	children.push_back(move(join->children[0]));

	// we replace it with a PhysicalChunkCollectionScan, that scans the ChunkCollection that we keep cached
	// the actual chunk collection to scan will be created in the DelimJoinGlobalState
	auto cached_chunk_scan = make_unique<PhysicalChunkScan>(children[0]->GetTypes(), PhysicalOperatorType::CHUNK_SCAN,
	                                                        estimated_cardinality);
	join->children[0] = move(cached_chunk_scan);
}

vector<PhysicalOperator *> PhysicalDelimJoin::GetChildren() const {
	vector<PhysicalOperator *> result;
	for (auto &child : children) {
		result.push_back(child.get());
	}
	result.push_back(join.get());
	result.push_back(distinct.get());
	return result;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class DelimJoinGlobalState : public GlobalSinkState {
public:
	explicit DelimJoinGlobalState(Allocator &allocator, const PhysicalDelimJoin *delim_join) : lhs_data(allocator) {
		D_ASSERT(delim_join->delim_scans.size() > 0);
		// set up the delim join chunk to scan in the original join
		auto &cached_chunk_scan = (PhysicalChunkScan &)*delim_join->join->children[0];
		cached_chunk_scan.collection = &lhs_data;
	}

	ChunkCollection lhs_data;
	mutex lhs_lock;

	void Merge(ChunkCollection &input) {
		lock_guard<mutex> guard(lhs_lock);
		lhs_data.Append(input);
	}
};

class DelimJoinLocalState : public LocalSinkState {
public:
	explicit DelimJoinLocalState(Allocator &allocator) : lhs_data(allocator) {
	}

	unique_ptr<LocalSinkState> distinct_state;
	ChunkCollection lhs_data;

	void Append(DataChunk &input) {
		lhs_data.Append(input);
	}
};

unique_ptr<GlobalSinkState> PhysicalDelimJoin::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_unique<DelimJoinGlobalState>(BufferAllocator::Get(context), this);
	distinct->sink_state = distinct->GetGlobalSinkState(context);
	if (delim_scans.size() > 1) {
		PhysicalHashAggregate::SetMultiScan(*distinct->sink_state);
	}
	return move(state);
}

unique_ptr<LocalSinkState> PhysicalDelimJoin::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_unique<DelimJoinLocalState>(Allocator::Get(context.client));
	state->distinct_state = distinct->GetLocalSinkState(context);
	return move(state);
}

SinkResultType PhysicalDelimJoin::Sink(ExecutionContext &context, GlobalSinkState &state_p, LocalSinkState &lstate_p,
                                       DataChunk &input) const {
	auto &lstate = (DelimJoinLocalState &)lstate_p;
	lstate.lhs_data.Append(input);
	distinct->Sink(context, *distinct->sink_state, *lstate.distinct_state, input);
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalDelimJoin::Combine(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate_p) const {
	auto &lstate = (DelimJoinLocalState &)lstate_p;
	auto &gstate = (DelimJoinGlobalState &)state;
	gstate.Merge(lstate.lhs_data);
	distinct->Combine(context, *distinct->sink_state, *lstate.distinct_state);
}

SinkFinalizeType PhysicalDelimJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &client,
                                             GlobalSinkState &gstate) const {
	// finalize the distinct HT
	D_ASSERT(distinct);
	distinct->Finalize(pipeline, event, client, *distinct->sink_state);
	return SinkFinalizeType::READY;
}

string PhysicalDelimJoin::ParamsToString() const {
	return join->ParamsToString();
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalDelimJoin::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
	op_state.reset();
	sink_state.reset();

	// duplicate eliminated join
	auto pipeline = make_shared<Pipeline>(executor);
	state.SetPipelineSink(*pipeline, this);
	current.AddDependency(pipeline);

	// recurse into the pipeline child
	children[0]->BuildPipelines(executor, *pipeline, state);
	if (type == PhysicalOperatorType::DELIM_JOIN) {
		// recurse into the actual join
		// any pipelines in there depend on the main pipeline
		// any scan of the duplicate eliminated data on the RHS depends on this pipeline
		// we add an entry to the mapping of (PhysicalOperator*) -> (Pipeline*)
		for (auto &delim_scan : delim_scans) {
			state.delim_join_dependencies[delim_scan] = pipeline.get();
		}
		join->BuildPipelines(executor, current, state);
	}
	if (!state.recursive_cte) {
		// regular pipeline: schedule it
		state.AddPipeline(executor, move(pipeline));
	} else {
		// CTE pipeline! add it to the CTE pipelines
		auto &cte = (PhysicalRecursiveCTE &)*state.recursive_cte;
		cte.pipelines.push_back(move(pipeline));
	}
}

} // namespace duckdb










namespace duckdb {

PhysicalHashJoin::PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                   const vector<idx_t> &left_projection_map,
                                   const vector<idx_t> &right_projection_map_p, vector<LogicalType> delim_types,
                                   idx_t estimated_cardinality, PerfectHashJoinStats perfect_join_stats)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::HASH_JOIN, move(cond), join_type, estimated_cardinality),
      right_projection_map(right_projection_map_p), delim_types(move(delim_types)),
      perfect_join_statistics(move(perfect_join_stats)) {

	children.push_back(move(left));
	children.push_back(move(right));

	D_ASSERT(left_projection_map.empty());
	for (auto &condition : conditions) {
		condition_types.push_back(condition.left->return_type);
	}

	// for ANTI, SEMI and MARK join, we only need to store the keys, so for these the build types are empty
	if (join_type != JoinType::ANTI && join_type != JoinType::SEMI && join_type != JoinType::MARK) {
		build_types = LogicalOperator::MapTypes(children[1]->GetTypes(), right_projection_map);
	}
}

PhysicalHashJoin::PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                   idx_t estimated_cardinality, PerfectHashJoinStats perfect_join_state)
    : PhysicalHashJoin(op, move(left), move(right), move(cond), join_type, {}, {}, {}, estimated_cardinality,
                       std::move(perfect_join_state)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class HashJoinLocalState : public LocalSinkState {
public:
	HashJoinLocalState(Allocator &allocator, const PhysicalHashJoin &hj) : build_executor(allocator) {
		if (!hj.right_projection_map.empty()) {
			build_chunk.Initialize(allocator, hj.build_types);
		}
		for (auto &cond : hj.conditions) {
			build_executor.AddExpression(*cond.right);
		}
		join_keys.Initialize(allocator, hj.condition_types);
	}

	DataChunk build_chunk;
	DataChunk join_keys;
	ExpressionExecutor build_executor;
};

class HashJoinGlobalState : public GlobalSinkState {
public:
	HashJoinGlobalState() {
	}

	//! The HT used by the join
	unique_ptr<JoinHashTable> hash_table;
	//! The perfect hash join executor (if any)
	unique_ptr<PerfectHashJoinExecutor> perfect_join_executor;
	//! Whether or not the hash table has been finalized
	bool finalized = false;
};

unique_ptr<GlobalSinkState> PhysicalHashJoin::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_unique<HashJoinGlobalState>();
	state->hash_table =
	    make_unique<JoinHashTable>(BufferManager::GetBufferManager(context), conditions, build_types, join_type);
	if (!delim_types.empty() && join_type == JoinType::MARK) {
		// correlated MARK join
		if (delim_types.size() + 1 == conditions.size()) {
			// the correlated MARK join has one more condition than the amount of correlated columns
			// this is the case in a correlated ANY() expression
			// in this case we need to keep track of additional entries, namely:
			// - (1) the total amount of elements per group
			// - (2) the amount of non-null elements per group
			// we need these to correctly deal with the cases of either:
			// - (1) the group being empty [in which case the result is always false, even if the comparison is NULL]
			// - (2) the group containing a NULL value [in which case FALSE becomes NULL]
			auto &info = state->hash_table->correlated_mark_join_info;

			vector<LogicalType> payload_types;
			vector<BoundAggregateExpression *> correlated_aggregates;
			unique_ptr<BoundAggregateExpression> aggr;

			// jury-rigging the GroupedAggregateHashTable
			// we need a count_star and a count to get counts with and without NULLs
			aggr = AggregateFunction::BindAggregateFunction(context, CountStarFun::GetFunction(), {}, nullptr, false);
			correlated_aggregates.push_back(&*aggr);
			payload_types.push_back(aggr->return_type);
			info.correlated_aggregates.push_back(move(aggr));

			auto count_fun = CountFun::GetFunction();
			vector<unique_ptr<Expression>> children;
			// this is a dummy but we need it to make the hash table understand whats going on
			children.push_back(make_unique_base<Expression, BoundReferenceExpression>(count_fun.return_type, 0));
			aggr = AggregateFunction::BindAggregateFunction(context, count_fun, move(children), nullptr, false);
			correlated_aggregates.push_back(&*aggr);
			payload_types.push_back(aggr->return_type);
			info.correlated_aggregates.push_back(move(aggr));

			auto &allocator = Allocator::Get(context);
			info.correlated_counts = make_unique<GroupedAggregateHashTable>(
			    allocator, BufferManager::GetBufferManager(context), delim_types, payload_types, correlated_aggregates);
			info.correlated_types = delim_types;
			info.group_chunk.Initialize(allocator, delim_types);
			info.result_chunk.Initialize(allocator, payload_types);
		}
	}
	// for perfect hash join
	state->perfect_join_executor =
	    make_unique<PerfectHashJoinExecutor>(*this, *state->hash_table, perfect_join_statistics);
	return move(state);
}

unique_ptr<LocalSinkState> PhysicalHashJoin::GetLocalSinkState(ExecutionContext &context) const {
	auto &allocator = Allocator::Get(context.client);
	auto state = make_unique<HashJoinLocalState>(allocator, *this);
	return move(state);
}

SinkResultType PhysicalHashJoin::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate_p,
                                      DataChunk &input) const {
	auto &sink = (HashJoinGlobalState &)state;
	auto &lstate = (HashJoinLocalState &)lstate_p;
	// resolve the join keys for the right chunk
	lstate.join_keys.Reset();
	lstate.build_executor.Execute(input, lstate.join_keys);
	// TODO: add statement to check for possible per
	// build the HT
	if (!right_projection_map.empty()) {
		// there is a projection map: fill the build chunk with the projected columns
		lstate.build_chunk.Reset();
		lstate.build_chunk.SetCardinality(input);
		for (idx_t i = 0; i < right_projection_map.size(); i++) {
			lstate.build_chunk.data[i].Reference(input.data[right_projection_map[i]]);
		}
		sink.hash_table->Build(lstate.join_keys, lstate.build_chunk);
	} else if (!build_types.empty()) {
		// there is not a projected map: place the entire right chunk in the HT
		sink.hash_table->Build(lstate.join_keys, input);
	} else {
		// there are only keys: place an empty chunk in the payload
		lstate.build_chunk.SetCardinality(input.size());
		sink.hash_table->Build(lstate.join_keys, lstate.build_chunk);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalHashJoin::Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const {
	auto &state = (HashJoinLocalState &)lstate;
	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(this, &state.build_executor, "build_executor", 1);
	client_profiler.Flush(context.thread.profiler);
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PhysicalHashJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            GlobalSinkState &gstate) const {
	auto &sink = (HashJoinGlobalState &)gstate;
	// check for possible perfect hash table
	auto use_perfect_hash = sink.perfect_join_executor->CanDoPerfectHashJoin();
	if (use_perfect_hash) {
		D_ASSERT(sink.hash_table->equality_types.size() == 1);
		auto key_type = sink.hash_table->equality_types[0];
		use_perfect_hash = sink.perfect_join_executor->BuildPerfectHashTable(key_type);
	}
	// In case of a large build side or duplicates, use regular hash join
	if (!use_perfect_hash) {
		sink.perfect_join_executor.reset();
		sink.hash_table->Finalize();
	}
	sink.finalized = true;
	if (sink.hash_table->Count() == 0 && EmptyResultIfRHSIsEmpty()) {
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class PhysicalHashJoinState : public OperatorState {
public:
	explicit PhysicalHashJoinState(Allocator &allocator) : probe_executor(allocator) {
	}

	DataChunk join_keys;
	ExpressionExecutor probe_executor;
	unique_ptr<JoinHashTable::ScanStructure> scan_structure;
	unique_ptr<OperatorState> perfect_hash_join_state;

public:
	void Finalize(PhysicalOperator *op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, &probe_executor, "probe_executor", 0);
	}
};

unique_ptr<OperatorState> PhysicalHashJoin::GetOperatorState(ExecutionContext &context) const {
	auto &allocator = Allocator::Get(context.client);
	auto &sink = (HashJoinGlobalState &)*sink_state;
	auto state = make_unique<PhysicalHashJoinState>(allocator);
	if (sink.perfect_join_executor) {
		state->perfect_hash_join_state = sink.perfect_join_executor->GetOperatorState(context);
	} else {
		state->join_keys.Initialize(allocator, condition_types);
		for (auto &cond : conditions) {
			state->probe_executor.AddExpression(*cond.left);
		}
	}
	return move(state);
}

OperatorResultType PhysicalHashJoin::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                             GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = (PhysicalHashJoinState &)state_p;
	auto &sink = (HashJoinGlobalState &)*sink_state;
	D_ASSERT(sink.finalized);

	if (sink.hash_table->Count() == 0 && EmptyResultIfRHSIsEmpty()) {
		return OperatorResultType::FINISHED;
	}
	if (sink.perfect_join_executor) {
		return sink.perfect_join_executor->ProbePerfectHashTable(context, input, chunk, *state.perfect_hash_join_state);
	}

	if (state.scan_structure) {
		// still have elements remaining from the previous probe (i.e. we got
		// >1024 elements in the previous probe)
		state.scan_structure->Next(state.join_keys, input, chunk);
		if (chunk.size() > 0) {
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
		state.scan_structure = nullptr;
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// probe the HT
	if (sink.hash_table->Count() == 0) {
		ConstructEmptyJoinResult(sink.hash_table->join_type, sink.hash_table->has_null, input, chunk);
		return OperatorResultType::NEED_MORE_INPUT;
	}
	// resolve the join keys for the left chunk
	state.join_keys.Reset();
	state.probe_executor.Execute(input, state.join_keys);

	// perform the actual probe
	state.scan_structure = sink.hash_table->Probe(state.join_keys);
	state.scan_structure->Next(state.join_keys, input, chunk);
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class HashJoinScanState : public GlobalSourceState {
public:
	explicit HashJoinScanState(const PhysicalHashJoin &op) : op(op) {
	}

	const PhysicalHashJoin &op;
	//! Only used for FULL OUTER JOIN: scan state of the final scan to find unmatched tuples in the build-side
	JoinHTScanState ht_scan_state;

	idx_t MaxThreads() override {
		auto &sink = (HashJoinGlobalState &)*op.sink_state;
		return sink.hash_table->Count() / (STANDARD_VECTOR_SIZE * 10);
	}
};

unique_ptr<GlobalSourceState> PhysicalHashJoin::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<HashJoinScanState>(*this);
}

void PhysicalHashJoin::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                               LocalSourceState &lstate) const {
	D_ASSERT(IsRightOuterJoin(join_type));
	// check if we need to scan any unmatched tuples from the RHS for the full/right outer join
	auto &sink = (HashJoinGlobalState &)*sink_state;
	auto &state = (HashJoinScanState &)gstate;
	sink.hash_table->ScanFullOuter(chunk, state.ht_scan_state);
}

} // namespace duckdb













#include <thread>

namespace duckdb {

PhysicalIEJoin::PhysicalIEJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                               unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                               idx_t estimated_cardinality)
    : PhysicalRangeJoin(op, PhysicalOperatorType::IE_JOIN, move(left), move(right), move(cond), join_type,
                        estimated_cardinality) {

	// 1. let L1 (resp. L2) be the array of column X (resp. Y)
	D_ASSERT(conditions.size() >= 2);
	lhs_orders.resize(2);
	rhs_orders.resize(2);
	for (idx_t i = 0; i < 2; ++i) {
		auto &cond = conditions[i];
		D_ASSERT(cond.left->return_type == cond.right->return_type);
		join_key_types.push_back(cond.left->return_type);

		// Convert the conditions to sort orders
		auto left = cond.left->Copy();
		auto right = cond.right->Copy();
		auto sense = OrderType::INVALID;

		// 2. if (op1 ∈ {>, ≥}) sort L1 in descending order
		// 3. else if (op1 ∈ {<, ≤}) sort L1 in ascending order
		// 4. if (op2 ∈ {>, ≥}) sort L2 in ascending order
		// 5. else if (op2 ∈ {<, ≤}) sort L2 in descending order
		switch (cond.comparison) {
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			sense = i ? OrderType::ASCENDING : OrderType::DESCENDING;
			break;
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			sense = i ? OrderType::DESCENDING : OrderType::ASCENDING;
			break;
		default:
			throw NotImplementedException("Unimplemented join type for IEJoin");
		}
		lhs_orders[i].emplace_back(BoundOrderByNode(sense, OrderByNullType::NULLS_LAST, move(left)));
		rhs_orders[i].emplace_back(BoundOrderByNode(sense, OrderByNullType::NULLS_LAST, move(right)));
	}

	for (idx_t i = 2; i < conditions.size(); ++i) {
		auto &cond = conditions[i];
		D_ASSERT(cond.left->return_type == cond.right->return_type);
		join_key_types.push_back(cond.left->return_type);
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class IEJoinLocalState : public LocalSinkState {
public:
	using LocalSortedTable = PhysicalRangeJoin::LocalSortedTable;

	IEJoinLocalState(Allocator &allocator, const PhysicalRangeJoin &op, const idx_t child)
	    : table(allocator, op, child) {
	}

	//! The local sort state
	LocalSortedTable table;
};

class IEJoinGlobalState : public GlobalSinkState {
public:
	using GlobalSortedTable = PhysicalRangeJoin::GlobalSortedTable;

public:
	IEJoinGlobalState(ClientContext &context, const PhysicalIEJoin &op) : child(0) {
		tables.resize(2);
		RowLayout lhs_layout;
		lhs_layout.Initialize(op.children[0]->types);
		vector<BoundOrderByNode> lhs_order;
		lhs_order.emplace_back(op.lhs_orders[0][0].Copy());
		tables[0] = make_unique<GlobalSortedTable>(context, lhs_order, lhs_layout);

		RowLayout rhs_layout;
		rhs_layout.Initialize(op.children[1]->types);
		vector<BoundOrderByNode> rhs_order;
		rhs_order.emplace_back(op.rhs_orders[0][0].Copy());
		tables[1] = make_unique<GlobalSortedTable>(context, rhs_order, rhs_layout);
	}

	IEJoinGlobalState(IEJoinGlobalState &prev)
	    : GlobalSinkState(prev), tables(move(prev.tables)), child(prev.child + 1) {
	}

	void Sink(DataChunk &input, IEJoinLocalState &lstate) {
		auto &table = *tables[child];
		auto &global_sort_state = table.global_sort_state;
		auto &local_sort_state = lstate.table.local_sort_state;

		// Sink the data into the local sort state
		lstate.table.Sink(input, global_sort_state);

		// When sorting data reaches a certain size, we sort it
		if (local_sort_state.SizeInBytes() >= table.memory_per_thread) {
			local_sort_state.Sort(global_sort_state, true);
		}
	}

	vector<unique_ptr<GlobalSortedTable>> tables;
	size_t child;
};

unique_ptr<GlobalSinkState> PhysicalIEJoin::GetGlobalSinkState(ClientContext &context) const {
	D_ASSERT(!sink_state);
	return make_unique<IEJoinGlobalState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalIEJoin::GetLocalSinkState(ExecutionContext &context) const {
	idx_t sink_child = 0;
	if (sink_state) {
		const auto &ie_sink = (IEJoinGlobalState &)*sink_state;
		sink_child = ie_sink.child;
	}
	return make_unique<IEJoinLocalState>(Allocator::Get(context.client), *this, sink_child);
}

SinkResultType PhysicalIEJoin::Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
                                    DataChunk &input) const {
	auto &gstate = (IEJoinGlobalState &)gstate_p;
	auto &lstate = (IEJoinLocalState &)lstate_p;

	gstate.Sink(input, lstate);

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalIEJoin::Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const {
	auto &gstate = (IEJoinGlobalState &)gstate_p;
	auto &lstate = (IEJoinLocalState &)lstate_p;
	gstate.tables[gstate.child]->Combine(lstate.table);
	auto &client_profiler = QueryProfiler::Get(context.client);

	context.thread.profiler.Flush(this, &lstate.table.executor, gstate.child ? "rhs_executor" : "lhs_executor", 1);
	client_profiler.Flush(context.thread.profiler);
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PhysicalIEJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          GlobalSinkState &gstate_p) const {
	auto &gstate = (IEJoinGlobalState &)gstate_p;
	auto &table = *gstate.tables[gstate.child];
	auto &global_sort_state = table.global_sort_state;

	if ((gstate.child == 1 && IsRightOuterJoin(join_type)) || (gstate.child == 0 && IsLeftOuterJoin(join_type))) {
		// for FULL/LEFT/RIGHT OUTER JOIN, initialize found_match to false for every tuple
		table.IntializeMatches();
	}
	if (gstate.child == 1 && global_sort_state.sorted_blocks.empty() && EmptyResultIfRHSIsEmpty()) {
		// Empty input!
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Sort the current input child
	table.Finalize(pipeline, event);

	// Move to the next input child
	++gstate.child;

	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
OperatorResultType PhysicalIEJoin::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                           GlobalOperatorState &gstate, OperatorState &state) const {
	return OperatorResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
struct SBIterator {
	static int ComparisonValue(ExpressionType comparison) {
		switch (comparison) {
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_GREATERTHAN:
			return -1;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			return 0;
		default:
			throw InternalException("Unimplemented comparison type for IEJoin!");
		}
	}

	explicit SBIterator(GlobalSortState &gss, ExpressionType comparison, idx_t entry_idx_p = 0)
	    : sort_layout(gss.sort_layout), block_count(gss.sorted_blocks[0]->radix_sorting_data.size()),
	      block_capacity(gss.block_capacity), cmp_size(sort_layout.comparison_size), entry_size(sort_layout.entry_size),
	      all_constant(sort_layout.all_constant), external(gss.external), cmp(ComparisonValue(comparison)),
	      scan(gss.buffer_manager, gss), block_ptr(nullptr), entry_ptr(nullptr) {

		scan.sb = gss.sorted_blocks[0].get();
		scan.block_idx = block_count;
		SetIndex(entry_idx_p);
	}

	inline idx_t GetIndex() const {
		return entry_idx;
	}

	inline void SetIndex(idx_t entry_idx_p) {
		const auto new_block_idx = entry_idx_p / block_capacity;
		if (new_block_idx != scan.block_idx) {
			scan.SetIndices(new_block_idx, 0);
			if (new_block_idx < block_count) {
				scan.PinRadix(scan.block_idx);
				block_ptr = scan.RadixPtr();
				if (!all_constant) {
					scan.PinData(*scan.sb->blob_sorting_data);
				}
			}
		}

		scan.entry_idx = entry_idx_p % block_capacity;
		entry_ptr = block_ptr + scan.entry_idx * entry_size;
		entry_idx = entry_idx_p;
	}

	inline SBIterator &operator++() {
		if (++scan.entry_idx < block_capacity) {
			entry_ptr += entry_size;
			++entry_idx;
		} else {
			SetIndex(entry_idx + 1);
		}

		return *this;
	}

	inline SBIterator &operator--() {
		if (scan.entry_idx) {
			--scan.entry_idx;
			--entry_idx;
			entry_ptr -= entry_size;
		} else {
			SetIndex(entry_idx - 1);
		}

		return *this;
	}

	inline bool Compare(const SBIterator &other) const {
		int comp_res;
		if (all_constant) {
			comp_res = FastMemcmp(entry_ptr, other.entry_ptr, cmp_size);
		} else {
			comp_res = Comparators::CompareTuple(scan, other.scan, entry_ptr, other.entry_ptr, sort_layout, external);
		}

		return comp_res <= cmp;
	}

	// Fixed comparison parameters
	const SortLayout &sort_layout;
	const idx_t block_count;
	const idx_t block_capacity;
	const size_t cmp_size;
	const size_t entry_size;
	const bool all_constant;
	const bool external;
	const int cmp;

	// Iteration state
	SBScanState scan;
	idx_t entry_idx;
	data_ptr_t block_ptr;
	data_ptr_t entry_ptr;
};

struct IEJoinUnion {
	using SortedTable = PhysicalRangeJoin::GlobalSortedTable;

	static idx_t AppendKey(SortedTable &table, ExpressionExecutor &executor, SortedTable &marked, int64_t increment,
	                       int64_t base, const idx_t block_idx);

	static void Sort(SortedTable &table) {
		auto &global_sort_state = table.global_sort_state;
		global_sort_state.PrepareMergePhase();
		while (global_sort_state.sorted_blocks.size() > 1) {
			global_sort_state.InitializeMergeRound();
			MergeSorter merge_sorter(global_sort_state, global_sort_state.buffer_manager);
			merge_sorter.PerformInMergeRound();
			global_sort_state.CompleteMergeRound(true);
		}
	}

	template <typename T>
	static vector<T> ExtractColumn(SortedTable &table, idx_t col_idx) {
		vector<T> result;
		result.reserve(table.count);

		auto &gstate = table.global_sort_state;
		auto &blocks = *gstate.sorted_blocks[0]->payload_data;
		PayloadScanner scanner(blocks, gstate, false);

		DataChunk payload;
		payload.Initialize(Allocator::DefaultAllocator(), gstate.payload_layout.GetTypes());
		for (;;) {
			scanner.Scan(payload);
			const auto count = payload.size();
			if (!count) {
				break;
			}

			const auto data_ptr = FlatVector::GetData<T>(payload.data[col_idx]);
			result.insert(result.end(), data_ptr, data_ptr + count);
		}

		return result;
	}

	IEJoinUnion(ClientContext &context, const PhysicalIEJoin &op, SortedTable &t1, const idx_t b1, SortedTable &t2,
	            const idx_t b2);

	idx_t SearchL1(idx_t pos);
	bool NextRow();

	//! Inverted loop
	idx_t JoinComplexBlocks(SelectionVector &lsel, SelectionVector &rsel);

	//! L1
	unique_ptr<SortedTable> l1;
	//! L2
	unique_ptr<SortedTable> l2;

	//! Li
	vector<int64_t> li;
	//! P
	vector<idx_t> p;

	//! B
	vector<validity_t> bit_array;
	ValidityMask bit_mask;

	//! Bloom Filter
	static constexpr idx_t BLOOM_CHUNK_BITS = 1024;
	idx_t bloom_count;
	vector<validity_t> bloom_array;
	ValidityMask bloom_filter;

	//! Iteration state
	idx_t n;
	idx_t i;
	idx_t j;
	unique_ptr<SBIterator> op1;
	unique_ptr<SBIterator> off1;
	unique_ptr<SBIterator> op2;
	unique_ptr<SBIterator> off2;
	int64_t lrid;
};

idx_t IEJoinUnion::AppendKey(SortedTable &table, ExpressionExecutor &executor, SortedTable &marked, int64_t increment,
                             int64_t base, const idx_t block_idx) {
	LocalSortState local_sort_state;
	local_sort_state.Initialize(marked.global_sort_state, marked.global_sort_state.buffer_manager);

	// Reading
	const auto valid = table.count - table.has_null;
	auto &gstate = table.global_sort_state;
	PayloadScanner scanner(gstate, block_idx);
	auto table_idx = block_idx * gstate.block_capacity;

	DataChunk scanned;
	scanned.Initialize(Allocator::DefaultAllocator(), scanner.GetPayloadTypes());

	// Writing
	auto types = local_sort_state.sort_layout->logical_types;
	const idx_t payload_idx = types.size();

	const auto &payload_types = local_sort_state.payload_layout->GetTypes();
	types.insert(types.end(), payload_types.begin(), payload_types.end());
	const idx_t rid_idx = types.size() - 1;

	DataChunk keys;
	DataChunk payload;
	keys.Initialize(Allocator::DefaultAllocator(), types);

	idx_t inserted = 0;
	for (auto rid = base; table_idx < valid;) {
		scanner.Scan(scanned);

		// NULLs are at the end, so stop when we reach them
		auto scan_count = scanned.size();
		if (table_idx + scan_count > valid) {
			scan_count = valid - table_idx;
			scanned.SetCardinality(scan_count);
		}
		if (scan_count == 0) {
			break;
		}
		table_idx += scan_count;

		// Compute the input columns from the payload
		keys.Reset();
		keys.Split(payload, rid_idx);
		executor.Execute(scanned, keys);

		// Mark the rid column
		payload.data[0].Sequence(rid, increment);
		payload.SetCardinality(scan_count);
		keys.Fuse(payload);
		rid += increment * scan_count;

		// Sort on the sort columns (which will no longer be needed)
		keys.Split(payload, payload_idx);
		local_sort_state.SinkChunk(keys, payload);
		inserted += scan_count;
		keys.Fuse(payload);

		// Flush when we have enough data
		if (local_sort_state.SizeInBytes() >= marked.memory_per_thread) {
			local_sort_state.Sort(marked.global_sort_state, true);
		}
	}
	marked.global_sort_state.AddLocalState(local_sort_state);
	marked.count += inserted;

	return inserted;
}

IEJoinUnion::IEJoinUnion(ClientContext &context, const PhysicalIEJoin &op, SortedTable &t1, const idx_t b1,
                         SortedTable &t2, const idx_t b2)
    : n(0), i(0) {
	auto &allocator = Allocator::Get(context);
	// input : query Q with 2 join predicates t1.X op1 t2.X' and t1.Y op2 t2.Y', tables T, T' of sizes m and n resp.
	// output: a list of tuple pairs (ti , tj)
	// Note that T/T' are already sorted on X/X' and contain the payload data
	// We only join the two block numbers and use the sizes of the blocks as the counts

	// 0. Filter out tables with no overlap
	if (!t1.BlockSize(b1) || !t2.BlockSize(b2)) {
		return;
	}

	const auto &cmp1 = op.conditions[0].comparison;
	SBIterator bounds1(t1.global_sort_state, cmp1);
	SBIterator bounds2(t2.global_sort_state, cmp1);

	// t1.X[0] op1 t2.X'[-1]
	bounds1.SetIndex(bounds1.block_capacity * b1);
	bounds2.SetIndex(bounds2.block_capacity * b2 + t2.BlockSize(b2) - 1);
	if (!bounds1.Compare(bounds2)) {
		return;
	}

	// 1. let L1 (resp. L2) be the array of column X (resp. Y )
	const auto &order1 = op.lhs_orders[0][0];
	const auto &order2 = op.lhs_orders[1][0];

	// 2. if (op1 ∈ {>, ≥}) sort L1 in descending order
	// 3. else if (op1 ∈ {<, ≤}) sort L1 in ascending order

	// For the union algorithm, we make a unified table with the keys and the rids as the payload:
	//		X/X', Y/Y', R/R'/Li
	// The first position is the sort key.
	vector<LogicalType> types;
	types.emplace_back(order2.expression->return_type);
	types.emplace_back(LogicalType::BIGINT);
	RowLayout payload_layout;
	payload_layout.Initialize(types);

	// Sort on the first expression
	auto ref = make_unique<BoundReferenceExpression>(order1.expression->return_type, 0);
	vector<BoundOrderByNode> orders;
	orders.emplace_back(BoundOrderByNode(order1.type, order1.null_order, move(ref)));

	l1 = make_unique<SortedTable>(context, orders, payload_layout);

	// LHS has positive rids
	ExpressionExecutor l_executor(allocator);
	l_executor.AddExpression(*order1.expression);
	l_executor.AddExpression(*order2.expression);
	AppendKey(t1, l_executor, *l1, 1, 1, b1);

	// RHS has negative rids
	ExpressionExecutor r_executor(allocator);
	r_executor.AddExpression(*op.rhs_orders[0][0].expression);
	r_executor.AddExpression(*op.rhs_orders[1][0].expression);
	AppendKey(t2, r_executor, *l1, -1, -1, b2);

	Sort(*l1);

	op1 = make_unique<SBIterator>(l1->global_sort_state, cmp1);
	off1 = make_unique<SBIterator>(l1->global_sort_state, cmp1);

	// We don't actually need the L1 column, just its sort key, which is in the sort blocks
	li = ExtractColumn<int64_t>(*l1, types.size() - 1);

	// 4. if (op2 ∈ {>, ≥}) sort L2 in ascending order
	// 5. else if (op2 ∈ {<, ≤}) sort L2 in descending order

	// We sort on Y/Y' to obtain the sort keys and the permutation array.
	// For this we just need a two-column table of Y, P
	types.clear();
	types.emplace_back(LogicalType::BIGINT);
	payload_layout.Initialize(types);

	// Sort on the first expression
	orders.clear();
	ref = make_unique<BoundReferenceExpression>(order2.expression->return_type, 0);
	orders.emplace_back(BoundOrderByNode(order2.type, order2.null_order, move(ref)));

	ExpressionExecutor executor(allocator);
	executor.AddExpression(*orders[0].expression);

	l2 = make_unique<SortedTable>(context, orders, payload_layout);
	for (idx_t base = 0, block_idx = 0; block_idx < l1->BlockCount(); ++block_idx) {
		base += AppendKey(*l1, executor, *l2, 1, base, block_idx);
	}

	Sort(*l2);

	// We don't actually need the L2 column, just its sort key, which is in the sort blocks

	// 6. compute the permutation array P of L2 w.r.t. L1
	p = ExtractColumn<idx_t>(*l2, types.size() - 1);

	// 7. initialize bit-array B (|B| = n), and set all bits to 0
	n = l2->count.load();
	bit_array.resize(ValidityMask::EntryCount(n), 0);
	bit_mask.Initialize(bit_array.data());

	// Bloom filter
	bloom_count = (n + (BLOOM_CHUNK_BITS - 1)) / BLOOM_CHUNK_BITS;
	bloom_array.resize(ValidityMask::EntryCount(bloom_count), 0);
	bloom_filter.Initialize(bloom_array.data());

	// 11. for(i←1 to n) do
	const auto &cmp2 = op.conditions[1].comparison;
	op2 = make_unique<SBIterator>(l2->global_sort_state, cmp2);
	off2 = make_unique<SBIterator>(l2->global_sort_state, cmp2);
	i = 0;
	j = 0;
	(void)NextRow();
}

idx_t IEJoinUnion::SearchL1(idx_t pos) {
	// Perform an exponential search in the appropriate direction
	op1->SetIndex(pos);

	idx_t step = 1;
	auto hi = pos;
	auto lo = pos;
	if (!op1->cmp) {
		// Scan left for loose inequality
		lo -= MinValue(step, lo);
		step *= 2;
		off1->SetIndex(lo);
		while (lo > 0 && op1->Compare(*off1)) {
			hi = lo;
			lo -= MinValue(step, lo);
			step *= 2;
			off1->SetIndex(lo);
		}
	} else {
		// Scan right for strict inequality
		hi += MinValue(step, n - hi);
		step *= 2;
		off1->SetIndex(hi);
		while (hi < n && !op1->Compare(*off1)) {
			lo = hi;
			hi += MinValue(step, n - hi);
			step *= 2;
			off1->SetIndex(hi);
		}
	}

	// Binary search the target area
	while (lo < hi) {
		const auto mid = lo + (hi - lo) / 2;
		off1->SetIndex(mid);
		if (op1->Compare(*off1)) {
			hi = mid;
		} else {
			lo = mid + 1;
		}
	}

	off1->SetIndex(lo);

	return lo;
}

bool IEJoinUnion::NextRow() {
	for (; i < n; ++i) {
		// 12. pos ← P[i]
		auto pos = p[i];
		lrid = li[pos];
		if (lrid < 0) {
			continue;
		}

		// 16. B[pos] ← 1
		op2->SetIndex(i);
		for (; off2->GetIndex() < n; ++(*off2)) {
			if (!off2->Compare(*op2)) {
				break;
			}
			const auto p2 = p[off2->GetIndex()];
			if (li[p2] < 0) {
				// Only mark rhs matches.
				bit_mask.SetValid(p2);
				bloom_filter.SetValid(p2 / BLOOM_CHUNK_BITS);
			}
		}

		// 9.  if (op1 ∈ {≤,≥} and op2 ∈ {≤,≥}) eqOff = 0
		// 10. else eqOff = 1
		// No, because there could be more than one equal value.
		// Find the leftmost off1 where L1[pos] op1 L1[off1..n]
		// These are the rows that satisfy the op1 condition
		// and that is where we should start scanning B from
		j = SearchL1(pos);

		return true;
	}
	return false;
}

static idx_t NextValid(const ValidityMask &bits, idx_t j, const idx_t n) {
	if (j >= n) {
		return n;
	}

	// We can do a first approximation by checking entries one at a time
	// which gives 64:1.
	idx_t entry_idx, idx_in_entry;
	bits.GetEntryIndex(j, entry_idx, idx_in_entry);
	auto entry = bits.GetValidityEntry(entry_idx++);

	// Trim the bits before the start position
	entry &= (ValidityMask::ValidityBuffer::MAX_ENTRY << idx_in_entry);

	// Check the non-ragged entries
	for (const auto entry_count = bits.EntryCount(n); entry_idx < entry_count; ++entry_idx) {
		if (entry) {
			for (; idx_in_entry < bits.BITS_PER_VALUE; ++idx_in_entry, ++j) {
				if (bits.RowIsValid(entry, idx_in_entry)) {
					return j;
				}
			}
		} else {
			j += bits.BITS_PER_VALUE - idx_in_entry;
		}

		entry = bits.GetValidityEntry(entry_idx);
		idx_in_entry = 0;
	}

	// Check the final entry
	for (; j < n; ++idx_in_entry, ++j) {
		if (bits.RowIsValid(entry, idx_in_entry)) {
			return j;
		}
	}

	return j;
}

idx_t IEJoinUnion::JoinComplexBlocks(SelectionVector &lsel, SelectionVector &rsel) {
	// 8. initialize join result as an empty list for tuple pairs
	idx_t result_count = 0;

	// 11. for(i←1 to n) do
	while (i < n) {
		// 13. for (j ← pos+eqOff to n) do
		for (;;) {
			// 14. if B[j] = 1 then

			//	Use the Bloom filter to find candidate blocks
			while (j < n) {
				auto bloom_begin = NextValid(bloom_filter, j / BLOOM_CHUNK_BITS, bloom_count) * BLOOM_CHUNK_BITS;
				auto bloom_end = MinValue<idx_t>(n, bloom_begin + BLOOM_CHUNK_BITS);

				j = MaxValue<idx_t>(j, bloom_begin);
				j = NextValid(bit_mask, j, bloom_end);
				if (j < bloom_end) {
					break;
				}
			}

			if (j >= n) {
				break;
			}

			// Filter out tuples with the same sign (they come from the same table)
			const auto rrid = li[j];
			++j;

			// 15. add tuples w.r.t. (L1[j], L1[i]) to join result
			if (lrid > 0 && rrid < 0) {
				lsel.set_index(result_count, sel_t(+lrid - 1));
				rsel.set_index(result_count, sel_t(-rrid - 1));
				++result_count;
				if (result_count == STANDARD_VECTOR_SIZE) {
					// out of space!
					return result_count;
				}
			}
		}
		++i;

		if (!NextRow()) {
			break;
		}
	}

	return result_count;
}

class IEJoinState : public OperatorState {
public:
	explicit IEJoinState(Allocator &allocator, const PhysicalIEJoin &op) : local_left(allocator, op, 0) {};

	IEJoinLocalState local_left;
};

class IEJoinLocalSourceState : public LocalSourceState {
public:
	explicit IEJoinLocalSourceState(Allocator &allocator, const PhysicalIEJoin &op)
	    : op(op), true_sel(STANDARD_VECTOR_SIZE), left_executor(allocator), right_executor(allocator),
	      left_matches(nullptr), right_matches(nullptr) {

		if (op.conditions.size() < 3) {
			return;
		}

		vector<LogicalType> left_types;
		vector<LogicalType> right_types;
		for (idx_t i = 2; i < op.conditions.size(); ++i) {
			const auto &cond = op.conditions[i];

			left_types.push_back(cond.left->return_type);
			left_executor.AddExpression(*cond.left);

			right_types.push_back(cond.left->return_type);
			right_executor.AddExpression(*cond.right);
		}

		left_keys.Initialize(allocator, left_types);
		right_keys.Initialize(allocator, right_types);
	}

	idx_t SelectOuterRows(bool *matches) {
		idx_t count = 0;
		for (; outer_idx < outer_count; ++outer_idx) {
			if (!matches[outer_idx]) {
				true_sel.set_index(count++, outer_idx);
				if (count >= STANDARD_VECTOR_SIZE) {
					break;
				}
			}
		}

		return count;
	}

	const PhysicalIEJoin &op;

	// Joining
	unique_ptr<IEJoinUnion> joiner;

	idx_t left_base;
	idx_t left_block_index;

	idx_t right_base;
	idx_t right_block_index;

	// Trailing predicates
	SelectionVector true_sel;

	ExpressionExecutor left_executor;
	DataChunk left_keys;

	ExpressionExecutor right_executor;
	DataChunk right_keys;

	// Outer joins
	idx_t outer_idx;
	idx_t outer_count;
	bool *left_matches;
	bool *right_matches;
};

void PhysicalIEJoin::ResolveComplexJoin(ExecutionContext &context, DataChunk &chunk, LocalSourceState &state_p) const {
	auto &state = (IEJoinLocalSourceState &)state_p;
	auto &ie_sink = (IEJoinGlobalState &)*sink_state;
	auto &left_table = *ie_sink.tables[0];
	auto &right_table = *ie_sink.tables[1];

	const auto left_cols = children[0]->GetTypes().size();
	do {
		SelectionVector lsel(STANDARD_VECTOR_SIZE);
		SelectionVector rsel(STANDARD_VECTOR_SIZE);
		auto result_count = state.joiner->JoinComplexBlocks(lsel, rsel);
		if (result_count == 0) {
			// exhausted this pair
			return;
		}

		// found matches: extract them
		chunk.Reset();
		SliceSortedPayload(chunk, left_table.global_sort_state, state.left_block_index, lsel, result_count, 0);
		SliceSortedPayload(chunk, right_table.global_sort_state, state.right_block_index, rsel, result_count,
		                   left_cols);
		chunk.SetCardinality(result_count);

		auto sel = FlatVector::IncrementalSelectionVector();
		if (conditions.size() > 2) {
			// If there are more expressions to compute,
			// split the result chunk into the left and right halves
			// so we can compute the values for comparison.
			const auto tail_cols = conditions.size() - 2;

			DataChunk right_chunk;
			chunk.Split(right_chunk, left_cols);
			state.left_executor.SetChunk(chunk);
			state.right_executor.SetChunk(right_chunk);

			auto tail_count = result_count;
			auto true_sel = &state.true_sel;
			for (size_t cmp_idx = 0; cmp_idx < tail_cols; ++cmp_idx) {
				auto &left = state.left_keys.data[cmp_idx];
				state.left_executor.ExecuteExpression(cmp_idx, left);

				auto &right = state.right_keys.data[cmp_idx];
				state.right_executor.ExecuteExpression(cmp_idx, right);

				if (tail_count < result_count) {
					left.Slice(*sel, tail_count);
					right.Slice(*sel, tail_count);
				}
				tail_count = SelectJoinTail(conditions[cmp_idx + 2].comparison, left, right, sel, tail_count, true_sel);
				sel = true_sel;
			}
			chunk.Fuse(right_chunk);

			if (tail_count < result_count) {
				result_count = tail_count;
				chunk.Slice(*sel, result_count);
			}
		}

		// found matches: mark the found matches if required
		if (left_table.found_match) {
			for (idx_t i = 0; i < result_count; i++) {
				left_table.found_match[state.left_base + lsel[sel->get_index(i)]] = true;
			}
		}
		if (right_table.found_match) {
			for (idx_t i = 0; i < result_count; i++) {
				right_table.found_match[state.right_base + rsel[sel->get_index(i)]] = true;
			}
		}
		chunk.Verify();
	} while (chunk.size() == 0);
}

class IEJoinGlobalSourceState : public GlobalSourceState {
public:
	explicit IEJoinGlobalSourceState(const PhysicalIEJoin &op)
	    : op(op), initialized(false), next_pair(0), completed(0), left_outers(0), next_left(0), right_outers(0),
	      next_right(0) {
	}

	void Initialize(IEJoinGlobalState &sink_state) {
		lock_guard<mutex> initializing(lock);
		if (initialized) {
			return;
		}

		// Compute the starting row for reach block
		// (In theory these are all the same size, but you never know...)
		auto &left_table = *sink_state.tables[0];
		const auto left_blocks = left_table.BlockCount();
		idx_t left_base = 0;

		for (size_t lhs = 0; lhs < left_blocks; ++lhs) {
			left_bases.emplace_back(left_base);
			left_base += left_table.BlockSize(lhs);
		}

		auto &right_table = *sink_state.tables[1];
		const auto right_blocks = right_table.BlockCount();
		idx_t right_base = 0;
		for (size_t rhs = 0; rhs < right_blocks; ++rhs) {
			right_bases.emplace_back(right_base);
			right_base += right_table.BlockSize(rhs);
		}

		// Outer join block counts
		if (left_table.found_match) {
			left_outers = left_blocks;
		}

		if (right_table.found_match) {
			right_outers = right_blocks;
		}

		// Ready for action
		initialized = true;
	}

public:
	idx_t MaxThreads() override {
		// We can't leverage any more threads than block pairs.
		const auto &sink_state = ((IEJoinGlobalState &)*op.sink_state);
		return sink_state.tables[0]->BlockCount() * sink_state.tables[1]->BlockCount();
	}

	void GetNextPair(ClientContext &client, IEJoinGlobalState &gstate, IEJoinLocalSourceState &lstate) {
		auto &left_table = *gstate.tables[0];
		auto &right_table = *gstate.tables[1];

		const auto left_blocks = left_table.BlockCount();
		const auto right_blocks = right_table.BlockCount();
		const auto pair_count = left_blocks * right_blocks;

		// Regular block
		const auto i = next_pair++;
		if (i < pair_count) {
			const auto b1 = i / right_blocks;
			const auto b2 = i % right_blocks;

			lstate.left_block_index = b1;
			lstate.left_base = left_bases[b1];

			lstate.right_block_index = b2;
			lstate.right_base = right_bases[b2];

			lstate.joiner = make_unique<IEJoinUnion>(client, op, left_table, b1, right_table, b2);
			return;
		} else {
			--next_pair;
		}

		// Outer joins
		if (!left_outers && !right_outers) {
			return;
		}

		// Spin wait for regular blocks to finish(!)
		while (completed < pair_count) {
			std::this_thread::yield();
		}

		// Left outer blocks
		const auto l = next_left++;
		if (l < left_outers) {
			lstate.left_block_index = l;
			lstate.left_base = left_bases[l];

			lstate.left_matches = left_table.found_match.get() + lstate.left_base;
			lstate.outer_idx = 0;
			lstate.outer_count = left_table.BlockSize(l);
			return;
		} else {
			lstate.left_matches = nullptr;
			--next_left;
		}

		// Right outer block
		const auto r = next_right++;
		if (r < right_outers) {
			lstate.right_block_index = r;
			lstate.right_base = right_bases[r];

			lstate.right_matches = right_table.found_match.get() + lstate.right_base;
			lstate.outer_idx = 0;
			lstate.outer_count = right_table.BlockSize(r);
			return;
		} else {
			lstate.right_matches = nullptr;
			--next_right;
		}
	}

	void PairCompleted(ClientContext &client, IEJoinGlobalState &gstate, IEJoinLocalSourceState &lstate) {
		lstate.joiner.reset();
		++completed;
		GetNextPair(client, gstate, lstate);
	}

	const PhysicalIEJoin &op;

	mutex lock;
	bool initialized;

	// Join queue state
	std::atomic<size_t> next_pair;
	std::atomic<size_t> completed;

	// Block base row number
	vector<idx_t> left_bases;
	vector<idx_t> right_bases;

	// Outer joins
	idx_t left_outers;
	std::atomic<idx_t> next_left;

	idx_t right_outers;
	std::atomic<idx_t> next_right;
};

unique_ptr<GlobalSourceState> PhysicalIEJoin::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<IEJoinGlobalSourceState>(*this);
}

unique_ptr<LocalSourceState> PhysicalIEJoin::GetLocalSourceState(ExecutionContext &context,
                                                                 GlobalSourceState &gstate) const {
	return make_unique<IEJoinLocalSourceState>(Allocator::Get(context.client), *this);
}

void PhysicalIEJoin::GetData(ExecutionContext &context, DataChunk &result, GlobalSourceState &gstate,
                             LocalSourceState &lstate) const {
	auto &ie_sink = (IEJoinGlobalState &)*sink_state;
	auto &ie_gstate = (IEJoinGlobalSourceState &)gstate;
	auto &ie_lstate = (IEJoinLocalSourceState &)lstate;

	ie_gstate.Initialize(ie_sink);

	if (!ie_lstate.joiner) {
		ie_gstate.GetNextPair(context.client, ie_sink, ie_lstate);
	}

	// Process INNER results
	while (ie_lstate.joiner) {
		ResolveComplexJoin(context, result, ie_lstate);

		if (result.size()) {
			return;
		}

		ie_gstate.PairCompleted(context.client, ie_sink, ie_lstate);
	}

	// Process LEFT OUTER results
	const auto left_cols = children[0]->GetTypes().size();
	while (ie_lstate.left_matches) {
		const idx_t count = ie_lstate.SelectOuterRows(ie_lstate.left_matches);
		if (!count) {
			ie_gstate.GetNextPair(context.client, ie_sink, ie_lstate);
			continue;
		}

		SliceSortedPayload(result, ie_sink.tables[0]->global_sort_state, ie_lstate.left_base, ie_lstate.true_sel,
		                   count);

		// Fill in NULLs to the right
		for (auto col_idx = left_cols; col_idx < result.ColumnCount(); ++col_idx) {
			result.data[col_idx].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result.data[col_idx], true);
		}

		result.SetCardinality(count);
		result.Verify();

		return;
	}

	// Process RIGHT OUTER results
	while (ie_lstate.right_matches) {
		const idx_t count = ie_lstate.SelectOuterRows(ie_lstate.right_matches);
		if (!count) {
			ie_gstate.GetNextPair(context.client, ie_sink, ie_lstate);
			continue;
		}

		SliceSortedPayload(result, ie_sink.tables[1]->global_sort_state, ie_lstate.right_base, ie_lstate.true_sel,
		                   count, left_cols);

		// Fill in NULLs to the left
		for (idx_t col_idx = 0; col_idx < left_cols; ++col_idx) {
			result.data[col_idx].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result.data[col_idx], true);
		}

		result.SetCardinality(count);
		result.Verify();

		return;
	}
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalIEJoin::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
	D_ASSERT(children.size() == 2);
	if (state.recursive_cte) {
		throw NotImplementedException("IEJoins are not supported in recursive CTEs yet");
	}

	// Build the LHS
	auto lhs_pipeline = make_shared<Pipeline>(executor);
	state.SetPipelineSink(*lhs_pipeline, this);
	D_ASSERT(children[0].get());
	children[0]->BuildPipelines(executor, *lhs_pipeline, state);

	// Build the RHS
	auto rhs_pipeline = make_shared<Pipeline>(executor);
	state.SetPipelineSink(*rhs_pipeline, this);
	D_ASSERT(children[1].get());
	children[1]->BuildPipelines(executor, *rhs_pipeline, state);

	// RHS => LHS => current
	current.AddDependency(rhs_pipeline);
	rhs_pipeline->AddDependency(lhs_pipeline);

	state.AddPipeline(executor, move(lhs_pipeline));
	state.AddPipeline(executor, move(rhs_pipeline));

	// Now build both and scan
	state.SetPipelineSource(current, this);
}

} // namespace duckdb












namespace duckdb {

class IndexJoinOperatorState : public OperatorState {
public:
	IndexJoinOperatorState(Allocator &allocator, const PhysicalIndexJoin &op) : probe_executor(allocator) {
		rhs_rows.resize(STANDARD_VECTOR_SIZE);
		result_sizes.resize(STANDARD_VECTOR_SIZE);

		join_keys.Initialize(allocator, op.condition_types);
		for (auto &cond : op.conditions) {
			probe_executor.AddExpression(*cond.left);
		}
		if (!op.fetch_types.empty()) {
			rhs_chunk.Initialize(allocator, op.fetch_types);
		}
		rhs_sel.Initialize(STANDARD_VECTOR_SIZE);
	}

	bool first_fetch = true;
	idx_t lhs_idx = 0;
	idx_t rhs_idx = 0;
	idx_t result_size = 0;
	vector<idx_t> result_sizes;
	DataChunk join_keys;
	DataChunk rhs_chunk;
	SelectionVector rhs_sel;
	//! Vector of rows that mush be fetched for every LHS key
	vector<vector<row_t>> rhs_rows;
	ExpressionExecutor probe_executor;

public:
	void Finalize(PhysicalOperator *op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, &probe_executor, "probe_executor", 0);
	}
};

PhysicalIndexJoin::PhysicalIndexJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                     unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                     const vector<idx_t> &left_projection_map_p, vector<idx_t> right_projection_map_p,
                                     vector<column_t> column_ids_p, Index *index_p, bool lhs_first,
                                     idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::INDEX_JOIN, move(op.types), estimated_cardinality),
      left_projection_map(left_projection_map_p), right_projection_map(move(right_projection_map_p)), index(index_p),
      conditions(move(cond)), join_type(join_type), lhs_first(lhs_first) {
	column_ids = move(column_ids_p);
	children.push_back(move(left));
	children.push_back(move(right));
	for (auto &condition : conditions) {
		condition_types.push_back(condition.left->return_type);
	}
	//! Only add to fetch_ids columns that are not indexed
	for (auto &index_id : index->column_ids) {
		index_ids.insert(index_id);
	}
	for (idx_t column_id = 0; column_id < column_ids.size(); column_id++) {
		auto it = index_ids.find(column_ids[column_id]);
		if (it == index_ids.end()) {
			fetch_ids.push_back(column_ids[column_id]);
			fetch_types.push_back(children[1]->types[column_id]);
		}
	}
	if (right_projection_map.empty()) {
		for (column_t i = 0; i < column_ids.size(); i++) {
			right_projection_map.push_back(i);
		}
	}
	if (left_projection_map.empty()) {
		for (column_t i = 0; i < children[0]->types.size(); i++) {
			left_projection_map.push_back(i);
		}
	}
}

unique_ptr<OperatorState> PhysicalIndexJoin::GetOperatorState(ExecutionContext &context) const {
	return make_unique<IndexJoinOperatorState>(Allocator::Get(context.client), *this);
}

void PhysicalIndexJoin::Output(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                               OperatorState &state_p) const {
	auto &transaction = Transaction::GetTransaction(context.client);
	auto &phy_tbl_scan = (PhysicalTableScan &)*children[1];
	auto &bind_tbl = (TableScanBindData &)*phy_tbl_scan.bind_data;
	auto &state = (IndexJoinOperatorState &)state_p;

	auto tbl = bind_tbl.table->storage.get();
	idx_t output_sel_idx = 0;
	vector<row_t> fetch_rows;

	while (output_sel_idx < STANDARD_VECTOR_SIZE && state.lhs_idx < input.size()) {
		if (state.rhs_idx < state.result_sizes[state.lhs_idx]) {
			state.rhs_sel.set_index(output_sel_idx++, state.lhs_idx);
			if (!fetch_types.empty()) {
				//! We need to collect the rows we want to fetch
				fetch_rows.push_back(state.rhs_rows[state.lhs_idx][state.rhs_idx]);
			}
			state.rhs_idx++;
		} else {
			//! We are done with the matches from this LHS Key
			state.rhs_idx = 0;
			state.lhs_idx++;
		}
	}
	//! Now we fetch the RHS data
	if (!fetch_types.empty()) {
		if (fetch_rows.empty()) {
			return;
		}
		state.rhs_chunk.Reset();
		ColumnFetchState fetch_state;
		Vector row_ids(LogicalType::ROW_TYPE, (data_ptr_t)&fetch_rows[0]);
		tbl->Fetch(transaction, state.rhs_chunk, fetch_ids, row_ids, output_sel_idx, fetch_state);
	}

	//! Now we actually produce our result chunk
	idx_t left_offset = lhs_first ? 0 : right_projection_map.size();
	idx_t right_offset = lhs_first ? left_projection_map.size() : 0;
	idx_t rhs_column_idx = 0;
	for (idx_t i = 0; i < right_projection_map.size(); i++) {
		auto it = index_ids.find(column_ids[right_projection_map[i]]);
		if (it == index_ids.end()) {
			chunk.data[right_offset + i].Reference(state.rhs_chunk.data[rhs_column_idx++]);
		} else {
			chunk.data[right_offset + i].Slice(state.join_keys.data[0], state.rhs_sel, output_sel_idx);
		}
	}
	for (idx_t i = 0; i < left_projection_map.size(); i++) {
		chunk.data[left_offset + i].Slice(input.data[left_projection_map[i]], state.rhs_sel, output_sel_idx);
	}

	state.result_size = output_sel_idx;
	chunk.SetCardinality(state.result_size);
}

void PhysicalIndexJoin::GetRHSMatches(ExecutionContext &context, DataChunk &input, OperatorState &state_p) const {
	auto &state = (IndexJoinOperatorState &)state_p;
	auto &art = (ART &)*index;
	auto &transaction = Transaction::GetTransaction(context.client);
	for (idx_t i = 0; i < input.size(); i++) {
		auto equal_value = state.join_keys.GetValue(0, i);
		auto index_state = art.InitializeScanSinglePredicate(transaction, equal_value, ExpressionType::COMPARE_EQUAL);
		state.rhs_rows[i].clear();
		if (!equal_value.IsNull()) {
			if (fetch_types.empty()) {
				IndexLock lock;
				index->InitializeLock(lock);
				art.SearchEqualJoinNoFetch(equal_value, state.result_sizes[i]);
			} else {
				IndexLock lock;
				index->InitializeLock(lock);
				art.SearchEqual((ARTIndexScanState *)index_state.get(), (idx_t)-1, state.rhs_rows[i]);
				state.result_sizes[i] = state.rhs_rows[i].size();
			}
		} else {
			//! This is null so no matches
			state.result_sizes[i] = 0;
		}
	}
	for (idx_t i = input.size(); i < STANDARD_VECTOR_SIZE; i++) {
		//! No LHS chunk value so result size is empty
		state.result_sizes[i] = 0;
	}
}

OperatorResultType PhysicalIndexJoin::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                              GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = (IndexJoinOperatorState &)state_p;

	state.result_size = 0;
	if (state.first_fetch) {
		state.probe_executor.Execute(input, state.join_keys);

		//! Fill Matches for the current LHS chunk
		GetRHSMatches(context, input, state_p);
		state.first_fetch = false;
	}
	//! Check if we need to get a new LHS chunk
	if (state.lhs_idx >= input.size()) {
		state.lhs_idx = 0;
		state.rhs_idx = 0;
		state.first_fetch = true;
		return OperatorResultType::NEED_MORE_INPUT;
	}
	//! Output vectors
	if (state.lhs_idx < input.size()) {
		Output(context, input, chunk, state_p);
	}
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalIndexJoin::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
	// index join: we only continue into the LHS
	// the right side is probed by the index join
	// so we don't need to do anything in the pipeline with this child
	state.AddPipelineOperator(current, this);
	children[0]->BuildPipelines(executor, current, state);
}

vector<const PhysicalOperator *> PhysicalIndexJoin::GetSources() const {
	return children[0]->GetSources();
}

} // namespace duckdb



namespace duckdb {

PhysicalJoin::PhysicalJoin(LogicalOperator &op, PhysicalOperatorType type, JoinType join_type,
                           idx_t estimated_cardinality)
    : PhysicalOperator(type, op.types, estimated_cardinality), join_type(join_type) {
}

bool PhysicalJoin::EmptyResultIfRHSIsEmpty() const {
	// empty RHS with INNER, RIGHT or SEMI join means empty result set
	switch (join_type) {
	case JoinType::INNER:
	case JoinType::RIGHT:
	case JoinType::SEMI:
		return true;
	default:
		return false;
	}
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalJoin::BuildJoinPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state,
                                      PhysicalOperator &op) {
	op.op_state.reset();
	op.sink_state.reset();

	// on the LHS (probe child), the operator becomes a regular operator
	state.AddPipelineOperator(current, &op);
	if (op.IsSource()) {
		// FULL or RIGHT outer join
		// schedule a scan of the node as a child pipeline
		// this scan has to be performed AFTER all the probing has happened
		if (state.recursive_cte) {
			throw NotImplementedException("FULL and RIGHT outer joins are not supported in recursive CTEs yet");
		}
		state.AddChildPipeline(executor, current);
	}
	// continue building the pipeline on this child
	op.children[0]->BuildPipelines(executor, current, state);

	// on the RHS (build side), we construct a new child pipeline with this pipeline as its source
	op.BuildChildPipeline(executor, current, state, op.children[1].get());
}

void PhysicalJoin::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
	PhysicalJoin::BuildJoinPipelines(executor, current, state, *this);
}

vector<const PhysicalOperator *> PhysicalJoin::GetSources() const {
	auto result = children[0]->GetSources();
	if (IsSource()) {
		result.push_back(this);
	}
	return result;
}

} // namespace duckdb








namespace duckdb {

PhysicalNestedLoopJoin::PhysicalNestedLoopJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                               unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond,
                                               JoinType join_type, idx_t estimated_cardinality)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::NESTED_LOOP_JOIN, move(cond), join_type, estimated_cardinality) {
	children.push_back(move(left));
	children.push_back(move(right));
}

static bool HasNullValues(DataChunk &chunk) {
	for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
		VectorData vdata;
		chunk.data[col_idx].Orrify(chunk.size(), vdata);

		if (vdata.validity.AllValid()) {
			continue;
		}
		for (idx_t i = 0; i < chunk.size(); i++) {
			auto idx = vdata.sel->get_index(i);
			if (!vdata.validity.RowIsValid(idx)) {
				return true;
			}
		}
	}
	return false;
}

template <bool MATCH>
static void ConstructSemiOrAntiJoinResult(DataChunk &left, DataChunk &result, bool found_match[]) {
	D_ASSERT(left.ColumnCount() == result.ColumnCount());
	// create the selection vector from the matches that were found
	idx_t result_count = 0;
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < left.size(); i++) {
		if (found_match[i] == MATCH) {
			sel.set_index(result_count++, i);
		}
	}
	// construct the final result
	if (result_count > 0) {
		// we only return the columns on the left side
		// project them using the result selection vector
		// reference the columns of the left side from the result
		result.Slice(left, sel, result_count);
	} else {
		result.SetCardinality(0);
	}
}

void PhysicalJoin::ConstructSemiJoinResult(DataChunk &left, DataChunk &result, bool found_match[]) {
	ConstructSemiOrAntiJoinResult<true>(left, result, found_match);
}

void PhysicalJoin::ConstructAntiJoinResult(DataChunk &left, DataChunk &result, bool found_match[]) {
	ConstructSemiOrAntiJoinResult<false>(left, result, found_match);
}

void PhysicalJoin::ConstructMarkJoinResult(DataChunk &join_keys, DataChunk &left, DataChunk &result, bool found_match[],
                                           bool has_null) {
	// for the initial set of columns we just reference the left side
	result.SetCardinality(left);
	for (idx_t i = 0; i < left.ColumnCount(); i++) {
		result.data[i].Reference(left.data[i]);
	}
	auto &mark_vector = result.data.back();
	mark_vector.SetVectorType(VectorType::FLAT_VECTOR);
	// first we set the NULL values from the join keys
	// if there is any NULL in the keys, the result is NULL
	auto bool_result = FlatVector::GetData<bool>(mark_vector);
	auto &mask = FlatVector::Validity(mark_vector);
	for (idx_t col_idx = 0; col_idx < join_keys.ColumnCount(); col_idx++) {
		VectorData jdata;
		join_keys.data[col_idx].Orrify(join_keys.size(), jdata);
		if (!jdata.validity.AllValid()) {
			for (idx_t i = 0; i < join_keys.size(); i++) {
				auto jidx = jdata.sel->get_index(i);
				mask.Set(i, jdata.validity.RowIsValid(jidx));
			}
		}
	}
	// now set the remaining entries to either true or false based on whether a match was found
	if (found_match) {
		for (idx_t i = 0; i < left.size(); i++) {
			bool_result[i] = found_match[i];
		}
	} else {
		memset(bool_result, 0, sizeof(bool) * left.size());
	}
	// if the right side contains NULL values, the result of any FALSE becomes NULL
	if (has_null) {
		for (idx_t i = 0; i < left.size(); i++) {
			if (!bool_result[i]) {
				mask.SetInvalid(i);
			}
		}
	}
}

bool PhysicalNestedLoopJoin::IsSupported(const vector<JoinCondition> &conditions) {
	for (auto &cond : conditions) {
		if (cond.left->return_type.InternalType() == PhysicalType::STRUCT ||
		    cond.left->return_type.InternalType() == PhysicalType::LIST) {
			return false;
		}
	}
	return true;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class NestedLoopJoinLocalState : public LocalSinkState {
public:
	explicit NestedLoopJoinLocalState(Allocator &allocator, const vector<JoinCondition> &conditions)
	    : rhs_executor(allocator) {
		vector<LogicalType> condition_types;
		for (auto &cond : conditions) {
			rhs_executor.AddExpression(*cond.right);
			condition_types.push_back(cond.right->return_type);
		}
		right_condition.Initialize(allocator, condition_types);
	}

	//! The chunk holding the right condition
	DataChunk right_condition;
	//! The executor of the RHS condition
	ExpressionExecutor rhs_executor;
};

class NestedLoopJoinGlobalState : public GlobalSinkState {
public:
	explicit NestedLoopJoinGlobalState(Allocator &allocator)
	    : right_data(allocator), right_chunks(allocator), has_null(false) {
	}

	mutex nj_lock;
	//! Materialized data of the RHS
	ChunkCollection right_data;
	//! Materialized join condition of the RHS
	ChunkCollection right_chunks;
	//! Whether or not the RHS of the nested loop join has NULL values
	bool has_null;
	//! A bool indicating for each tuple in the RHS if they found a match (only used in FULL OUTER JOIN)
	unique_ptr<bool[]> right_found_match;
};

SinkResultType PhysicalNestedLoopJoin::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                            DataChunk &input) const {
	auto &gstate = (NestedLoopJoinGlobalState &)state;
	auto &nlj_state = (NestedLoopJoinLocalState &)lstate;

	// resolve the join expression of the right side
	nlj_state.right_condition.Reset();
	nlj_state.rhs_executor.Execute(input, nlj_state.right_condition);

	// if we have not seen any NULL values yet, and we are performing a MARK join, check if there are NULL values in
	// this chunk
	if (join_type == JoinType::MARK && !gstate.has_null) {
		if (HasNullValues(nlj_state.right_condition)) {
			gstate.has_null = true;
		}
	}

	// append the data and the
	lock_guard<mutex> nj_guard(gstate.nj_lock);
	gstate.right_data.Append(input);
	gstate.right_chunks.Append(nlj_state.right_condition);
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalNestedLoopJoin::Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const {
	auto &state = (NestedLoopJoinLocalState &)lstate;
	auto &client_profiler = QueryProfiler::Get(context.client);

	context.thread.profiler.Flush(this, &state.rhs_executor, "rhs_executor", 1);
	client_profiler.Flush(context.thread.profiler);
}

SinkFinalizeType PhysicalNestedLoopJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                  GlobalSinkState &gstate_p) const {
	auto &gstate = (NestedLoopJoinGlobalState &)gstate_p;
	if (join_type == JoinType::OUTER || join_type == JoinType::RIGHT) {
		// for FULL/RIGHT OUTER JOIN, initialize found_match to false for every tuple
		gstate.right_found_match = unique_ptr<bool[]>(new bool[gstate.right_data.Count()]);
		memset(gstate.right_found_match.get(), 0, sizeof(bool) * gstate.right_data.Count());
	}
	if (gstate.right_chunks.Count() == 0 && EmptyResultIfRHSIsEmpty()) {
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSinkState> PhysicalNestedLoopJoin::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<NestedLoopJoinGlobalState>(Allocator::Get(context));
}

unique_ptr<LocalSinkState> PhysicalNestedLoopJoin::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<NestedLoopJoinLocalState>(Allocator::Get(context.client), conditions);
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class PhysicalNestedLoopJoinState : public OperatorState {
public:
	PhysicalNestedLoopJoinState(Allocator &allocator, const PhysicalNestedLoopJoin &op,
	                            const vector<JoinCondition> &conditions)
	    : fetch_next_left(true), fetch_next_right(false), right_chunk(0), lhs_executor(allocator), left_tuple(0),
	      right_tuple(0) {
		vector<LogicalType> condition_types;
		for (auto &cond : conditions) {
			lhs_executor.AddExpression(*cond.left);
			condition_types.push_back(cond.left->return_type);
		}
		left_condition.Initialize(allocator, condition_types);
		if (IsLeftOuterJoin(op.join_type)) {
			left_found_match = unique_ptr<bool[]>(new bool[STANDARD_VECTOR_SIZE]);
			memset(left_found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
		}
	}

	bool fetch_next_left;
	bool fetch_next_right;
	idx_t right_chunk;
	DataChunk left_condition;
	//! The executor of the LHS condition
	ExpressionExecutor lhs_executor;

	idx_t left_tuple;
	idx_t right_tuple;

	unique_ptr<bool[]> left_found_match;

public:
	void Finalize(PhysicalOperator *op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, &lhs_executor, "lhs_executor", 0);
	}
};

unique_ptr<OperatorState> PhysicalNestedLoopJoin::GetOperatorState(ExecutionContext &context) const {
	return make_unique<PhysicalNestedLoopJoinState>(Allocator::Get(context.client), *this, conditions);
}

OperatorResultType PhysicalNestedLoopJoin::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                   GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &gstate = (NestedLoopJoinGlobalState &)*sink_state;

	if (gstate.right_chunks.Count() == 0) {
		// empty RHS
		if (!EmptyResultIfRHSIsEmpty()) {
			ConstructEmptyJoinResult(join_type, gstate.has_null, input, chunk);
			return OperatorResultType::NEED_MORE_INPUT;
		} else {
			return OperatorResultType::FINISHED;
		}
	}

	switch (join_type) {
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::MARK:
		// simple joins can have max STANDARD_VECTOR_SIZE matches per chunk
		ResolveSimpleJoin(context, input, chunk, state_p);
		return OperatorResultType::NEED_MORE_INPUT;
	case JoinType::LEFT:
	case JoinType::INNER:
	case JoinType::OUTER:
	case JoinType::RIGHT:
		return ResolveComplexJoin(context, input, chunk, state_p);
	default:
		throw NotImplementedException("Unimplemented type for nested loop join!");
	}
}

void PhysicalNestedLoopJoin::ResolveSimpleJoin(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                               OperatorState &state_p) const {
	auto &state = (PhysicalNestedLoopJoinState &)state_p;
	auto &gstate = (NestedLoopJoinGlobalState &)*sink_state;

	// resolve the left join condition for the current chunk
	state.lhs_executor.Execute(input, state.left_condition);

	bool found_match[STANDARD_VECTOR_SIZE] = {false};
	NestedLoopJoinMark::Perform(state.left_condition, gstate.right_chunks, found_match, conditions);
	switch (join_type) {
	case JoinType::MARK:
		// now construct the mark join result from the found matches
		PhysicalJoin::ConstructMarkJoinResult(state.left_condition, input, chunk, found_match, gstate.has_null);
		break;
	case JoinType::SEMI:
		// construct the semi join result from the found matches
		PhysicalJoin::ConstructSemiJoinResult(input, chunk, found_match);
		break;
	case JoinType::ANTI:
		// construct the anti join result from the found matches
		PhysicalJoin::ConstructAntiJoinResult(input, chunk, found_match);
		break;
	default:
		throw NotImplementedException("Unimplemented type for simple nested loop join!");
	}
}

void PhysicalJoin::ConstructLeftJoinResult(DataChunk &left, DataChunk &result, bool found_match[]) {
	SelectionVector remaining_sel(STANDARD_VECTOR_SIZE);
	idx_t remaining_count = 0;
	for (idx_t i = 0; i < left.size(); i++) {
		if (!found_match[i]) {
			remaining_sel.set_index(remaining_count++, i);
		}
	}
	if (remaining_count > 0) {
		result.Slice(left, remaining_sel, remaining_count);
		for (idx_t idx = left.ColumnCount(); idx < result.ColumnCount(); idx++) {
			result.data[idx].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result.data[idx], true);
		}
	}
}

OperatorResultType PhysicalNestedLoopJoin::ResolveComplexJoin(ExecutionContext &context, DataChunk &input,
                                                              DataChunk &chunk, OperatorState &state_p) const {
	auto &state = (PhysicalNestedLoopJoinState &)state_p;
	auto &gstate = (NestedLoopJoinGlobalState &)*sink_state;

	idx_t match_count;
	do {
		if (state.fetch_next_right) {
			// we exhausted the chunk on the right: move to the next chunk on the right
			state.right_chunk++;
			state.left_tuple = 0;
			state.right_tuple = 0;
			state.fetch_next_right = false;
			// check if we exhausted all chunks on the RHS
			if (state.right_chunk >= gstate.right_chunks.ChunkCount()) {
				state.fetch_next_left = true;
				// we exhausted all chunks on the right: move to the next chunk on the left
				if (IsLeftOuterJoin(join_type)) {
					// left join: before we move to the next chunk, see if we need to output any vectors that didn't
					// have a match found
					PhysicalJoin::ConstructLeftJoinResult(input, chunk, state.left_found_match.get());
					memset(state.left_found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
				}
				return OperatorResultType::NEED_MORE_INPUT;
			}
		}
		if (state.fetch_next_left) {
			// resolve the left join condition for the current chunk
			state.left_condition.Reset();
			state.lhs_executor.Execute(input, state.left_condition);

			state.left_tuple = 0;
			state.right_tuple = 0;
			state.right_chunk = 0;
			state.fetch_next_left = false;
		}
		// now we have a left and a right chunk that we can join together
		// note that we only get here in the case of a LEFT, INNER or FULL join
		auto &left_chunk = input;
		auto &right_chunk = gstate.right_chunks.GetChunk(state.right_chunk);
		auto &right_data = gstate.right_data.GetChunk(state.right_chunk);

		// sanity check
		left_chunk.Verify();
		right_chunk.Verify();
		right_data.Verify();

		// now perform the join
		SelectionVector lvector(STANDARD_VECTOR_SIZE), rvector(STANDARD_VECTOR_SIZE);
		match_count = NestedLoopJoinInner::Perform(state.left_tuple, state.right_tuple, state.left_condition,
		                                           right_chunk, lvector, rvector, conditions);
		// we have finished resolving the join conditions
		if (match_count > 0) {
			// we have matching tuples!
			// construct the result
			if (state.left_found_match) {
				for (idx_t i = 0; i < match_count; i++) {
					state.left_found_match[lvector.get_index(i)] = true;
				}
			}
			if (gstate.right_found_match) {
				for (idx_t i = 0; i < match_count; i++) {
					gstate.right_found_match[state.right_chunk * STANDARD_VECTOR_SIZE + rvector.get_index(i)] = true;
				}
			}
			chunk.Slice(input, lvector, match_count);
			chunk.Slice(right_data, rvector, match_count, input.ColumnCount());
		}

		// check if we exhausted the RHS, if we did we need to move to the next right chunk in the next iteration
		if (state.right_tuple >= right_chunk.size()) {
			state.fetch_next_right = true;
		}
	} while (match_count == 0);
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class NestedLoopJoinScanState : public GlobalSourceState {
public:
	explicit NestedLoopJoinScanState(const PhysicalNestedLoopJoin &op) : op(op), right_outer_position(0) {
	}

	mutex lock;
	const PhysicalNestedLoopJoin &op;
	idx_t right_outer_position;

public:
	idx_t MaxThreads() override {
		auto &sink = (NestedLoopJoinGlobalState &)*op.sink_state;
		return sink.right_chunks.Count() / (STANDARD_VECTOR_SIZE * 10);
	}
};

unique_ptr<GlobalSourceState> PhysicalNestedLoopJoin::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<NestedLoopJoinScanState>(*this);
}

void PhysicalNestedLoopJoin::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                     LocalSourceState &lstate) const {
	D_ASSERT(IsRightOuterJoin(join_type));
	// check if we need to scan any unmatched tuples from the RHS for the full/right outer join
	auto &sink = (NestedLoopJoinGlobalState &)*sink_state;
	auto &state = (NestedLoopJoinScanState &)gstate;

	// if the LHS is exhausted in a FULL/RIGHT OUTER JOIN, we scan the found_match for any chunks we
	// still need to output
	lock_guard<mutex> l(state.lock);
	ConstructFullOuterJoinResult(sink.right_found_match.get(), sink.right_data, chunk, state.right_outer_position);
}

} // namespace duckdb













namespace duckdb {

PhysicalPiecewiseMergeJoin::PhysicalPiecewiseMergeJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                                       unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond,
                                                       JoinType join_type, idx_t estimated_cardinality)
    : PhysicalRangeJoin(op, PhysicalOperatorType::PIECEWISE_MERGE_JOIN, move(left), move(right), move(cond), join_type,
                        estimated_cardinality) {

	for (auto &cond : conditions) {
		D_ASSERT(cond.left->return_type == cond.right->return_type);
		join_key_types.push_back(cond.left->return_type);

		// Convert the conditions to sort orders
		auto left = cond.left->Copy();
		auto right = cond.right->Copy();
		switch (cond.comparison) {
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			lhs_orders.emplace_back(BoundOrderByNode(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, move(left)));
			rhs_orders.emplace_back(BoundOrderByNode(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, move(right)));
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			lhs_orders.emplace_back(BoundOrderByNode(OrderType::DESCENDING, OrderByNullType::NULLS_LAST, move(left)));
			rhs_orders.emplace_back(BoundOrderByNode(OrderType::DESCENDING, OrderByNullType::NULLS_LAST, move(right)));
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
		case ExpressionType::COMPARE_DISTINCT_FROM:
			// Allowed in multi-predicate joins, but can't be first/sort.
			D_ASSERT(!lhs_orders.empty());
			lhs_orders.emplace_back(BoundOrderByNode(OrderType::INVALID, OrderByNullType::NULLS_LAST, move(left)));
			rhs_orders.emplace_back(BoundOrderByNode(OrderType::INVALID, OrderByNullType::NULLS_LAST, move(right)));
			break;

		default:
			// COMPARE EQUAL not supported with merge join
			throw NotImplementedException("Unimplemented join type for merge join");
		}
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class MergeJoinLocalState : public LocalSinkState {
public:
	explicit MergeJoinLocalState(Allocator &allocator, const PhysicalRangeJoin &op, const idx_t child)
	    : table(allocator, op, child) {
	}

	//! The local sort state
	PhysicalRangeJoin::LocalSortedTable table;
};

class MergeJoinGlobalState : public GlobalSinkState {
public:
	using GlobalSortedTable = PhysicalRangeJoin::GlobalSortedTable;

public:
	MergeJoinGlobalState(ClientContext &context, const PhysicalPiecewiseMergeJoin &op) {
		RowLayout rhs_layout;
		rhs_layout.Initialize(op.children[1]->types);
		vector<BoundOrderByNode> rhs_order;
		rhs_order.emplace_back(op.rhs_orders[0].Copy());
		table = make_unique<GlobalSortedTable>(context, rhs_order, rhs_layout);
	}

	inline idx_t Count() const {
		return table->count;
	}

	void Sink(DataChunk &input, MergeJoinLocalState &lstate) {
		auto &global_sort_state = table->global_sort_state;
		auto &local_sort_state = lstate.table.local_sort_state;

		// Sink the data into the local sort state
		lstate.table.Sink(input, global_sort_state);

		// When sorting data reaches a certain size, we sort it
		if (local_sort_state.SizeInBytes() >= table->memory_per_thread) {
			local_sort_state.Sort(global_sort_state, true);
		}
	}

	unique_ptr<GlobalSortedTable> table;
};

unique_ptr<GlobalSinkState> PhysicalPiecewiseMergeJoin::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<MergeJoinGlobalState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalPiecewiseMergeJoin::GetLocalSinkState(ExecutionContext &context) const {
	// We only sink the RHS
	return make_unique<MergeJoinLocalState>(Allocator::Get(context.client), *this, 1);
}

SinkResultType PhysicalPiecewiseMergeJoin::Sink(ExecutionContext &context, GlobalSinkState &gstate_p,
                                                LocalSinkState &lstate_p, DataChunk &input) const {
	auto &gstate = (MergeJoinGlobalState &)gstate_p;
	auto &lstate = (MergeJoinLocalState &)lstate_p;

	gstate.Sink(input, lstate);

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalPiecewiseMergeJoin::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                         LocalSinkState &lstate_p) const {
	auto &gstate = (MergeJoinGlobalState &)gstate_p;
	auto &lstate = (MergeJoinLocalState &)lstate_p;
	gstate.table->Combine(lstate.table);
	auto &client_profiler = QueryProfiler::Get(context.client);

	context.thread.profiler.Flush(this, &lstate.table.executor, "rhs_executor", 1);
	client_profiler.Flush(context.thread.profiler);
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PhysicalPiecewiseMergeJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                      GlobalSinkState &gstate_p) const {
	auto &gstate = (MergeJoinGlobalState &)gstate_p;
	auto &global_sort_state = gstate.table->global_sort_state;

	if (IsRightOuterJoin(join_type)) {
		// for FULL/RIGHT OUTER JOIN, initialize found_match to false for every tuple
		gstate.table->IntializeMatches();
	}
	if (global_sort_state.sorted_blocks.empty() && EmptyResultIfRHSIsEmpty()) {
		// Empty input!
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Sort the current input child
	gstate.table->Finalize(pipeline, event);

	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class PiecewiseMergeJoinState : public OperatorState {
public:
	using LocalSortedTable = PhysicalRangeJoin::LocalSortedTable;

	explicit PiecewiseMergeJoinState(Allocator &allocator, const PhysicalPiecewiseMergeJoin &op,
	                                 BufferManager &buffer_manager, bool force_external)
	    : allocator(allocator), op(op), buffer_manager(buffer_manager), force_external(force_external),
	      left_position(0), first_fetch(true), finished(true), right_position(0), right_chunk_index(0),
	      rhs_executor(allocator) {
		vector<LogicalType> condition_types;
		for (auto &order : op.lhs_orders) {
			condition_types.push_back(order.expression->return_type);
		}
		if (IsLeftOuterJoin(op.join_type)) {
			lhs_found_match = unique_ptr<bool[]>(new bool[STANDARD_VECTOR_SIZE]);
			memset(lhs_found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
		}
		lhs_layout.Initialize(op.children[0]->types);
		lhs_payload.Initialize(allocator, op.children[0]->types);

		lhs_order.emplace_back(op.lhs_orders[0].Copy());

		// Set up shared data for multiple predicates
		sel.Initialize(STANDARD_VECTOR_SIZE);
		condition_types.clear();
		for (auto &order : op.rhs_orders) {
			rhs_executor.AddExpression(*order.expression);
			condition_types.push_back(order.expression->return_type);
		}
		rhs_keys.Initialize(allocator, condition_types);
	}

	Allocator &allocator;
	const PhysicalPiecewiseMergeJoin &op;
	BufferManager &buffer_manager;
	bool force_external;

	// Block sorting
	DataChunk lhs_payload;
	unique_ptr<bool[]> lhs_found_match;
	vector<BoundOrderByNode> lhs_order;
	RowLayout lhs_layout;
	unique_ptr<LocalSortedTable> lhs_local_table;
	unique_ptr<GlobalSortState> lhs_global_state;

	// Simple scans
	idx_t left_position;

	// Complex scans
	bool first_fetch;
	bool finished;
	idx_t right_position;
	idx_t right_chunk_index;
	idx_t right_base;

	// Secondary predicate shared data
	SelectionVector sel;
	DataChunk rhs_keys;
	DataChunk rhs_input;
	ExpressionExecutor rhs_executor;

public:
	void ResolveJoinKeys(DataChunk &input) {
		// sort by join key
		lhs_global_state = make_unique<GlobalSortState>(buffer_manager, lhs_order, lhs_layout);
		lhs_local_table = make_unique<LocalSortedTable>(allocator, op, 0);
		lhs_local_table->Sink(input, *lhs_global_state);

		// Set external (can be forced with the PRAGMA)
		lhs_global_state->external = force_external;
		lhs_global_state->AddLocalState(lhs_local_table->local_sort_state);
		lhs_global_state->PrepareMergePhase();
		while (lhs_global_state->sorted_blocks.size() > 1) {
			MergeSorter merge_sorter(*lhs_global_state, buffer_manager);
			merge_sorter.PerformInMergeRound();
			lhs_global_state->CompleteMergeRound();
		}

		// Scan the sorted payload
		D_ASSERT(lhs_global_state->sorted_blocks.size() == 1);

		PayloadScanner scanner(*lhs_global_state->sorted_blocks[0]->payload_data, *lhs_global_state);
		lhs_payload.Reset();
		scanner.Scan(lhs_payload);

		// Recompute the sorted keys from the sorted input
		lhs_local_table->keys.Reset();
		lhs_local_table->executor.Execute(lhs_payload, lhs_local_table->keys);
	}

	void Finalize(PhysicalOperator *op, ExecutionContext &context) override {
		if (lhs_local_table) {
			context.thread.profiler.Flush(op, &lhs_local_table->executor, "lhs_executor", 0);
		}
	}
};

unique_ptr<OperatorState> PhysicalPiecewiseMergeJoin::GetOperatorState(ExecutionContext &context) const {
	auto &buffer_manager = BufferManager::GetBufferManager(context.client);
	auto &config = ClientConfig::GetConfig(context.client);
	return make_unique<PiecewiseMergeJoinState>(Allocator::Get(context.client), *this, buffer_manager,
	                                            config.force_external);
}

static inline idx_t SortedBlockNotNull(const idx_t base, const idx_t count, const idx_t not_null) {
	return MinValue(base + count, MaxValue(base, not_null)) - base;
}

static int MergeJoinComparisonValue(ExpressionType comparison) {
	switch (comparison) {
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
		return -1;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return 0;
	default:
		throw InternalException("Unimplemented comparison type for merge join!");
	}
}

struct BlockMergeInfo {
	GlobalSortState &state;
	//! The block being scanned
	const idx_t block_idx;
	//! The number of not-NULL values in the block (they are at the end)
	const idx_t not_null;
	//! The current offset in the block
	idx_t &entry_idx;
	SelectionVector result;

	BlockMergeInfo(GlobalSortState &state, idx_t block_idx, idx_t &entry_idx, idx_t not_null)
	    : state(state), block_idx(block_idx), not_null(not_null), entry_idx(entry_idx), result(STANDARD_VECTOR_SIZE) {
	}
};

static void MergeJoinPinSortingBlock(SBScanState &scan, const idx_t block_idx) {
	scan.SetIndices(block_idx, 0);
	scan.PinRadix(block_idx);

	auto &sd = *scan.sb->blob_sorting_data;
	if (block_idx < sd.data_blocks.size()) {
		scan.PinData(sd);
	}
}

static data_ptr_t MergeJoinRadixPtr(SBScanState &scan, const idx_t entry_idx) {
	scan.entry_idx = entry_idx;
	return scan.RadixPtr();
}

static idx_t MergeJoinSimpleBlocks(PiecewiseMergeJoinState &lstate, MergeJoinGlobalState &rstate, bool *found_match,
                                   const ExpressionType comparison) {
	const auto cmp = MergeJoinComparisonValue(comparison);

	// The sort parameters should all be the same
	auto &lsort = *lstate.lhs_global_state;
	auto &rsort = rstate.table->global_sort_state;
	D_ASSERT(lsort.sort_layout.all_constant == rsort.sort_layout.all_constant);
	const auto all_constant = lsort.sort_layout.all_constant;
	D_ASSERT(lsort.external == rsort.external);
	const auto external = lsort.external;

	// There should only be one sorted block if they have been sorted
	D_ASSERT(lsort.sorted_blocks.size() == 1);
	SBScanState lread(lsort.buffer_manager, lsort);
	lread.sb = lsort.sorted_blocks[0].get();

	const idx_t l_block_idx = 0;
	idx_t l_entry_idx = 0;
	const auto lhs_not_null = lstate.lhs_local_table->count - lstate.lhs_local_table->has_null;
	MergeJoinPinSortingBlock(lread, l_block_idx);
	auto l_ptr = MergeJoinRadixPtr(lread, l_entry_idx);

	D_ASSERT(rsort.sorted_blocks.size() == 1);
	SBScanState rread(rsort.buffer_manager, rsort);
	rread.sb = rsort.sorted_blocks[0].get();

	const auto cmp_size = lsort.sort_layout.comparison_size;
	const auto entry_size = lsort.sort_layout.entry_size;

	idx_t right_base = 0;
	for (idx_t r_block_idx = 0; r_block_idx < rread.sb->radix_sorting_data.size(); r_block_idx++) {
		// we only care about the BIGGEST value in each of the RHS data blocks
		// because we want to figure out if the LHS values are less than [or equal] to ANY value
		// get the biggest value from the RHS chunk
		MergeJoinPinSortingBlock(rread, r_block_idx);

		auto &rblock = rread.sb->radix_sorting_data[r_block_idx];
		const auto r_not_null =
		    SortedBlockNotNull(right_base, rblock.count, rstate.table->count - rstate.table->has_null);
		if (r_not_null == 0) {
			break;
		}
		const auto r_entry_idx = r_not_null - 1;
		right_base += rblock.count;

		auto r_ptr = MergeJoinRadixPtr(rread, r_entry_idx);

		// now we start from the current lpos value and check if we found a new value that is [<= OR <] the max RHS
		// value
		while (true) {
			int comp_res;
			if (all_constant) {
				comp_res = FastMemcmp(l_ptr, r_ptr, cmp_size);
			} else {
				lread.entry_idx = l_entry_idx;
				rread.entry_idx = r_entry_idx;
				comp_res = Comparators::CompareTuple(lread, rread, l_ptr, r_ptr, lsort.sort_layout, external);
			}

			if (comp_res <= cmp) {
				// found a match for lpos, set it in the found_match vector
				found_match[l_entry_idx] = true;
				l_entry_idx++;
				l_ptr += entry_size;
				if (l_entry_idx >= lhs_not_null) {
					// early out: we exhausted the entire LHS and they all match
					return 0;
				}
			} else {
				// we found no match: any subsequent value from the LHS we scan now will be bigger and thus also not
				// match move to the next RHS chunk
				break;
			}
		}
	}
	return 0;
}

void PhysicalPiecewiseMergeJoin::ResolveSimpleJoin(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                   OperatorState &state_p) const {
	auto &state = (PiecewiseMergeJoinState &)state_p;
	auto &gstate = (MergeJoinGlobalState &)*sink_state;

	state.ResolveJoinKeys(input);
	auto &lhs_table = *state.lhs_local_table;

	// perform the actual join
	bool found_match[STANDARD_VECTOR_SIZE];
	memset(found_match, 0, sizeof(found_match));
	MergeJoinSimpleBlocks(state, gstate, found_match, conditions[0].comparison);

	// use the sorted payload
	const auto lhs_not_null = lhs_table.count - lhs_table.has_null;
	auto &payload = state.lhs_payload;

	// now construct the result based on the join result
	switch (join_type) {
	case JoinType::MARK: {
		// The only part of the join keys that is actually used is the validity mask.
		// Since the payload is sorted, we can just set the tail end of the validity masks to invalid.
		for (auto &key : lhs_table.keys.data) {
			key.Normalify(lhs_table.keys.size());
			auto &mask = FlatVector::Validity(key);
			if (mask.AllValid()) {
				continue;
			}
			mask.SetAllValid(lhs_not_null);
			for (idx_t i = lhs_not_null; i < lhs_table.count; ++i) {
				mask.SetInvalid(i);
			}
		}
		// So we make a set of keys that have the validity mask set for the
		PhysicalJoin::ConstructMarkJoinResult(lhs_table.keys, payload, chunk, found_match, gstate.table->has_null);
		break;
	}
	case JoinType::SEMI:
		PhysicalJoin::ConstructSemiJoinResult(payload, chunk, found_match);
		break;
	case JoinType::ANTI:
		PhysicalJoin::ConstructAntiJoinResult(payload, chunk, found_match);
		break;
	default:
		throw NotImplementedException("Unimplemented join type for merge join");
	}
}

static idx_t MergeJoinComplexBlocks(BlockMergeInfo &l, BlockMergeInfo &r, const ExpressionType comparison) {
	const auto cmp = MergeJoinComparisonValue(comparison);

	// The sort parameters should all be the same
	D_ASSERT(l.state.sort_layout.all_constant == r.state.sort_layout.all_constant);
	const auto all_constant = r.state.sort_layout.all_constant;
	D_ASSERT(l.state.external == r.state.external);
	const auto external = l.state.external;

	// There should only be one sorted block if they have been sorted
	D_ASSERT(l.state.sorted_blocks.size() == 1);
	SBScanState lread(l.state.buffer_manager, l.state);
	lread.sb = l.state.sorted_blocks[0].get();
	D_ASSERT(lread.sb->radix_sorting_data.size() == 1);
	MergeJoinPinSortingBlock(lread, l.block_idx);
	auto l_start = MergeJoinRadixPtr(lread, 0);
	auto l_ptr = MergeJoinRadixPtr(lread, l.entry_idx);

	D_ASSERT(r.state.sorted_blocks.size() == 1);
	SBScanState rread(r.state.buffer_manager, r.state);
	rread.sb = r.state.sorted_blocks[0].get();

	if (r.entry_idx >= r.not_null) {
		return 0;
	}

	MergeJoinPinSortingBlock(rread, r.block_idx);
	auto r_ptr = MergeJoinRadixPtr(rread, r.entry_idx);

	const auto cmp_size = l.state.sort_layout.comparison_size;
	const auto entry_size = l.state.sort_layout.entry_size;

	idx_t result_count = 0;
	while (true) {
		if (l.entry_idx < l.not_null) {
			int comp_res;
			if (all_constant) {
				comp_res = FastMemcmp(l_ptr, r_ptr, cmp_size);
			} else {
				lread.entry_idx = l.entry_idx;
				rread.entry_idx = r.entry_idx;
				comp_res = Comparators::CompareTuple(lread, rread, l_ptr, r_ptr, l.state.sort_layout, external);
			}

			if (comp_res <= cmp) {
				// left side smaller: found match
				l.result.set_index(result_count, sel_t(l.entry_idx));
				r.result.set_index(result_count, sel_t(r.entry_idx));
				result_count++;
				// move left side forward
				l.entry_idx++;
				l_ptr += entry_size;
				if (result_count == STANDARD_VECTOR_SIZE) {
					// out of space!
					break;
				}
				continue;
			}
		}
		// right side smaller or equal, or left side exhausted: move
		// right pointer forward reset left side to start
		r.entry_idx++;
		if (r.entry_idx >= r.not_null) {
			break;
		}
		r_ptr += entry_size;

		l_ptr = l_start;
		l.entry_idx = 0;
	}

	return result_count;
}

OperatorResultType PhysicalPiecewiseMergeJoin::ResolveComplexJoin(ExecutionContext &context, DataChunk &input,
                                                                  DataChunk &chunk, OperatorState &state_p) const {
	auto &state = (PiecewiseMergeJoinState &)state_p;
	auto &gstate = (MergeJoinGlobalState &)*sink_state;
	auto &rsorted = *gstate.table->global_sort_state.sorted_blocks[0];
	const auto left_cols = input.ColumnCount();
	const auto tail_cols = conditions.size() - 1;
	do {
		if (state.first_fetch) {
			state.ResolveJoinKeys(input);

			state.right_chunk_index = 0;
			state.right_base = 0;
			state.left_position = 0;
			state.right_position = 0;
			state.first_fetch = false;
			state.finished = false;
		}
		if (state.finished) {
			if (IsLeftOuterJoin(join_type)) {
				// left join: before we move to the next chunk, see if we need to output any vectors that didn't
				// have a match found
				PhysicalJoin::ConstructLeftJoinResult(state.lhs_payload, chunk, state.lhs_found_match.get());
				memset(state.lhs_found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
			}
			state.first_fetch = true;
			state.finished = false;
			return OperatorResultType::NEED_MORE_INPUT;
		}

		auto &lhs_table = *state.lhs_local_table;
		const auto lhs_not_null = lhs_table.count - lhs_table.has_null;
		BlockMergeInfo left_info(*state.lhs_global_state, 0, state.left_position, lhs_not_null);

		const auto &rblock = rsorted.radix_sorting_data[state.right_chunk_index];
		const auto rhs_not_null =
		    SortedBlockNotNull(state.right_base, rblock.count, gstate.table->count - gstate.table->has_null);
		BlockMergeInfo right_info(gstate.table->global_sort_state, state.right_chunk_index, state.right_position,
		                          rhs_not_null);

		idx_t result_count = MergeJoinComplexBlocks(left_info, right_info, conditions[0].comparison);
		if (result_count == 0) {
			// exhausted this chunk on the right side
			// move to the next right chunk
			state.left_position = 0;
			state.right_position = 0;
			state.right_base += rsorted.radix_sorting_data[state.right_chunk_index].count;
			state.right_chunk_index++;
			if (state.right_chunk_index >= rsorted.radix_sorting_data.size()) {
				state.finished = true;
			}
		} else {
			// found matches: extract them
			chunk.Reset();
			for (idx_t c = 0; c < state.lhs_payload.ColumnCount(); ++c) {
				chunk.data[c].Slice(state.lhs_payload.data[c], left_info.result, result_count);
			}
			SliceSortedPayload(chunk, right_info.state, right_info.block_idx, right_info.result, result_count,
			                   left_cols);
			chunk.SetCardinality(result_count);

			auto sel = FlatVector::IncrementalSelectionVector();
			if (tail_cols) {
				// If there are more expressions to compute,
				// split the result chunk into the left and right halves
				// so we can compute the values for comparison.
				chunk.Split(state.rhs_input, left_cols);
				state.rhs_executor.SetChunk(state.rhs_input);
				state.rhs_keys.Reset();

				auto tail_count = result_count;
				for (size_t cmp_idx = 1; cmp_idx < conditions.size(); ++cmp_idx) {
					Vector left(lhs_table.keys.data[cmp_idx]);
					left.Slice(left_info.result, result_count);

					auto &right = state.rhs_keys.data[cmp_idx];
					state.rhs_executor.ExecuteExpression(cmp_idx, right);

					if (tail_count < result_count) {
						left.Slice(*sel, tail_count);
						right.Slice(*sel, tail_count);
					}
					tail_count =
					    SelectJoinTail(conditions[cmp_idx].comparison, left, right, sel, tail_count, &state.sel);
					sel = &state.sel;
				}
				chunk.Fuse(state.rhs_input);

				if (tail_count < result_count) {
					result_count = tail_count;
					chunk.Slice(*sel, result_count);
				}
			}

			// found matches: mark the found matches if required
			if (state.lhs_found_match) {
				for (idx_t i = 0; i < result_count; i++) {
					state.lhs_found_match[left_info.result[sel->get_index(i)]] = true;
				}
			}
			if (gstate.table->found_match) {
				//	Absolute position of the block + start position inside that block
				for (idx_t i = 0; i < result_count; i++) {
					gstate.table->found_match[state.right_base + right_info.result[sel->get_index(i)]] = true;
				}
			}
			chunk.SetCardinality(result_count);
			chunk.Verify();
		}
	} while (chunk.size() == 0);
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

OperatorResultType PhysicalPiecewiseMergeJoin::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                       GlobalOperatorState &gstate_p, OperatorState &state) const {
	auto &gstate = (MergeJoinGlobalState &)*sink_state;

	if (gstate.Count() == 0) {
		// empty RHS
		if (!EmptyResultIfRHSIsEmpty()) {
			ConstructEmptyJoinResult(join_type, gstate.table->has_null, input, chunk);
			return OperatorResultType::NEED_MORE_INPUT;
		} else {
			return OperatorResultType::FINISHED;
		}
	}

	input.Verify();
	switch (join_type) {
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::MARK:
		// simple joins can have max STANDARD_VECTOR_SIZE matches per chunk
		ResolveSimpleJoin(context, input, chunk, state);
		return OperatorResultType::NEED_MORE_INPUT;
	case JoinType::LEFT:
	case JoinType::INNER:
	case JoinType::RIGHT:
	case JoinType::OUTER:
		return ResolveComplexJoin(context, input, chunk, state);
	default:
		throw NotImplementedException("Unimplemented type for piecewise merge loop join!");
	}
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PiecewiseJoinScanState : public GlobalSourceState {
public:
	explicit PiecewiseJoinScanState(const PhysicalPiecewiseMergeJoin &op) : op(op), right_outer_position(0) {
	}

	mutex lock;
	const PhysicalPiecewiseMergeJoin &op;
	unique_ptr<PayloadScanner> scanner;
	idx_t right_outer_position;

public:
	idx_t MaxThreads() override {
		auto &sink = (MergeJoinGlobalState &)*op.sink_state;
		return sink.Count() / (STANDARD_VECTOR_SIZE * idx_t(10));
	}
};

unique_ptr<GlobalSourceState> PhysicalPiecewiseMergeJoin::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<PiecewiseJoinScanState>(*this);
}

void PhysicalPiecewiseMergeJoin::GetData(ExecutionContext &context, DataChunk &result, GlobalSourceState &gstate,
                                         LocalSourceState &lstate) const {
	D_ASSERT(IsRightOuterJoin(join_type));
	// check if we need to scan any unmatched tuples from the RHS for the full/right outer join
	auto &sink = (MergeJoinGlobalState &)*sink_state;
	auto &state = (PiecewiseJoinScanState &)gstate;

	lock_guard<mutex> l(state.lock);
	if (!state.scanner) {
		// Initialize scanner (if not yet initialized)
		auto &sort_state = sink.table->global_sort_state;
		if (sort_state.sorted_blocks.empty()) {
			return;
		}
		state.scanner = make_unique<PayloadScanner>(*sort_state.sorted_blocks[0]->payload_data, sort_state);
	}

	// if the LHS is exhausted in a FULL/RIGHT OUTER JOIN, we scan the found_match for any chunks we
	// still need to output
	const auto found_match = sink.table->found_match.get();

	// ConstructFullOuterJoinResult(sink.table->found_match.get(), sink.right_chunks, chunk,
	// state.right_outer_position);
	DataChunk rhs_chunk;
	rhs_chunk.Initialize(Allocator::Get(context.client), sink.table->global_sort_state.payload_layout.GetTypes());
	SelectionVector rsel(STANDARD_VECTOR_SIZE);
	for (;;) {
		// Read the next sorted chunk
		state.scanner->Scan(rhs_chunk);

		const auto count = rhs_chunk.size();
		if (count == 0) {
			return;
		}

		idx_t result_count = 0;
		// figure out which tuples didn't find a match in the RHS
		for (idx_t i = 0; i < count; i++) {
			if (!found_match[state.right_outer_position + i]) {
				rsel.set_index(result_count++, i);
			}
		}
		state.right_outer_position += count;

		if (result_count > 0) {
			// if there were any tuples that didn't find a match, output them
			const idx_t left_column_count = children[0]->types.size();
			for (idx_t col_idx = 0; col_idx < left_column_count; ++col_idx) {
				result.data[col_idx].SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(result.data[col_idx], true);
			}
			const idx_t right_column_count = children[1]->types.size();
			;
			for (idx_t col_idx = 0; col_idx < right_column_count; ++col_idx) {
				result.data[left_column_count + col_idx].Slice(rhs_chunk.data[col_idx], rsel, result_count);
			}
			result.SetCardinality(result_count);
			return;
		}
	}
}

} // namespace duckdb













#include <thread>

namespace duckdb {

PhysicalRangeJoin::LocalSortedTable::LocalSortedTable(Allocator &allocator, const PhysicalRangeJoin &op,
                                                      const idx_t child)
    : op(op), executor(allocator), has_null(0), count(0) {
	// Initialize order clause expression executor and key DataChunk
	vector<LogicalType> types;
	for (const auto &cond : op.conditions) {
		const auto &expr = child ? cond.right : cond.left;
		executor.AddExpression(*expr);

		types.push_back(expr->return_type);
	}
	keys.Initialize(allocator, types);
}

void PhysicalRangeJoin::LocalSortedTable::Sink(DataChunk &input, GlobalSortState &global_sort_state) {
	// Initialize local state (if necessary)
	if (!local_sort_state.initialized) {
		local_sort_state.Initialize(global_sort_state, global_sort_state.buffer_manager);
	}

	// Obtain sorting columns
	keys.Reset();
	executor.Execute(input, keys);

	// Count the NULLs so we can exclude them later
	has_null += MergeNulls(op.conditions);
	count += keys.size();

	//	Only sort the primary key
	DataChunk join_head;
	join_head.data.emplace_back(Vector(keys.data[0]));
	join_head.SetCardinality(keys.size());

	// Sink the data into the local sort state
	local_sort_state.SinkChunk(join_head, input);
}

PhysicalRangeJoin::GlobalSortedTable::GlobalSortedTable(ClientContext &context, const vector<BoundOrderByNode> &orders,
                                                        RowLayout &payload_layout)
    : global_sort_state(BufferManager::GetBufferManager(context), orders, payload_layout), has_null(0), count(0),
      memory_per_thread(0) {
	D_ASSERT(orders.size() == 1);

	// Set external (can be force with the PRAGMA)
	auto &config = ClientConfig::GetConfig(context);
	global_sort_state.external = config.force_external;
	// Memory usage per thread should scale with max mem / num threads
	// We take 1/4th of this, to be conservative
	idx_t max_memory = global_sort_state.buffer_manager.GetMaxMemory();
	idx_t num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	memory_per_thread = (max_memory / num_threads) / 4;
}

void PhysicalRangeJoin::GlobalSortedTable::Combine(LocalSortedTable &ltable) {
	global_sort_state.AddLocalState(ltable.local_sort_state);
	has_null += ltable.has_null;
	count += ltable.count;
}

void PhysicalRangeJoin::GlobalSortedTable::IntializeMatches() {
	found_match = unique_ptr<bool[]>(new bool[Count()]);
	memset(found_match.get(), 0, sizeof(bool) * Count());
}

void PhysicalRangeJoin::GlobalSortedTable::Print() {
	global_sort_state.Print();
}

class RangeJoinMergeTask : public ExecutorTask {
public:
	using GlobalSortedTable = PhysicalRangeJoin::GlobalSortedTable;

public:
	RangeJoinMergeTask(shared_ptr<Event> event_p, ClientContext &context, GlobalSortedTable &table)
	    : ExecutorTask(context), event(move(event_p)), context(context), table(table) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		// Initialize iejoin sorted and iterate until done
		auto &global_sort_state = table.global_sort_state;
		MergeSorter merge_sorter(global_sort_state, BufferManager::GetBufferManager(context));
		merge_sorter.PerformInMergeRound();
		event->FinishTask();

		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	shared_ptr<Event> event;
	ClientContext &context;
	GlobalSortedTable &table;
};

class RangeJoinMergeEvent : public Event {
public:
	using GlobalSortedTable = PhysicalRangeJoin::GlobalSortedTable;

public:
	RangeJoinMergeEvent(GlobalSortedTable &table_p, Pipeline &pipeline_p)
	    : Event(pipeline_p.executor), table(table_p), pipeline(pipeline_p) {
	}

	GlobalSortedTable &table;
	Pipeline &pipeline;

public:
	void Schedule() override {
		auto &context = pipeline.GetClientContext();

		// Schedule tasks equal to the number of threads, which will each merge multiple partitions
		auto &ts = TaskScheduler::GetScheduler(context);
		idx_t num_threads = ts.NumberOfThreads();

		vector<unique_ptr<Task>> iejoin_tasks;
		for (idx_t tnum = 0; tnum < num_threads; tnum++) {
			iejoin_tasks.push_back(make_unique<RangeJoinMergeTask>(shared_from_this(), context, table));
		}
		SetTasks(move(iejoin_tasks));
	}

	void FinishEvent() override {
		auto &global_sort_state = table.global_sort_state;

		global_sort_state.CompleteMergeRound(true);
		if (global_sort_state.sorted_blocks.size() > 1) {
			// Multiple blocks remaining: Schedule the next round
			table.ScheduleMergeTasks(pipeline, *this);
		}
	}
};

void PhysicalRangeJoin::GlobalSortedTable::ScheduleMergeTasks(Pipeline &pipeline, Event &event) {
	// Initialize global sort state for a round of merging
	global_sort_state.InitializeMergeRound();
	auto new_event = make_shared<RangeJoinMergeEvent>(*this, pipeline);
	event.InsertEvent(move(new_event));
}

void PhysicalRangeJoin::GlobalSortedTable::Finalize(Pipeline &pipeline, Event &event) {
	// Prepare for merge sort phase
	global_sort_state.PrepareMergePhase();

	// Start the merge phase or finish if a merge is not necessary
	if (global_sort_state.sorted_blocks.size() > 1) {
		ScheduleMergeTasks(pipeline, event);
	}
}

PhysicalRangeJoin::PhysicalRangeJoin(LogicalOperator &op, PhysicalOperatorType type, unique_ptr<PhysicalOperator> left,
                                     unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                     idx_t estimated_cardinality)
    : PhysicalComparisonJoin(op, type, move(cond), join_type, estimated_cardinality) {
	// Reorder the conditions so that ranges are at the front.
	// TODO: use stats to improve the choice?
	// TODO: Prefer fixed length types?
	if (conditions.size() > 1) {
		auto conditions_p = std::move(conditions);
		conditions.resize(conditions_p.size());
		idx_t range_position = 0;
		idx_t other_position = conditions_p.size();
		for (idx_t i = 0; i < conditions_p.size(); ++i) {
			switch (conditions_p[i].comparison) {
			case ExpressionType::COMPARE_LESSTHAN:
			case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			case ExpressionType::COMPARE_GREATERTHAN:
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
				conditions[range_position++] = std::move(conditions_p[i]);
				break;
			default:
				conditions[--other_position] = std::move(conditions_p[i]);
				break;
			}
		}
	}

	children.push_back(move(left));
	children.push_back(move(right));
}

idx_t PhysicalRangeJoin::LocalSortedTable::MergeNulls(const vector<JoinCondition> &conditions) {
	// Merge the validity masks of the comparison keys into the primary
	// Return the number of NULLs in the resulting chunk
	D_ASSERT(keys.ColumnCount() > 0);
	const auto count = keys.size();

	size_t all_constant = 0;
	for (auto &v : keys.data) {
		if (v.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			++all_constant;
		}
	}

	auto &primary = keys.data[0];
	if (all_constant == keys.data.size()) {
		//	Either all NULL or no NULLs
		for (auto &v : keys.data) {
			if (ConstantVector::IsNull(v)) {
				ConstantVector::SetNull(primary, true);
				return count;
			}
		}
		return 0;
	} else if (keys.ColumnCount() > 1) {
		//	Normalify the primary, as it will need to merge arbitrary validity masks
		primary.Normalify(count);
		auto &pvalidity = FlatVector::Validity(primary);
		D_ASSERT(keys.ColumnCount() == conditions.size());
		for (size_t c = 1; c < keys.data.size(); ++c) {
			// Skip comparisons that accept NULLs
			if (conditions[c].comparison == ExpressionType::COMPARE_DISTINCT_FROM) {
				continue;
			}
			//	Orrify the rest, as the sort code will do this anyway.
			auto &v = keys.data[c];
			VectorData vdata;
			v.Orrify(count, vdata);
			auto &vvalidity = vdata.validity;
			if (vvalidity.AllValid()) {
				continue;
			}
			pvalidity.EnsureWritable();
			switch (v.GetVectorType()) {
			case VectorType::FLAT_VECTOR: {
				// Merge entire entries
				auto pmask = pvalidity.GetData();
				const auto entry_count = pvalidity.EntryCount(count);
				for (idx_t entry_idx = 0; entry_idx < entry_count; ++entry_idx) {
					pmask[entry_idx] &= vvalidity.GetValidityEntry(entry_idx);
				}
				break;
			}
			case VectorType::CONSTANT_VECTOR:
				// All or nothing
				if (ConstantVector::IsNull(v)) {
					pvalidity.SetAllInvalid(count);
					return count;
				}
				break;
			default:
				// One by one
				for (idx_t i = 0; i < count; ++i) {
					const auto idx = vdata.sel->get_index(i);
					if (!vvalidity.RowIsValidUnsafe(idx)) {
						pvalidity.SetInvalidUnsafe(i);
					}
				}
				break;
			}
		}
		return count - pvalidity.CountValid(count);
	} else {
		return count - VectorOperations::CountNotNull(primary, count);
	}
}

void PhysicalRangeJoin::SliceSortedPayload(DataChunk &payload, GlobalSortState &state, const idx_t block_idx,
                                           const SelectionVector &result, const idx_t result_count,
                                           const idx_t left_cols) {
	// There should only be one sorted block if they have been sorted
	D_ASSERT(state.sorted_blocks.size() == 1);
	SBScanState read_state(state.buffer_manager, state);
	read_state.sb = state.sorted_blocks[0].get();
	auto &sorted_data = *read_state.sb->payload_data;

	read_state.SetIndices(block_idx, 0);
	read_state.PinData(sorted_data);
	const auto data_ptr = read_state.DataPtr(sorted_data);

	// Set up a batch of pointers to scan data from
	Vector addresses(LogicalType::POINTER, result_count);
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);

	// Set up the data pointers for the values that are actually referenced
	const idx_t &row_width = sorted_data.layout.GetRowWidth();

	auto prev_idx = result.get_index(0);
	SelectionVector gsel(result_count);
	idx_t addr_count = 0;
	gsel.set_index(0, addr_count);
	data_pointers[addr_count] = data_ptr + prev_idx * row_width;
	for (idx_t i = 1; i < result_count; ++i) {
		const auto row_idx = result.get_index(i);
		if (row_idx != prev_idx) {
			data_pointers[++addr_count] = data_ptr + row_idx * row_width;
			prev_idx = row_idx;
		}
		gsel.set_index(i, addr_count);
	}
	++addr_count;

	// Unswizzle the offsets back to pointers (if needed)
	if (!sorted_data.layout.AllConstant() && state.external) {
		RowOperations::UnswizzlePointers(sorted_data.layout, data_ptr, read_state.payload_heap_handle.Ptr(),
		                                 addr_count);
	}

	// Deserialize the payload data
	auto sel = FlatVector::IncrementalSelectionVector();
	for (idx_t col_idx = 0; col_idx < sorted_data.layout.ColumnCount(); col_idx++) {
		const auto col_offset = sorted_data.layout.GetOffsets()[col_idx];
		auto &col = payload.data[left_cols + col_idx];
		RowOperations::Gather(addresses, *sel, col, *sel, addr_count, col_offset, col_idx);
		col.Slice(gsel, result_count);
	}
}

idx_t PhysicalRangeJoin::SelectJoinTail(const ExpressionType &condition, Vector &left, Vector &right,
                                        const SelectionVector *sel, idx_t count, SelectionVector *true_sel) {
	switch (condition) {
	case ExpressionType::COMPARE_NOTEQUAL:
		return VectorOperations::NotEquals(left, right, sel, count, true_sel, nullptr);
	case ExpressionType::COMPARE_LESSTHAN:
		return VectorOperations::LessThan(left, right, sel, count, true_sel, nullptr);
	case ExpressionType::COMPARE_GREATERTHAN:
		return VectorOperations::GreaterThan(left, right, sel, count, true_sel, nullptr);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return VectorOperations::LessThanEquals(left, right, sel, count, true_sel, nullptr);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return VectorOperations::GreaterThanEquals(left, right, sel, count, true_sel, nullptr);
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return VectorOperations::DistinctFrom(left, right, sel, count, true_sel, nullptr);
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
	case ExpressionType::COMPARE_EQUAL:
	default:
		throw InternalException("Unsupported comparison type for PhysicalRangeJoin");
	}

	return count;
}

} // namespace duckdb









namespace duckdb {

PhysicalOrder::PhysicalOrder(vector<LogicalType> types, vector<BoundOrderByNode> orders, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::ORDER_BY, move(types), estimated_cardinality), orders(move(orders)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class OrderGlobalState : public GlobalSinkState {
public:
	OrderGlobalState(BufferManager &buffer_manager, const PhysicalOrder &order, RowLayout &payload_layout)
	    : global_sort_state(buffer_manager, order.orders, payload_layout) {
	}

	//! Global sort state
	GlobalSortState global_sort_state;
	//! Memory usage per thread
	idx_t memory_per_thread;
};

class OrderLocalState : public LocalSinkState {
public:
	OrderLocalState(ExecutionContext &context, const vector<BoundOrderByNode> &orders)
	    : executor(Allocator::Get(context.client)) {
		// Initialize order clause expression executor and DataChunk
		vector<LogicalType> types;
		for (auto &order : orders) {
			types.push_back(order.expression->return_type);
			executor.AddExpression(*order.expression);
		}
		sort.Initialize(Allocator::Get(context.client), types);
	}

public:
	//! The local sort state
	LocalSortState local_sort_state;
	//! Local copy of the sorting expression executor
	ExpressionExecutor executor;
	//! Holds a vector of incoming sorting columns
	DataChunk sort;
};

unique_ptr<GlobalSinkState> PhysicalOrder::GetGlobalSinkState(ClientContext &context) const {
	// Get the payload layout from the return types
	RowLayout payload_layout;
	payload_layout.Initialize(types);
	auto state = make_unique<OrderGlobalState>(BufferManager::GetBufferManager(context), *this, payload_layout);
	// Set external (can be force with the PRAGMA)
	state->global_sort_state.external = ClientConfig::GetConfig(context).force_external;
	// Memory usage per thread should scale with max mem / num threads
	// We take 1/4th of this, to be conservative
	idx_t max_memory = BufferManager::GetBufferManager(context).GetMaxMemory();
	idx_t num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	state->memory_per_thread = (max_memory / num_threads) / 4;
	return move(state);
}

unique_ptr<LocalSinkState> PhysicalOrder::GetLocalSinkState(ExecutionContext &context) const {
	auto result = make_unique<OrderLocalState>(context, orders);
	return move(result);
}

SinkResultType PhysicalOrder::Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
                                   DataChunk &input) const {
	auto &gstate = (OrderGlobalState &)gstate_p;
	auto &lstate = (OrderLocalState &)lstate_p;

	auto &global_sort_state = gstate.global_sort_state;
	auto &local_sort_state = lstate.local_sort_state;

	// Initialize local state (if necessary)
	if (!local_sort_state.initialized) {
		local_sort_state.Initialize(global_sort_state, BufferManager::GetBufferManager(context.client));
	}

	// Obtain sorting columns
	auto &sort = lstate.sort;
	sort.Reset();
	lstate.executor.Execute(input, sort);

	// Sink the data into the local sort state
	sort.Verify();
	input.Verify();
	local_sort_state.SinkChunk(sort, input);

	// When sorting data reaches a certain size, we sort it
	if (local_sort_state.SizeInBytes() >= gstate.memory_per_thread) {
		local_sort_state.Sort(global_sort_state, true);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalOrder::Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const {
	auto &gstate = (OrderGlobalState &)gstate_p;
	auto &lstate = (OrderLocalState &)lstate_p;
	gstate.global_sort_state.AddLocalState(lstate.local_sort_state);
}

class PhysicalOrderMergeTask : public ExecutorTask {
public:
	PhysicalOrderMergeTask(shared_ptr<Event> event_p, ClientContext &context, OrderGlobalState &state)
	    : ExecutorTask(context), event(move(event_p)), context(context), state(state) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		// Initialize merge sorted and iterate until done
		auto &global_sort_state = state.global_sort_state;
		MergeSorter merge_sorter(global_sort_state, BufferManager::GetBufferManager(context));
		merge_sorter.PerformInMergeRound();
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	shared_ptr<Event> event;
	ClientContext &context;
	OrderGlobalState &state;
};

class OrderMergeEvent : public Event {
public:
	OrderMergeEvent(OrderGlobalState &gstate_p, Pipeline &pipeline_p)
	    : Event(pipeline_p.executor), gstate(gstate_p), pipeline(pipeline_p) {
	}

	OrderGlobalState &gstate;
	Pipeline &pipeline;

public:
	void Schedule() override {
		auto &context = pipeline.GetClientContext();

		// Schedule tasks equal to the number of threads, which will each merge multiple partitions
		auto &ts = TaskScheduler::GetScheduler(context);
		idx_t num_threads = ts.NumberOfThreads();

		vector<unique_ptr<Task>> merge_tasks;
		for (idx_t tnum = 0; tnum < num_threads; tnum++) {
			merge_tasks.push_back(make_unique<PhysicalOrderMergeTask>(shared_from_this(), context, gstate));
		}
		SetTasks(move(merge_tasks));
	}

	void FinishEvent() override {
		auto &global_sort_state = gstate.global_sort_state;

		global_sort_state.CompleteMergeRound();
		if (global_sort_state.sorted_blocks.size() > 1) {
			// Multiple blocks remaining: Schedule the next round
			PhysicalOrder::ScheduleMergeTasks(pipeline, *this, gstate);
		}
	}
};

SinkFinalizeType PhysicalOrder::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                         GlobalSinkState &gstate_p) const {
	auto &state = (OrderGlobalState &)gstate_p;
	auto &global_sort_state = state.global_sort_state;

	if (global_sort_state.sorted_blocks.empty()) {
		// Empty input!
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Prepare for merge sort phase
	global_sort_state.PrepareMergePhase();

	// Start the merge phase or finish if a merge is not necessary
	if (global_sort_state.sorted_blocks.size() > 1) {
		PhysicalOrder::ScheduleMergeTasks(pipeline, event, state);
	}
	return SinkFinalizeType::READY;
}

void PhysicalOrder::ScheduleMergeTasks(Pipeline &pipeline, Event &event, OrderGlobalState &state) {
	// Initialize global sort state for a round of merging
	state.global_sort_state.InitializeMergeRound();
	auto new_event = make_shared<OrderMergeEvent>(state, pipeline);
	event.InsertEvent(move(new_event));
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PhysicalOrderOperatorState : public GlobalSourceState {
public:
	//! Payload scanner
	unique_ptr<PayloadScanner> scanner;
};

unique_ptr<GlobalSourceState> PhysicalOrder::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<PhysicalOrderOperatorState>();
}

void PhysicalOrder::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                            LocalSourceState &lstate) const {
	auto &state = (PhysicalOrderOperatorState &)gstate;

	if (!state.scanner) {
		// Initialize scanner (if not yet initialized)
		auto &gstate = (OrderGlobalState &)*this->sink_state;
		auto &global_sort_state = gstate.global_sort_state;
		if (global_sort_state.sorted_blocks.empty()) {
			return;
		}
		state.scanner =
		    make_unique<PayloadScanner>(*global_sort_state.sorted_blocks[0]->payload_data, global_sort_state);
	}

	// Scan the next data chunk
	state.scanner->Scan(chunk);
}

string PhysicalOrder::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < orders.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += orders[i].expression->ToString() + " ";
		result += orders[i].type == OrderType::DESCENDING ? "DESC" : "ASC";
	}
	return result;
}

} // namespace duckdb










namespace duckdb {

PhysicalTopN::PhysicalTopN(vector<LogicalType> types, vector<BoundOrderByNode> orders, idx_t limit, idx_t offset,
                           idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::TOP_N, move(types), estimated_cardinality), orders(move(orders)),
      limit(limit), offset(offset) {
}

//===--------------------------------------------------------------------===//
// Heaps
//===--------------------------------------------------------------------===//
class TopNHeap;

struct TopNScanState {
	unique_ptr<PayloadScanner> scanner;
	idx_t pos;
	bool exclude_offset;
};

class TopNSortState {
public:
	explicit TopNSortState(TopNHeap &heap);

	TopNHeap &heap;
	unique_ptr<LocalSortState> local_state;
	unique_ptr<GlobalSortState> global_state;
	idx_t count;
	bool is_sorted;

public:
	void Initialize();
	void Append(DataChunk &sort_chunk, DataChunk &payload);

	void Sink(DataChunk &input);
	void Finalize();

	void Move(TopNSortState &other);

	void InitializeScan(TopNScanState &state, bool exclude_offset);
	void Scan(TopNScanState &state, DataChunk &chunk);
};

class TopNHeap {
public:
	TopNHeap(ClientContext &context, const vector<LogicalType> &payload_types, const vector<BoundOrderByNode> &orders,
	         idx_t limit, idx_t offset);
	TopNHeap(ExecutionContext &context, const vector<LogicalType> &payload_types,
	         const vector<BoundOrderByNode> &orders, idx_t limit, idx_t offset);
	TopNHeap(BufferManager &buffer_manager, Allocator &allocator, const vector<LogicalType> &payload_types,
	         const vector<BoundOrderByNode> &orders, idx_t limit, idx_t offset);

	Allocator &allocator;
	BufferManager &buffer_manager;
	const vector<LogicalType> &payload_types;
	const vector<BoundOrderByNode> &orders;
	idx_t limit;
	idx_t offset;
	TopNSortState sort_state;
	ExpressionExecutor executor;
	DataChunk sort_chunk;
	DataChunk compare_chunk;
	DataChunk payload_chunk;
	//! A set of boundary values that determine either the minimum or the maximum value we have to consider for our
	//! top-n
	DataChunk boundary_values;
	//! Whether or not the boundary_values has been set. The boundary_values are only set after a reduce step
	bool has_boundary_values;

	SelectionVector final_sel;
	SelectionVector true_sel;
	SelectionVector false_sel;
	SelectionVector new_remaining_sel;

public:
	void Sink(DataChunk &input);
	void Combine(TopNHeap &other);
	void Reduce();
	void Finalize();

	void ExtractBoundaryValues(DataChunk &current_chunk, DataChunk &prev_chunk);

	void InitializeScan(TopNScanState &state, bool exclude_offset);
	void Scan(TopNScanState &state, DataChunk &chunk);

	bool CheckBoundaryValues(DataChunk &sort_chunk, DataChunk &payload);
};

//===--------------------------------------------------------------------===//
// TopNSortState
//===--------------------------------------------------------------------===//
TopNSortState::TopNSortState(TopNHeap &heap) : heap(heap), count(0), is_sorted(false) {
}

void TopNSortState::Initialize() {
	RowLayout layout;
	layout.Initialize(heap.payload_types);
	auto &buffer_manager = heap.buffer_manager;
	global_state = make_unique<GlobalSortState>(buffer_manager, heap.orders, layout);
	local_state = make_unique<LocalSortState>();
	local_state->Initialize(*global_state, buffer_manager);
}

void TopNSortState::Append(DataChunk &sort_chunk, DataChunk &payload) {
	D_ASSERT(!is_sorted);
	if (heap.has_boundary_values) {
		if (!heap.CheckBoundaryValues(sort_chunk, payload)) {
			return;
		}
	}

	local_state->SinkChunk(sort_chunk, payload);
	count += payload.size();
}

void TopNSortState::Sink(DataChunk &input) {
	// compute the ordering values for the new chunk
	heap.sort_chunk.Reset();
	heap.executor.Execute(input, heap.sort_chunk);

	// append the new chunk to what we have already
	Append(heap.sort_chunk, input);
}

void TopNSortState::Move(TopNSortState &other) {
	local_state = move(other.local_state);
	global_state = move(other.global_state);
	count = other.count;
	is_sorted = other.is_sorted;
}

void TopNSortState::Finalize() {
	D_ASSERT(!is_sorted);
	global_state->AddLocalState(*local_state);

	global_state->PrepareMergePhase();
	while (global_state->sorted_blocks.size() > 1) {
		MergeSorter merge_sorter(*global_state, heap.buffer_manager);
		merge_sorter.PerformInMergeRound();
		global_state->CompleteMergeRound();
	}
	is_sorted = true;
}

void TopNSortState::InitializeScan(TopNScanState &state, bool exclude_offset) {
	D_ASSERT(is_sorted);
	if (global_state->sorted_blocks.empty()) {
		state.scanner = nullptr;
	} else {
		D_ASSERT(global_state->sorted_blocks.size() == 1);
		state.scanner = make_unique<PayloadScanner>(*global_state->sorted_blocks[0]->payload_data, *global_state);
	}
	state.pos = 0;
	state.exclude_offset = exclude_offset && heap.offset > 0;
}

void TopNSortState::Scan(TopNScanState &state, DataChunk &chunk) {
	if (!state.scanner) {
		return;
	}
	auto offset = heap.offset;
	auto limit = heap.limit;
	D_ASSERT(is_sorted);
	while (chunk.size() == 0) {
		state.scanner->Scan(chunk);
		if (chunk.size() == 0) {
			break;
		}
		idx_t start = state.pos;
		idx_t end = state.pos + chunk.size();
		state.pos = end;

		idx_t chunk_start = 0;
		idx_t chunk_end = chunk.size();
		if (state.exclude_offset) {
			// we need to exclude all tuples before the OFFSET
			// check if we should include anything
			if (end <= offset) {
				// end is smaller than offset: include nothing!
				chunk.Reset();
				continue;
			} else if (start < offset) {
				// we need to slice
				chunk_start = offset - start;
			}
		}
		// check if we need to truncate at the offset + limit mark
		if (start >= offset + limit) {
			// we are finished
			chunk_end = 0;
		} else if (end > offset + limit) {
			// the end extends past the offset + limit
			// truncate the current chunk
			chunk_end = offset + limit - start;
		}
		D_ASSERT(chunk_end - chunk_start <= STANDARD_VECTOR_SIZE);
		if (chunk_end == chunk_start) {
			chunk.Reset();
			break;
		} else if (chunk_start > 0) {
			SelectionVector sel(STANDARD_VECTOR_SIZE);
			for (idx_t i = chunk_start; i < chunk_end; i++) {
				sel.set_index(i - chunk_start, i);
			}
			chunk.Slice(sel, chunk_end - chunk_start);
		} else if (chunk_end != chunk.size()) {
			chunk.SetCardinality(chunk_end);
		}
	}
}

//===--------------------------------------------------------------------===//
// TopNHeap
//===--------------------------------------------------------------------===//
TopNHeap::TopNHeap(BufferManager &buffer_manager, Allocator &allocator, const vector<LogicalType> &payload_types_p,
                   const vector<BoundOrderByNode> &orders_p, idx_t limit, idx_t offset)
    : allocator(allocator), buffer_manager(buffer_manager), payload_types(payload_types_p), orders(orders_p),
      limit(limit), offset(offset), sort_state(*this), executor(allocator), has_boundary_values(false),
      final_sel(STANDARD_VECTOR_SIZE), true_sel(STANDARD_VECTOR_SIZE), false_sel(STANDARD_VECTOR_SIZE),
      new_remaining_sel(STANDARD_VECTOR_SIZE) {
	// initialize the executor and the sort_chunk
	vector<LogicalType> sort_types;
	for (auto &order : orders) {
		auto &expr = order.expression;
		sort_types.push_back(expr->return_type);
		executor.AddExpression(*expr);
	}
	payload_chunk.Initialize(allocator, payload_types);
	sort_chunk.Initialize(allocator, sort_types);
	compare_chunk.Initialize(allocator, sort_types);
	boundary_values.Initialize(allocator, sort_types);
	sort_state.Initialize();
}

TopNHeap::TopNHeap(ClientContext &context, const vector<LogicalType> &payload_types,
                   const vector<BoundOrderByNode> &orders, idx_t limit, idx_t offset)
    : TopNHeap(BufferManager::GetBufferManager(context), BufferAllocator::Get(context), payload_types, orders, limit,
               offset) {
}

TopNHeap::TopNHeap(ExecutionContext &context, const vector<LogicalType> &payload_types,
                   const vector<BoundOrderByNode> &orders, idx_t limit, idx_t offset)
    : TopNHeap(BufferManager::GetBufferManager(context.client), Allocator::Get(context.client), payload_types, orders,
               limit, offset) {
}

void TopNHeap::Sink(DataChunk &input) {
	sort_state.Sink(input);
}

void TopNHeap::Combine(TopNHeap &other) {
	other.Finalize();

	TopNScanState state;
	other.InitializeScan(state, false);
	while (true) {
		payload_chunk.Reset();
		other.Scan(state, payload_chunk);
		if (payload_chunk.size() == 0) {
			break;
		}
		Sink(payload_chunk);
	}
	Reduce();
}

void TopNHeap::Finalize() {
	sort_state.Finalize();
}

void TopNHeap::Reduce() {
	idx_t min_sort_threshold = MaxValue<idx_t>(STANDARD_VECTOR_SIZE * 5, 2 * (limit + offset));
	if (sort_state.count < min_sort_threshold) {
		// only reduce when we pass two times the limit + offset, or 5 vectors (whichever comes first)
		return;
	}
	sort_state.Finalize();
	TopNSortState new_state(*this);
	new_state.Initialize();

	TopNScanState state;
	sort_state.InitializeScan(state, false);

	DataChunk new_chunk;
	new_chunk.Initialize(allocator, payload_types);

	DataChunk *current_chunk = &new_chunk;
	DataChunk *prev_chunk = &payload_chunk;
	has_boundary_values = false;
	while (true) {
		current_chunk->Reset();
		Scan(state, *current_chunk);
		if (current_chunk->size() == 0) {
			ExtractBoundaryValues(*current_chunk, *prev_chunk);
			break;
		}
		new_state.Sink(*current_chunk);
		std::swap(current_chunk, prev_chunk);
	}

	sort_state.Move(new_state);
}

void TopNHeap::ExtractBoundaryValues(DataChunk &current_chunk, DataChunk &prev_chunk) {
	// extract the last entry of the prev_chunk and set as minimum value
	D_ASSERT(prev_chunk.size() > 0);
	for (idx_t col_idx = 0; col_idx < current_chunk.ColumnCount(); col_idx++) {
		ConstantVector::Reference(current_chunk.data[col_idx], prev_chunk.data[col_idx], prev_chunk.size() - 1,
		                          prev_chunk.size());
	}
	current_chunk.SetCardinality(1);
	sort_chunk.Reset();
	executor.Execute(&current_chunk, sort_chunk);

	boundary_values.Reset();
	boundary_values.Append(sort_chunk);
	boundary_values.SetCardinality(1);
	for (idx_t i = 0; i < boundary_values.ColumnCount(); i++) {
		boundary_values.data[i].SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	has_boundary_values = true;
}

bool TopNHeap::CheckBoundaryValues(DataChunk &sort_chunk, DataChunk &payload) {
	// we have boundary values
	// from these boundary values, determine which values we should insert (if any)
	idx_t final_count = 0;

	SelectionVector remaining_sel(nullptr);
	idx_t remaining_count = sort_chunk.size();
	for (idx_t i = 0; i < orders.size(); i++) {
		if (remaining_sel.data()) {
			compare_chunk.data[i].Slice(sort_chunk.data[i], remaining_sel, remaining_count);
		} else {
			compare_chunk.data[i].Reference(sort_chunk.data[i]);
		}
		bool is_last = i + 1 == orders.size();
		idx_t true_count;
		if (orders[i].null_order == OrderByNullType::NULLS_LAST) {
			if (orders[i].type == OrderType::ASCENDING) {
				true_count = VectorOperations::DistinctLessThan(compare_chunk.data[i], boundary_values.data[i],
				                                                &remaining_sel, remaining_count, &true_sel, &false_sel);
			} else {
				true_count = VectorOperations::DistinctGreaterThanNullsFirst(compare_chunk.data[i],
				                                                             boundary_values.data[i], &remaining_sel,
				                                                             remaining_count, &true_sel, &false_sel);
			}
		} else {
			D_ASSERT(orders[i].null_order == OrderByNullType::NULLS_FIRST);
			if (orders[i].type == OrderType::ASCENDING) {
				true_count = VectorOperations::DistinctLessThanNullsFirst(compare_chunk.data[i],
				                                                          boundary_values.data[i], &remaining_sel,
				                                                          remaining_count, &true_sel, &false_sel);
			} else {
				true_count =
				    VectorOperations::DistinctGreaterThan(compare_chunk.data[i], boundary_values.data[i],
				                                          &remaining_sel, remaining_count, &true_sel, &false_sel);
			}
		}

		if (true_count > 0) {
			memcpy(final_sel.data() + final_count, true_sel.data(), true_count * sizeof(sel_t));
			final_count += true_count;
		}
		idx_t false_count = remaining_count - true_count;
		if (false_count > 0) {
			// check what we should continue to check
			compare_chunk.data[i].Slice(sort_chunk.data[i], false_sel, false_count);
			remaining_count = VectorOperations::NotDistinctFrom(compare_chunk.data[i], boundary_values.data[i],
			                                                    &false_sel, false_count, &new_remaining_sel, nullptr);
			if (is_last) {
				memcpy(final_sel.data() + final_count, new_remaining_sel.data(), remaining_count * sizeof(sel_t));
				final_count += remaining_count;
			} else {
				remaining_sel.Initialize(new_remaining_sel);
			}
		} else {
			break;
		}
	}
	if (final_count == 0) {
		return false;
	}
	if (final_count < sort_chunk.size()) {
		sort_chunk.Slice(final_sel, final_count);
		payload.Slice(final_sel, final_count);
	}
	return true;
}

void TopNHeap::InitializeScan(TopNScanState &state, bool exclude_offset) {
	sort_state.InitializeScan(state, exclude_offset);
}

void TopNHeap::Scan(TopNScanState &state, DataChunk &chunk) {
	sort_state.Scan(state, chunk);
}

class TopNGlobalState : public GlobalSinkState {
public:
	TopNGlobalState(ClientContext &context, const vector<LogicalType> &payload_types,
	                const vector<BoundOrderByNode> &orders, idx_t limit, idx_t offset)
	    : heap(context, payload_types, orders, limit, offset) {
	}

	mutex lock;
	TopNHeap heap;
};

class TopNLocalState : public LocalSinkState {
public:
	TopNLocalState(ExecutionContext &context, const vector<LogicalType> &payload_types,
	               const vector<BoundOrderByNode> &orders, idx_t limit, idx_t offset)
	    : heap(context, payload_types, orders, limit, offset) {
	}

	TopNHeap heap;
};

unique_ptr<LocalSinkState> PhysicalTopN::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<TopNLocalState>(context, types, orders, limit, offset);
}

unique_ptr<GlobalSinkState> PhysicalTopN::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<TopNGlobalState>(context, types, orders, limit, offset);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PhysicalTopN::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                  DataChunk &input) const {
	// append to the local sink state
	auto &sink = (TopNLocalState &)lstate;
	sink.heap.Sink(input);
	sink.heap.Reduce();
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
void PhysicalTopN::Combine(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate_p) const {
	auto &gstate = (TopNGlobalState &)state;
	auto &lstate = (TopNLocalState &)lstate_p;

	// scan the local top N and append it to the global heap
	lock_guard<mutex> glock(gstate.lock);
	gstate.heap.Combine(lstate.heap);
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PhysicalTopN::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                        GlobalSinkState &gstate_p) const {
	auto &gstate = (TopNGlobalState &)gstate_p;
	// global finalize: compute the final top N
	gstate.heap.Finalize();
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class TopNOperatorState : public GlobalSourceState {
public:
	TopNScanState state;
	bool initialized = false;
};

unique_ptr<GlobalSourceState> PhysicalTopN::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<TopNOperatorState>();
}

void PhysicalTopN::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                           LocalSourceState &lstate) const {
	if (limit == 0) {
		return;
	}
	auto &state = (TopNOperatorState &)gstate_p;
	auto &gstate = (TopNGlobalState &)*sink_state;

	if (!state.initialized) {
		gstate.heap.InitializeScan(state.state, true);
		state.initialized = true;
	}
	gstate.heap.Scan(state.state, chunk);
}

string PhysicalTopN::ParamsToString() const {
	string result;
	result += "Top " + to_string(limit);
	if (offset > 0) {
		result += "\n";
		result += "Offset " + to_string(offset);
	}
	result += "\n[INFOSEPARATOR]";
	for (idx_t i = 0; i < orders.size(); i++) {
		result += "\n";
		result += orders[i].expression->ToString() + " ";
		result += orders[i].type == OrderType::DESCENDING ? "DESC" : "ASC";
	}
	return result;
}

} // namespace duckdb

















#include <algorithm>
#include <cctype>
#include <cstring>
#include <fstream>

namespace duckdb {

static bool ParseBoolean(const Value &value, const string &loption);

static bool ParseBoolean(const vector<Value> &set, const string &loption) {
	if (set.empty()) {
		// no option specified: default to true
		return true;
	}
	if (set.size() > 1) {
		throw BinderException("\"%s\" expects a single argument as a boolean value (e.g. TRUE or 1)", loption);
	}
	return ParseBoolean(set[0], loption);
}

static bool ParseBoolean(const Value &value, const string &loption) {

	if (value.type().id() == LogicalTypeId::LIST) {
		auto &children = ListValue::GetChildren(value);
		return ParseBoolean(children, loption);
	}
	if (value.type() == LogicalType::FLOAT || value.type() == LogicalType::DOUBLE ||
	    value.type().id() == LogicalTypeId::DECIMAL) {
		throw BinderException("\"%s\" expects a boolean value (e.g. TRUE or 1)", loption);
	}
	return BooleanValue::Get(value.CastAs(LogicalType::BOOLEAN));
}

static string ParseString(const Value &value, const string &loption) {
	if (value.type().id() == LogicalTypeId::LIST) {
		auto &children = ListValue::GetChildren(value);
		if (children.size() != 1) {
			throw BinderException("\"%s\" expects a single argument as a string value", loption);
		}
		return ParseString(children[0], loption);
	}
	if (value.type().id() != LogicalTypeId::VARCHAR) {
		throw BinderException("\"%s\" expects a string argument!", loption);
	}
	return value.GetValue<string>();
}

static int64_t ParseInteger(const Value &value, const string &loption) {
	if (value.type().id() == LogicalTypeId::LIST) {
		auto &children = ListValue::GetChildren(value);
		if (children.size() != 1) {
			// no option specified or multiple options specified
			throw BinderException("\"%s\" expects a single argument as an integer value", loption);
		}
		return ParseInteger(children[0], loption);
	}
	return value.GetValue<int64_t>();
}

static vector<bool> ParseColumnList(const vector<Value> &set, vector<string> &names, const string &loption) {
	vector<bool> result;

	if (set.empty()) {
		throw BinderException("\"%s\" expects a column list or * as parameter", loption);
	}
	// list of options: parse the list
	unordered_map<string, bool> option_map;
	for (idx_t i = 0; i < set.size(); i++) {
		option_map[set[i].ToString()] = false;
	}
	result.resize(names.size(), false);
	for (idx_t i = 0; i < names.size(); i++) {
		auto entry = option_map.find(names[i]);
		if (entry != option_map.end()) {
			result[i] = true;
			entry->second = true;
		}
	}
	for (auto &entry : option_map) {
		if (!entry.second) {
			throw BinderException("\"%s\" expected to find %s, but it was not found in the table", loption,
			                      entry.first.c_str());
		}
	}
	return result;
}

static vector<bool> ParseColumnList(const Value &value, vector<string> &names, const string &loption) {
	vector<bool> result;

	// Only accept a list of arguments
	if (value.type().id() != LogicalTypeId::LIST) {
		// Support a single argument if it's '*'
		if (value.type().id() == LogicalTypeId::VARCHAR && value.GetValue<string>() == "*") {
			result.resize(names.size(), true);
			return result;
		}
		throw BinderException("\"%s\" expects a column list or * as parameter", loption);
	}
	auto &children = ListValue::GetChildren(value);
	// accept '*' as single argument
	if (children.size() == 1 && children[0].type().id() == LogicalTypeId::VARCHAR &&
	    children[0].GetValue<string>() == "*") {
		result.resize(names.size(), true);
		return result;
	}
	return ParseColumnList(children, names, loption);
}

struct CSVFileHandle {
public:
	explicit CSVFileHandle(unique_ptr<FileHandle> file_handle_p) : file_handle(move(file_handle_p)) {
		can_seek = file_handle->CanSeek();
		plain_file_source = file_handle->OnDiskFile() && can_seek;
		file_size = file_handle->GetFileSize();
	}

	bool CanSeek() {
		return can_seek;
	}
	void Seek(idx_t position) {
		if (!can_seek) {
			throw InternalException("Cannot seek in this file");
		}
		file_handle->Seek(position);
	}
	idx_t SeekPosition() {
		if (!can_seek) {
			throw InternalException("Cannot seek in this file");
		}
		return file_handle->SeekPosition();
	}
	void Reset() {
		if (plain_file_source) {
			file_handle->Reset();
		} else {
			if (!reset_enabled) {
				throw InternalException("Reset called but reset is not enabled for this CSV Handle");
			}
			read_position = 0;
		}
	}
	bool PlainFileSource() {
		return plain_file_source;
	}

	bool OnDiskFile() {
		return file_handle->OnDiskFile();
	}

	idx_t FileSize() {
		return file_size;
	}

	idx_t Read(void *buffer, idx_t nr_bytes) {
		if (!plain_file_source) {
			// not a plain file source: we need to do some bookkeeping around the reset functionality
			idx_t result_offset = 0;
			if (read_position < buffer_size) {
				// we need to read from our cached buffer
				auto buffer_read_count = MinValue<idx_t>(nr_bytes, buffer_size - read_position);
				memcpy(buffer, cached_buffer.get() + read_position, buffer_read_count);
				result_offset += buffer_read_count;
				read_position += buffer_read_count;
				if (result_offset == nr_bytes) {
					return nr_bytes;
				}
			} else if (!reset_enabled && cached_buffer) {
				// reset is disabled but we still have cached data
				// we can remove any cached data
				cached_buffer.reset();
				buffer_size = 0;
				buffer_capacity = 0;
				read_position = 0;
			}
			// we have data left to read from the file
			// read directly into the buffer
			auto bytes_read = file_handle->Read((char *)buffer + result_offset, nr_bytes - result_offset);
			read_position += bytes_read;
			if (reset_enabled) {
				// if reset caching is enabled, we need to cache the bytes that we have read
				if (buffer_size + bytes_read >= buffer_capacity) {
					// no space; first enlarge the buffer
					buffer_capacity = MaxValue<idx_t>(NextPowerOfTwo(buffer_size + bytes_read), buffer_capacity * 2);

					auto new_buffer = unique_ptr<data_t[]>(new data_t[buffer_capacity]);
					if (buffer_size > 0) {
						memcpy(new_buffer.get(), cached_buffer.get(), buffer_size);
					}
					cached_buffer = move(new_buffer);
				}
				memcpy(cached_buffer.get() + buffer_size, (char *)buffer + result_offset, bytes_read);
				buffer_size += bytes_read;
			}

			return result_offset + bytes_read;
		} else {
			return file_handle->Read(buffer, nr_bytes);
		}
	}

	string ReadLine() {
		string result;
		char buffer[1];
		while (true) {
			idx_t tuples_read = Read(buffer, 1);
			if (tuples_read == 0 || buffer[0] == '\n') {
				return result;
			}
			if (buffer[0] != '\r') {
				result += buffer[0];
			}
		}
	}

	void DisableReset() {
		this->reset_enabled = false;
	}

private:
	unique_ptr<FileHandle> file_handle;
	bool reset_enabled = true;
	bool can_seek = false;
	bool plain_file_source = false;
	idx_t file_size = 0;
	// reset support
	unique_ptr<data_t[]> cached_buffer;
	idx_t read_position = 0;
	idx_t buffer_size = 0;
	idx_t buffer_capacity = 0;
};

void BufferedCSVReaderOptions::SetDelimiter(const string &input) {
	this->delimiter = StringUtil::Replace(input, "\\t", "\t");
	this->has_delimiter = true;
	if (input.empty()) {
		this->delimiter = string("\0", 1);
	}
}

void BufferedCSVReaderOptions::SetDateFormat(LogicalTypeId type, const string &format, bool read_format) {
	string error;
	if (read_format) {
		auto &date_format = this->date_format[type];
		error = StrTimeFormat::ParseFormatSpecifier(format, date_format);
		date_format.format_specifier = format;
	} else {
		auto &date_format = this->write_date_format[type];
		error = StrTimeFormat::ParseFormatSpecifier(format, date_format);
	}
	if (!error.empty()) {
		throw InvalidInputException("Could not parse DATEFORMAT: %s", error.c_str());
	}
	has_format[type] = true;
}

void BufferedCSVReaderOptions::SetReadOption(const string &loption, const Value &value,
                                             vector<string> &expected_names) {
	if (SetBaseOption(loption, value)) {
		return;
	}
	if (loption == "auto_detect") {
		auto_detect = ParseBoolean(value, loption);
	} else if (loption == "sample_size") {
		int64_t sample_size = ParseInteger(value, loption);
		if (sample_size < 1 && sample_size != -1) {
			throw BinderException("Unsupported parameter for SAMPLE_SIZE: cannot be smaller than 1");
		}
		if (sample_size == -1) {
			sample_chunks = std::numeric_limits<uint64_t>::max();
			sample_chunk_size = STANDARD_VECTOR_SIZE;
		} else if (sample_size <= STANDARD_VECTOR_SIZE) {
			sample_chunk_size = sample_size;
			sample_chunks = 1;
		} else {
			sample_chunk_size = STANDARD_VECTOR_SIZE;
			sample_chunks = sample_size / STANDARD_VECTOR_SIZE;
		}
	} else if (loption == "skip") {
		skip_rows = ParseInteger(value, loption);
	} else if (loption == "max_line_size" || loption == "maximum_line_size") {
		maximum_line_size = ParseInteger(value, loption);
	} else if (loption == "sample_chunk_size") {
		sample_chunk_size = ParseInteger(value, loption);
		if (sample_chunk_size > STANDARD_VECTOR_SIZE) {
			throw BinderException(
			    "Unsupported parameter for SAMPLE_CHUNK_SIZE: cannot be bigger than STANDARD_VECTOR_SIZE %d",
			    STANDARD_VECTOR_SIZE);
		} else if (sample_chunk_size < 1) {
			throw BinderException("Unsupported parameter for SAMPLE_CHUNK_SIZE: cannot be smaller than 1");
		}
	} else if (loption == "sample_chunks") {
		sample_chunks = ParseInteger(value, loption);
		if (sample_chunks < 1) {
			throw BinderException("Unsupported parameter for SAMPLE_CHUNKS: cannot be smaller than 1");
		}
	} else if (loption == "force_not_null") {
		force_not_null = ParseColumnList(value, expected_names, loption);
	} else if (loption == "date_format" || loption == "dateformat") {
		string format = ParseString(value, loption);
		SetDateFormat(LogicalTypeId::DATE, format, true);
	} else if (loption == "timestamp_format" || loption == "timestampformat") {
		string format = ParseString(value, loption);
		SetDateFormat(LogicalTypeId::TIMESTAMP, format, true);
	} else if (loption == "escape") {
		escape = ParseString(value, loption);
		has_escape = true;
	} else if (loption == "ignore_errors") {
		ignore_errors = ParseBoolean(value, loption);
	} else {
		throw BinderException("Unrecognized option for CSV reader \"%s\"", loption);
	}
}

void BufferedCSVReaderOptions::SetWriteOption(const string &loption, const Value &value) {
	if (SetBaseOption(loption, value)) {
		return;
	}

	if (loption == "force_quote") {
		force_quote = ParseColumnList(value, names, loption);
	} else if (loption == "date_format" || loption == "dateformat") {
		string format = ParseString(value, loption);
		SetDateFormat(LogicalTypeId::DATE, format, false);
	} else if (loption == "timestamp_format" || loption == "timestampformat") {
		string format = ParseString(value, loption);
		if (StringUtil::Lower(format) == "iso") {
			format = "%Y-%m-%dT%H:%M:%S.%fZ";
		}
		SetDateFormat(LogicalTypeId::TIMESTAMP, format, false);
	} else {
		throw BinderException("Unrecognized option CSV writer \"%s\"", loption);
	}
}

bool BufferedCSVReaderOptions::SetBaseOption(const string &loption, const Value &value) {
	// Make sure this function was only called after the option was turned into lowercase
	D_ASSERT(!std::any_of(loption.begin(), loption.end(), ::isupper));

	if (StringUtil::StartsWith(loption, "delim") || StringUtil::StartsWith(loption, "sep")) {
		SetDelimiter(ParseString(value, loption));
	} else if (loption == "quote") {
		quote = ParseString(value, loption);
		has_quote = true;
	} else if (loption == "escape") {
		escape = ParseString(value, loption);
		has_escape = true;
	} else if (loption == "header") {
		header = ParseBoolean(value, loption);
		has_header = true;
	} else if (loption == "null" || loption == "nullstr") {
		null_str = ParseString(value, loption);
	} else if (loption == "encoding") {
		auto encoding = StringUtil::Lower(ParseString(value, loption));
		if (encoding != "utf8" && encoding != "utf-8") {
			throw BinderException("Copy is only supported for UTF-8 encoded files, ENCODING 'UTF-8'");
		}
	} else if (loption == "compression") {
		compression = FileCompressionTypeFromString(ParseString(value, loption));
	} else {
		// unrecognized option in base CSV
		return false;
	}
	return true;
}

std::string BufferedCSVReaderOptions::ToString() const {
	return "DELIMITER='" + delimiter + (has_delimiter ? "'" : (auto_detect ? "' (auto detected)" : "' (default)")) +
	       ", QUOTE='" + quote + (has_quote ? "'" : (auto_detect ? "' (auto detected)" : "' (default)")) +
	       ", ESCAPE='" + escape + (has_escape ? "'" : (auto_detect ? "' (auto detected)" : "' (default)")) +
	       ", HEADER=" + std::to_string(header) +
	       (has_header ? "" : (auto_detect ? " (auto detected)" : "' (default)")) +
	       ", SAMPLE_SIZE=" + std::to_string(sample_chunk_size * sample_chunks) +
	       ", IGNORE_ERRORS=" + std::to_string(ignore_errors) + ", ALL_VARCHAR=" + std::to_string(all_varchar);
}

static string GetLineNumberStr(idx_t linenr, bool linenr_estimated) {
	string estimated = (linenr_estimated ? string(" (estimated)") : string(""));
	return to_string(linenr + 1) + estimated;
}

static bool StartsWithNumericDate(string &separator, const string &value) {
	auto begin = value.c_str();
	auto end = begin + value.size();

	//	StrpTimeFormat::Parse will skip whitespace, so we can too
	auto field1 = std::find_if_not(begin, end, StringUtil::CharacterIsSpace);
	if (field1 == end) {
		return false;
	}

	//	first numeric field must start immediately
	if (!StringUtil::CharacterIsDigit(*field1)) {
		return false;
	}
	auto literal1 = std::find_if_not(field1, end, StringUtil::CharacterIsDigit);
	if (literal1 == end) {
		return false;
	}

	//	second numeric field must exist
	auto field2 = std::find_if(literal1, end, StringUtil::CharacterIsDigit);
	if (field2 == end) {
		return false;
	}
	auto literal2 = std::find_if_not(field2, end, StringUtil::CharacterIsDigit);
	if (literal2 == end) {
		return false;
	}

	//	third numeric field must exist
	auto field3 = std::find_if(literal2, end, StringUtil::CharacterIsDigit);
	if (field3 == end) {
		return false;
	}

	//	second literal must match first
	if (((field3 - literal2) != (field2 - literal1)) || strncmp(literal1, literal2, (field2 - literal1)) != 0) {
		return false;
	}

	//	copy the literal as the separator, escaping percent signs
	separator.clear();
	while (literal1 < field2) {
		const auto literal_char = *literal1++;
		if (literal_char == '%') {
			separator.push_back(literal_char);
		}
		separator.push_back(literal_char);
	}

	return true;
}

string GenerateDateFormat(const string &separator, const char *format_template) {
	string format_specifier = format_template;

	//	replace all dashes with the separator
	for (auto pos = std::find(format_specifier.begin(), format_specifier.end(), '-'); pos != format_specifier.end();
	     pos = std::find(pos + separator.size(), format_specifier.end(), '-')) {
		format_specifier.replace(pos, pos + 1, separator);
	}

	return format_specifier;
}

TextSearchShiftArray::TextSearchShiftArray() {
}

TextSearchShiftArray::TextSearchShiftArray(string search_term) : length(search_term.size()) {
	if (length > 255) {
		throw Exception("Size of delimiter/quote/escape in CSV reader is limited to 255 bytes");
	}
	// initialize the shifts array
	shifts = unique_ptr<uint8_t[]>(new uint8_t[length * 255]);
	memset(shifts.get(), 0, length * 255 * sizeof(uint8_t));
	// iterate over each of the characters in the array
	for (idx_t main_idx = 0; main_idx < length; main_idx++) {
		uint8_t current_char = (uint8_t)search_term[main_idx];
		// now move over all the remaining positions
		for (idx_t i = main_idx; i < length; i++) {
			bool is_match = true;
			// check if the prefix matches at this position
			// if it does, we move to this position after encountering the current character
			for (idx_t j = 0; j < main_idx; j++) {
				if (search_term[i - main_idx + j] != search_term[j]) {
					is_match = false;
				}
			}
			if (!is_match) {
				continue;
			}
			shifts[i * 255 + current_char] = main_idx + 1;
		}
	}
}

BufferedCSVReader::BufferedCSVReader(FileSystem &fs_p, Allocator &allocator, FileOpener *opener_p,
                                     BufferedCSVReaderOptions options_p, const vector<LogicalType> &requested_types)
    : fs(fs_p), allocator(allocator), opener(opener_p), options(move(options_p)), buffer_size(0), position(0),
      start(0) {
	file_handle = OpenCSV(options);
	Initialize(requested_types);
}

BufferedCSVReader::BufferedCSVReader(ClientContext &context, BufferedCSVReaderOptions options_p,
                                     const vector<LogicalType> &requested_types)
    : BufferedCSVReader(FileSystem::GetFileSystem(context), Allocator::Get(context), FileSystem::GetFileOpener(context),
                        move(options_p), requested_types) {
}

BufferedCSVReader::~BufferedCSVReader() {
}

idx_t BufferedCSVReader::GetFileSize() {
	return file_handle ? file_handle->FileSize() : 0;
}

void BufferedCSVReader::Initialize(const vector<LogicalType> &requested_types) {
	PrepareComplexParser();
	if (options.auto_detect) {
		sql_types = SniffCSV(requested_types);
		if (sql_types.empty()) {
			throw Exception("Failed to detect column types from CSV: is the file a valid CSV file?");
		}
		if (cached_chunks.empty()) {
			JumpToBeginning(options.skip_rows, options.header);
		}
	} else {
		sql_types = requested_types;
		ResetBuffer();
		SkipRowsAndReadHeader(options.skip_rows, options.header);
	}
	InitParseChunk(sql_types.size());
	// we only need reset support during the automatic CSV type detection
	// since reset support might require caching (in the case of streams), we disable it for the remainder
	file_handle->DisableReset();
}

void BufferedCSVReader::PrepareComplexParser() {
	delimiter_search = TextSearchShiftArray(options.delimiter);
	escape_search = TextSearchShiftArray(options.escape);
	quote_search = TextSearchShiftArray(options.quote);
}

unique_ptr<CSVFileHandle> BufferedCSVReader::OpenCSV(const BufferedCSVReaderOptions &options) {
	auto file_handle = fs.OpenFile(options.file_path.c_str(), FileFlags::FILE_FLAGS_READ, FileLockType::NO_LOCK,
	                               options.compression, this->opener);
	return make_unique<CSVFileHandle>(move(file_handle));
}

// Helper function to generate column names
static string GenerateColumnName(const idx_t total_cols, const idx_t col_number, const string &prefix = "column") {
	int max_digits = NumericHelper::UnsignedLength(total_cols - 1);
	int digits = NumericHelper::UnsignedLength(col_number);
	string leading_zeros = string(max_digits - digits, '0');
	string value = to_string(col_number);
	return string(prefix + leading_zeros + value);
}

// Helper function for UTF-8 aware space trimming
static string TrimWhitespace(const string &col_name) {
	utf8proc_int32_t codepoint;
	auto str = reinterpret_cast<const utf8proc_uint8_t *>(col_name.c_str());
	idx_t size = col_name.size();
	// Find the first character that is not left trimmed
	idx_t begin = 0;
	while (begin < size) {
		auto bytes = utf8proc_iterate(str + begin, size - begin, &codepoint);
		D_ASSERT(bytes > 0);
		if (utf8proc_category(codepoint) != UTF8PROC_CATEGORY_ZS) {
			break;
		}
		begin += bytes;
	}

	// Find the last character that is not right trimmed
	idx_t end;
	end = begin;
	for (auto next = begin; next < col_name.size();) {
		auto bytes = utf8proc_iterate(str + next, size - next, &codepoint);
		D_ASSERT(bytes > 0);
		next += bytes;
		if (utf8proc_category(codepoint) != UTF8PROC_CATEGORY_ZS) {
			end = next;
		}
	}

	// return the trimmed string
	return col_name.substr(begin, end - begin);
}

static string NormalizeColumnName(const string &col_name) {
	// normalize UTF8 characters to NFKD
	auto nfkd = utf8proc_NFKD((const utf8proc_uint8_t *)col_name.c_str(), col_name.size());
	const string col_name_nfkd = string((const char *)nfkd, strlen((const char *)nfkd));
	free(nfkd);

	// only keep ASCII characters 0-9 a-z A-Z and replace spaces with regular whitespace
	string col_name_ascii = "";
	for (idx_t i = 0; i < col_name_nfkd.size(); i++) {
		if (col_name_nfkd[i] == '_' || (col_name_nfkd[i] >= '0' && col_name_nfkd[i] <= '9') ||
		    (col_name_nfkd[i] >= 'A' && col_name_nfkd[i] <= 'Z') ||
		    (col_name_nfkd[i] >= 'a' && col_name_nfkd[i] <= 'z')) {
			col_name_ascii += col_name_nfkd[i];
		} else if (StringUtil::CharacterIsSpace(col_name_nfkd[i])) {
			col_name_ascii += " ";
		}
	}

	// trim whitespace and replace remaining whitespace by _
	string col_name_trimmed = TrimWhitespace(col_name_ascii);
	string col_name_cleaned = "";
	bool in_whitespace = false;
	for (idx_t i = 0; i < col_name_trimmed.size(); i++) {
		if (col_name_trimmed[i] == ' ') {
			if (!in_whitespace) {
				col_name_cleaned += "_";
				in_whitespace = true;
			}
		} else {
			col_name_cleaned += col_name_trimmed[i];
			in_whitespace = false;
		}
	}

	// don't leave string empty; if not empty, make lowercase
	if (col_name_cleaned.empty()) {
		col_name_cleaned = "_";
	} else {
		col_name_cleaned = StringUtil::Lower(col_name_cleaned);
	}

	// prepend _ if name starts with a digit or is a reserved keyword
	if (KeywordHelper::IsKeyword(col_name_cleaned) || (col_name_cleaned[0] >= '0' && col_name_cleaned[0] <= '9')) {
		col_name_cleaned = "_" + col_name_cleaned;
	}
	return col_name_cleaned;
}

void BufferedCSVReader::ResetBuffer() {
	buffer.reset();
	buffer_size = 0;
	position = 0;
	start = 0;
	cached_buffers.clear();
}

void BufferedCSVReader::ResetStream() {
	if (!file_handle->CanSeek()) {
		// seeking to the beginning appears to not be supported in all compiler/os-scenarios,
		// so we have to create a new stream source here for now
		file_handle->Reset();
	} else {
		file_handle->Seek(0);
	}
	linenr = 0;
	linenr_estimated = false;
	bytes_per_line_avg = 0;
	sample_chunk_idx = 0;
	jumping_samples = false;
}

void BufferedCSVReader::InitParseChunk(idx_t num_cols) {
	// adapt not null info
	if (options.force_not_null.size() != num_cols) {
		options.force_not_null.resize(num_cols, false);
	}
	if (num_cols == parse_chunk.ColumnCount()) {
		parse_chunk.Reset();
	} else {
		parse_chunk.Destroy();

		// initialize the parse_chunk with a set of VARCHAR types
		vector<LogicalType> varchar_types(num_cols, LogicalType::VARCHAR);
		parse_chunk.Initialize(allocator, varchar_types);
	}
}

void BufferedCSVReader::JumpToBeginning(idx_t skip_rows = 0, bool skip_header = false) {
	ResetBuffer();
	ResetStream();
	sample_chunk_idx = 0;
	bytes_in_chunk = 0;
	end_of_file_reached = false;
	bom_checked = false;
	SkipRowsAndReadHeader(skip_rows, skip_header);
}

void BufferedCSVReader::SkipRowsAndReadHeader(idx_t skip_rows, bool skip_header) {
	for (idx_t i = 0; i < skip_rows; i++) {
		// ignore skip rows
		string read_line = file_handle->ReadLine();
		linenr++;
	}

	if (skip_header) {
		// ignore the first line as a header line
		InitParseChunk(sql_types.size());
		ParseCSV(ParserMode::PARSING_HEADER);
	}
}

bool BufferedCSVReader::JumpToNextSample() {
	// get bytes contained in the previously read chunk
	idx_t remaining_bytes_in_buffer = buffer_size - start;
	bytes_in_chunk -= remaining_bytes_in_buffer;
	if (remaining_bytes_in_buffer == 0) {
		return false;
	}

	// assess if it makes sense to jump, based on size of the first chunk relative to size of the entire file
	if (sample_chunk_idx == 0) {
		idx_t bytes_first_chunk = bytes_in_chunk;
		double chunks_fit = (file_handle->FileSize() / (double)bytes_first_chunk);
		jumping_samples = chunks_fit >= options.sample_chunks;

		// jump back to the beginning
		JumpToBeginning(options.skip_rows, options.header);
		sample_chunk_idx++;
		return true;
	}

	if (end_of_file_reached || sample_chunk_idx >= options.sample_chunks) {
		return false;
	}

	// if we deal with any other sources than plaintext files, jumping_samples can be tricky. In that case
	// we just read x continuous chunks from the stream TODO: make jumps possible for zipfiles.
	if (!file_handle->PlainFileSource() || !jumping_samples) {
		sample_chunk_idx++;
		return true;
	}

	// update average bytes per line
	double bytes_per_line = bytes_in_chunk / (double)options.sample_chunk_size;
	bytes_per_line_avg = ((bytes_per_line_avg * (sample_chunk_idx)) + bytes_per_line) / (sample_chunk_idx + 1);

	// if none of the previous conditions were met, we can jump
	idx_t partition_size = (idx_t)round(file_handle->FileSize() / (double)options.sample_chunks);

	// calculate offset to end of the current partition
	int64_t offset = partition_size - bytes_in_chunk - remaining_bytes_in_buffer;
	auto current_pos = file_handle->SeekPosition();

	if (current_pos + offset < file_handle->FileSize()) {
		// set position in stream and clear failure bits
		file_handle->Seek(current_pos + offset);

		// estimate linenr
		linenr += (idx_t)round((offset + remaining_bytes_in_buffer) / bytes_per_line_avg);
		linenr_estimated = true;
	} else {
		// seek backwards from the end in last chunk and hope to catch the end of the file
		// TODO: actually it would be good to make sure that the end of file is being reached, because
		// messy end-lines are quite common. For this case, however, we first need a skip_end detection anyways.
		file_handle->Seek(file_handle->FileSize() - bytes_in_chunk);

		// estimate linenr
		linenr = (idx_t)round((file_handle->FileSize() - bytes_in_chunk) / bytes_per_line_avg);
		linenr_estimated = true;
	}

	// reset buffers and parse chunk
	ResetBuffer();

	// seek beginning of next line
	// FIXME: if this jump ends up in a quoted linebreak, we will have a problem
	string read_line = file_handle->ReadLine();
	linenr++;

	sample_chunk_idx++;

	return true;
}

void BufferedCSVReader::SetDateFormat(const string &format_specifier, const LogicalTypeId &sql_type) {
	options.has_format[sql_type] = true;
	auto &date_format = options.date_format[sql_type];
	date_format.format_specifier = format_specifier;
	StrTimeFormat::ParseFormatSpecifier(date_format.format_specifier, date_format);
}

bool BufferedCSVReader::TryCastValue(const Value &value, const LogicalType &sql_type) {
	if (options.has_format[LogicalTypeId::DATE] && sql_type.id() == LogicalTypeId::DATE) {
		date_t result;
		string error_message;
		return options.date_format[LogicalTypeId::DATE].TryParseDate(string_t(StringValue::Get(value)), result,
		                                                             error_message);
	} else if (options.has_format[LogicalTypeId::TIMESTAMP] && sql_type.id() == LogicalTypeId::TIMESTAMP) {
		timestamp_t result;
		string error_message;
		return options.date_format[LogicalTypeId::TIMESTAMP].TryParseTimestamp(string_t(StringValue::Get(value)),
		                                                                       result, error_message);
	} else {
		Value new_value;
		string error_message;
		return value.TryCastAs(sql_type, new_value, &error_message, true);
	}
}

struct TryCastDateOperator {
	static bool Operation(BufferedCSVReaderOptions &options, string_t input, date_t &result, string &error_message) {
		return options.date_format[LogicalTypeId::DATE].TryParseDate(input, result, error_message);
	}
};

struct TryCastTimestampOperator {
	static bool Operation(BufferedCSVReaderOptions &options, string_t input, timestamp_t &result,
	                      string &error_message) {
		return options.date_format[LogicalTypeId::TIMESTAMP].TryParseTimestamp(input, result, error_message);
	}
};

template <class OP, class T>
static bool TemplatedTryCastDateVector(BufferedCSVReaderOptions &options, Vector &input_vector, Vector &result_vector,
                                       idx_t count, string &error_message) {
	D_ASSERT(input_vector.GetType().id() == LogicalTypeId::VARCHAR);
	bool all_converted = true;
	UnaryExecutor::Execute<string_t, T>(input_vector, result_vector, count, [&](string_t input) {
		T result;
		if (!OP::Operation(options, input, result, error_message)) {
			all_converted = false;
		}
		return result;
	});
	return all_converted;
}

bool TryCastDateVector(BufferedCSVReaderOptions &options, Vector &input_vector, Vector &result_vector, idx_t count,
                       string &error_message) {
	return TemplatedTryCastDateVector<TryCastDateOperator, date_t>(options, input_vector, result_vector, count,
	                                                               error_message);
}

bool TryCastTimestampVector(BufferedCSVReaderOptions &options, Vector &input_vector, Vector &result_vector, idx_t count,
                            string &error_message) {
	return TemplatedTryCastDateVector<TryCastTimestampOperator, timestamp_t>(options, input_vector, result_vector,
	                                                                         count, error_message);
}

bool BufferedCSVReader::TryCastVector(Vector &parse_chunk_col, idx_t size, const LogicalType &sql_type) {
	// try vector-cast from string to sql_type
	Vector dummy_result(sql_type);
	if (options.has_format[LogicalTypeId::DATE] && sql_type == LogicalTypeId::DATE) {
		// use the date format to cast the chunk
		string error_message;
		return TryCastDateVector(options, parse_chunk_col, dummy_result, size, error_message);
	} else if (options.has_format[LogicalTypeId::TIMESTAMP] && sql_type == LogicalTypeId::TIMESTAMP) {
		// use the timestamp format to cast the chunk
		string error_message;
		return TryCastTimestampVector(options, parse_chunk_col, dummy_result, size, error_message);
	} else {
		// target type is not varchar: perform a cast
		string error_message;
		return VectorOperations::TryCast(parse_chunk_col, dummy_result, size, &error_message, true);
	}
}

enum class QuoteRule : uint8_t { QUOTES_RFC = 0, QUOTES_OTHER = 1, NO_QUOTES = 2 };

void BufferedCSVReader::DetectDialect(const vector<LogicalType> &requested_types,
                                      BufferedCSVReaderOptions &original_options,
                                      vector<BufferedCSVReaderOptions> &info_candidates, idx_t &best_num_cols) {
	// set up the candidates we consider for delimiter and quote rules based on user input
	vector<string> delim_candidates;
	vector<QuoteRule> quoterule_candidates;
	vector<vector<string>> quote_candidates_map;
	vector<vector<string>> escape_candidates_map = {{""}, {"\\"}, {""}};

	if (options.has_delimiter) {
		// user provided a delimiter: use that delimiter
		delim_candidates = {options.delimiter};
	} else {
		// no delimiter provided: try standard/common delimiters
		delim_candidates = {",", "|", ";", "\t"};
	}
	if (options.has_quote) {
		// user provided quote: use that quote rule
		quote_candidates_map = {{options.quote}, {options.quote}, {options.quote}};
	} else {
		// no quote rule provided: use standard/common quotes
		quote_candidates_map = {{"\""}, {"\"", "'"}, {""}};
	}
	if (options.has_escape) {
		// user provided escape: use that escape rule
		if (options.escape.empty()) {
			quoterule_candidates = {QuoteRule::QUOTES_RFC};
		} else {
			quoterule_candidates = {QuoteRule::QUOTES_OTHER};
		}
		escape_candidates_map[static_cast<uint8_t>(quoterule_candidates[0])] = {options.escape};
	} else {
		// no escape provided: try standard/common escapes
		quoterule_candidates = {QuoteRule::QUOTES_RFC, QuoteRule::QUOTES_OTHER, QuoteRule::NO_QUOTES};
	}

	idx_t best_consistent_rows = 0;
	for (auto quoterule : quoterule_candidates) {
		const auto &quote_candidates = quote_candidates_map[static_cast<uint8_t>(quoterule)];
		for (const auto &quote : quote_candidates) {
			for (const auto &delim : delim_candidates) {
				const auto &escape_candidates = escape_candidates_map[static_cast<uint8_t>(quoterule)];
				for (const auto &escape : escape_candidates) {
					BufferedCSVReaderOptions sniff_info = original_options;
					sniff_info.delimiter = delim;
					sniff_info.quote = quote;
					sniff_info.escape = escape;

					options = sniff_info;
					PrepareComplexParser();

					JumpToBeginning(original_options.skip_rows);
					sniffed_column_counts.clear();

					if (!TryParseCSV(ParserMode::SNIFFING_DIALECT)) {
						continue;
					}

					idx_t start_row = original_options.skip_rows;
					idx_t consistent_rows = 0;
					idx_t num_cols = 0;

					for (idx_t row = 0; row < sniffed_column_counts.size(); row++) {
						if (sniffed_column_counts[row] == num_cols) {
							consistent_rows++;
						} else {
							num_cols = sniffed_column_counts[row];
							start_row = row + original_options.skip_rows;
							consistent_rows = 1;
						}
					}

					// some logic
					bool more_values = (consistent_rows > best_consistent_rows && num_cols >= best_num_cols);
					bool single_column_before = best_num_cols < 2 && num_cols > best_num_cols;
					bool rows_consistent =
					    start_row + consistent_rows - original_options.skip_rows == sniffed_column_counts.size();
					bool more_than_one_row = (consistent_rows > 1);
					bool more_than_one_column = (num_cols > 1);
					bool start_good = !info_candidates.empty() && (start_row <= info_candidates.front().skip_rows);

					if (!requested_types.empty() && requested_types.size() != num_cols) {
						continue;
					} else if ((more_values || single_column_before) && rows_consistent) {
						sniff_info.skip_rows = start_row;
						sniff_info.num_cols = num_cols;
						best_consistent_rows = consistent_rows;
						best_num_cols = num_cols;

						info_candidates.clear();
						info_candidates.push_back(sniff_info);
					} else if (more_than_one_row && more_than_one_column && start_good && rows_consistent) {
						bool same_quote_is_candidate = false;
						for (auto &info_candidate : info_candidates) {
							if (quote.compare(info_candidate.quote) == 0) {
								same_quote_is_candidate = true;
							}
						}
						if (!same_quote_is_candidate) {
							sniff_info.skip_rows = start_row;
							sniff_info.num_cols = num_cols;
							info_candidates.push_back(sniff_info);
						}
					}
				}
			}
		}
	}
}

void BufferedCSVReader::DetectCandidateTypes(const vector<LogicalType> &type_candidates,
                                             const map<LogicalTypeId, vector<const char *>> &format_template_candidates,
                                             const vector<BufferedCSVReaderOptions> &info_candidates,
                                             BufferedCSVReaderOptions &original_options, idx_t best_num_cols,
                                             vector<vector<LogicalType>> &best_sql_types_candidates,
                                             std::map<LogicalTypeId, vector<string>> &best_format_candidates,
                                             DataChunk &best_header_row) {
	BufferedCSVReaderOptions best_options;
	idx_t min_varchar_cols = best_num_cols + 1;

	// check which info candidate leads to minimum amount of non-varchar columns...
	for (const auto &t : format_template_candidates) {
		best_format_candidates[t.first].clear();
	}
	for (auto &info_candidate : info_candidates) {
		options = info_candidate;
		vector<vector<LogicalType>> info_sql_types_candidates(options.num_cols, type_candidates);
		std::map<LogicalTypeId, bool> has_format_candidates;
		std::map<LogicalTypeId, vector<string>> format_candidates;
		for (const auto &t : format_template_candidates) {
			has_format_candidates[t.first] = false;
			format_candidates[t.first].clear();
		}

		// set all sql_types to VARCHAR so we can do datatype detection based on VARCHAR values
		sql_types.clear();
		sql_types.assign(options.num_cols, LogicalType::VARCHAR);

		// jump to beginning and skip potential header
		JumpToBeginning(options.skip_rows, true);
		DataChunk header_row;
		header_row.Initialize(allocator, sql_types);
		parse_chunk.Copy(header_row);

		if (header_row.size() == 0) {
			continue;
		}

		// init parse chunk and read csv with info candidate
		InitParseChunk(sql_types.size());
		ParseCSV(ParserMode::SNIFFING_DATATYPES);
		for (idx_t row_idx = 0; row_idx <= parse_chunk.size(); row_idx++) {
			bool is_header_row = row_idx == 0;
			idx_t row = row_idx - 1;
			for (idx_t col = 0; col < parse_chunk.ColumnCount(); col++) {
				auto &col_type_candidates = info_sql_types_candidates[col];
				while (col_type_candidates.size() > 1) {
					const auto &sql_type = col_type_candidates.back();
					// try cast from string to sql_type
					Value dummy_val;
					if (is_header_row) {
						dummy_val = header_row.GetValue(col, 0);
					} else {
						dummy_val = parse_chunk.GetValue(col, row);
					}
					// try formatting for date types if the user did not specify one and it starts with numeric values.
					string separator;
					if (has_format_candidates.count(sql_type.id()) && !original_options.has_format[sql_type.id()] &&
					    StartsWithNumericDate(separator, StringValue::Get(dummy_val))) {
						// generate date format candidates the first time through
						auto &type_format_candidates = format_candidates[sql_type.id()];
						const auto had_format_candidates = has_format_candidates[sql_type.id()];
						if (!has_format_candidates[sql_type.id()]) {
							has_format_candidates[sql_type.id()] = true;
							// order by preference
							auto entry = format_template_candidates.find(sql_type.id());
							if (entry != format_template_candidates.end()) {
								const auto &format_template_list = entry->second;
								for (const auto &t : format_template_list) {
									const auto format_string = GenerateDateFormat(separator, t);
									// don't parse ISO 8601
									if (format_string.find("%Y-%m-%d") == string::npos) {
										type_format_candidates.emplace_back(format_string);
									}
								}
							}
							//	initialise the first candidate
							options.has_format[sql_type.id()] = true;
							//	all formats are constructed to be valid
							SetDateFormat(type_format_candidates.back(), sql_type.id());
						}
						// check all formats and keep the first one that works
						StrpTimeFormat::ParseResult result;
						auto save_format_candidates = type_format_candidates;
						while (!type_format_candidates.empty()) {
							//	avoid using exceptions for flow control...
							auto &current_format = options.date_format[sql_type.id()];
							if (current_format.Parse(StringValue::Get(dummy_val), result)) {
								break;
							}
							//	doesn't work - move to the next one
							type_format_candidates.pop_back();
							options.has_format[sql_type.id()] = (!type_format_candidates.empty());
							if (!type_format_candidates.empty()) {
								SetDateFormat(type_format_candidates.back(), sql_type.id());
							}
						}
						//	if none match, then this is not a value of type sql_type,
						if (type_format_candidates.empty()) {
							//	so restore the candidates that did work.
							//	or throw them out if they were generated by this value.
							if (had_format_candidates) {
								type_format_candidates.swap(save_format_candidates);
								if (!type_format_candidates.empty()) {
									SetDateFormat(type_format_candidates.back(), sql_type.id());
								}
							} else {
								has_format_candidates[sql_type.id()] = false;
							}
						}
					}
					// try cast from string to sql_type
					if (TryCastValue(dummy_val, sql_type)) {
						break;
					} else {
						col_type_candidates.pop_back();
					}
				}
			}
			// reset type detection, because first row could be header,
			// but only do it if csv has more than one line (including header)
			if (parse_chunk.size() > 0 && is_header_row) {
				info_sql_types_candidates = vector<vector<LogicalType>>(options.num_cols, type_candidates);
				for (auto &f : format_candidates) {
					f.second.clear();
				}
				for (auto &h : has_format_candidates) {
					h.second = false;
				}
			}
		}

		idx_t varchar_cols = 0;
		for (idx_t col = 0; col < parse_chunk.ColumnCount(); col++) {
			auto &col_type_candidates = info_sql_types_candidates[col];
			// check number of varchar columns
			const auto &col_type = col_type_candidates.back();
			if (col_type == LogicalType::VARCHAR) {
				varchar_cols++;
			}
		}

		// it's good if the dialect creates more non-varchar columns, but only if we sacrifice < 30% of best_num_cols.
		if (varchar_cols < min_varchar_cols && parse_chunk.ColumnCount() > (best_num_cols * 0.7)) {
			// we have a new best_options candidate
			best_options = info_candidate;
			min_varchar_cols = varchar_cols;
			best_sql_types_candidates = info_sql_types_candidates;
			best_format_candidates = format_candidates;
			best_header_row.Destroy();
			auto header_row_types = header_row.GetTypes();
			best_header_row.Initialize(allocator, header_row_types);
			header_row.Copy(best_header_row);
		}
	}

	options = best_options;
	for (const auto &best : best_format_candidates) {
		if (!best.second.empty()) {
			SetDateFormat(best.second.back(), best.first);
		}
	}
}

void BufferedCSVReader::DetectHeader(const vector<vector<LogicalType>> &best_sql_types_candidates,
                                     const DataChunk &best_header_row) {
	// information for header detection
	bool first_row_consistent = true;
	bool first_row_nulls = false;

	// check if header row is all null and/or consistent with detected column data types
	first_row_nulls = true;
	for (idx_t col = 0; col < best_sql_types_candidates.size(); col++) {
		auto dummy_val = best_header_row.GetValue(col, 0);
		if (!dummy_val.IsNull()) {
			first_row_nulls = false;
		}

		// try cast to sql_type of column
		const auto &sql_type = best_sql_types_candidates[col].back();
		if (!TryCastValue(dummy_val, sql_type)) {
			first_row_consistent = false;
		}
	}

	// update parser info, and read, generate & set col_names based on previous findings
	if (((!first_row_consistent || first_row_nulls) && !options.has_header) || (options.has_header && options.header)) {
		options.header = true;
		case_insensitive_map_t<idx_t> name_collision_count;
		// get header names from CSV
		for (idx_t col = 0; col < options.num_cols; col++) {
			const auto &val = best_header_row.GetValue(col, 0);
			string col_name = val.ToString();

			// generate name if field is empty
			if (col_name.empty() || val.IsNull()) {
				col_name = GenerateColumnName(options.num_cols, col);
			}

			// normalize names or at least trim whitespace
			if (options.normalize_names) {
				col_name = NormalizeColumnName(col_name);
			} else {
				col_name = TrimWhitespace(col_name);
			}

			// avoid duplicate header names
			const string col_name_raw = col_name;
			while (name_collision_count.find(col_name) != name_collision_count.end()) {
				name_collision_count[col_name] += 1;
				col_name = col_name + "_" + to_string(name_collision_count[col_name]);
			}

			col_names.push_back(col_name);
			name_collision_count[col_name] = 0;
		}

	} else {
		options.header = false;
		for (idx_t col = 0; col < options.num_cols; col++) {
			string column_name = GenerateColumnName(options.num_cols, col);
			col_names.push_back(column_name);
		}
	}
}

vector<LogicalType> BufferedCSVReader::RefineTypeDetection(const vector<LogicalType> &type_candidates,
                                                           const vector<LogicalType> &requested_types,
                                                           vector<vector<LogicalType>> &best_sql_types_candidates,
                                                           map<LogicalTypeId, vector<string>> &best_format_candidates) {
	// for the type refine we set the SQL types to VARCHAR for all columns
	sql_types.clear();
	sql_types.assign(options.num_cols, LogicalType::VARCHAR);

	vector<LogicalType> detected_types;

	// if data types were provided, exit here if number of columns does not match
	if (!requested_types.empty()) {
		if (requested_types.size() != options.num_cols) {
			throw InvalidInputException(
			    "Error while determining column types: found %lld columns but expected %d. (%s)", options.num_cols,
			    requested_types.size(), options.ToString());
		} else {
			detected_types = requested_types;
		}
	} else if (options.all_varchar) {
		// return all types varchar
		detected_types = sql_types;
	} else {
		// jump through the rest of the file and continue to refine the sql type guess
		while (JumpToNextSample()) {
			InitParseChunk(sql_types.size());
			// if jump ends up a bad line, we just skip this chunk
			if (!TryParseCSV(ParserMode::SNIFFING_DATATYPES)) {
				continue;
			}
			for (idx_t col = 0; col < parse_chunk.ColumnCount(); col++) {
				vector<LogicalType> &col_type_candidates = best_sql_types_candidates[col];
				while (col_type_candidates.size() > 1) {
					const auto &sql_type = col_type_candidates.back();
					//	narrow down the date formats
					if (best_format_candidates.count(sql_type.id())) {
						auto &best_type_format_candidates = best_format_candidates[sql_type.id()];
						auto save_format_candidates = best_type_format_candidates;
						while (!best_type_format_candidates.empty()) {
							if (TryCastVector(parse_chunk.data[col], parse_chunk.size(), sql_type)) {
								break;
							}
							//	doesn't work - move to the next one
							best_type_format_candidates.pop_back();
							options.has_format[sql_type.id()] = (!best_type_format_candidates.empty());
							if (!best_type_format_candidates.empty()) {
								SetDateFormat(best_type_format_candidates.back(), sql_type.id());
							}
						}
						//	if none match, then this is not a column of type sql_type,
						if (best_type_format_candidates.empty()) {
							//	so restore the candidates that did work.
							best_type_format_candidates.swap(save_format_candidates);
							if (!best_type_format_candidates.empty()) {
								SetDateFormat(best_type_format_candidates.back(), sql_type.id());
							}
						}
					}

					if (TryCastVector(parse_chunk.data[col], parse_chunk.size(), sql_type)) {
						break;
					} else {
						col_type_candidates.pop_back();
					}
				}
			}

			if (!jumping_samples) {
				if ((sample_chunk_idx)*options.sample_chunk_size <= options.buffer_size) {
					// cache parse chunk
					// create a new chunk and fill it with the remainder
					auto chunk = make_unique<DataChunk>();
					auto parse_chunk_types = parse_chunk.GetTypes();
					chunk->Move(parse_chunk);
					cached_chunks.push(move(chunk));
				} else {
					while (!cached_chunks.empty()) {
						cached_chunks.pop();
					}
				}
			}
		}

		// set sql types
		for (auto &best_sql_types_candidate : best_sql_types_candidates) {
			LogicalType d_type = best_sql_types_candidate.back();
			if (best_sql_types_candidate.size() == type_candidates.size()) {
				d_type = LogicalType::VARCHAR;
			}
			detected_types.push_back(d_type);
		}
	}

	return detected_types;
}

vector<LogicalType> BufferedCSVReader::SniffCSV(const vector<LogicalType> &requested_types) {
	for (auto &type : requested_types) {
		// auto detect for blobs not supported: there may be invalid UTF-8 in the file
		if (type.id() == LogicalTypeId::BLOB) {
			return requested_types;
		}
	}

	// #######
	// ### dialect detection
	// #######
	BufferedCSVReaderOptions original_options = options;
	vector<BufferedCSVReaderOptions> info_candidates;
	idx_t best_num_cols = 0;

	DetectDialect(requested_types, original_options, info_candidates, best_num_cols);

	// if no dialect candidate was found, then file was most likely empty and we throw an exception
	if (info_candidates.empty()) {
		throw InvalidInputException(
		    "Error in file \"%s\": CSV options could not be auto-detected. Consider setting parser options manually.",
		    options.file_path);
	}

	// #######
	// ### type detection (initial)
	// #######
	// type candidates, ordered by descending specificity (~ from high to low)
	vector<LogicalType> type_candidates = {
	    LogicalType::VARCHAR, LogicalType::TIMESTAMP,
	    LogicalType::DATE,    LogicalType::TIME,
	    LogicalType::DOUBLE,  /* LogicalType::FLOAT,*/ LogicalType::BIGINT,
	    LogicalType::INTEGER, /*LogicalType::SMALLINT, LogicalType::TINYINT,*/ LogicalType::BOOLEAN,
	    LogicalType::SQLNULL};
	// format template candidates, ordered by descending specificity (~ from high to low)
	std::map<LogicalTypeId, vector<const char *>> format_template_candidates = {
	    {LogicalTypeId::DATE, {"%m-%d-%Y", "%m-%d-%y", "%d-%m-%Y", "%d-%m-%y", "%Y-%m-%d", "%y-%m-%d"}},
	    {LogicalTypeId::TIMESTAMP,
	     {"%Y-%m-%d %H:%M:%S.%f", "%m-%d-%Y %I:%M:%S %p", "%m-%d-%y %I:%M:%S %p", "%d-%m-%Y %H:%M:%S",
	      "%d-%m-%y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%y-%m-%d %H:%M:%S"}},
	};
	vector<vector<LogicalType>> best_sql_types_candidates;
	map<LogicalTypeId, vector<string>> best_format_candidates;
	DataChunk best_header_row;
	DetectCandidateTypes(type_candidates, format_template_candidates, info_candidates, original_options, best_num_cols,
	                     best_sql_types_candidates, best_format_candidates, best_header_row);

	// #######
	// ### header detection
	// #######
	options.num_cols = best_num_cols;
	DetectHeader(best_sql_types_candidates, best_header_row);

	// #######
	// ### type detection (refining)
	// #######
	return RefineTypeDetection(type_candidates, requested_types, best_sql_types_candidates, best_format_candidates);
}

bool BufferedCSVReader::TryParseComplexCSV(DataChunk &insert_chunk, string &error_message) {
	// used for parsing algorithm
	bool finished_chunk = false;
	idx_t column = 0;
	vector<idx_t> escape_positions;
	uint8_t delimiter_pos = 0, escape_pos = 0, quote_pos = 0;
	idx_t offset = 0;

	// read values into the buffer (if any)
	if (position >= buffer_size) {
		if (!ReadBuffer(start)) {
			return true;
		}
	}
	// start parsing the first value
	start = position;
	goto value_start;
value_start:
	/* state: value_start */
	// this state parses the first characters of a value
	offset = 0;
	delimiter_pos = 0;
	quote_pos = 0;
	do {
		idx_t count = 0;
		for (; position < buffer_size; position++) {
			quote_search.Match(quote_pos, buffer[position]);
			delimiter_search.Match(delimiter_pos, buffer[position]);
			count++;
			if (delimiter_pos == options.delimiter.size()) {
				// found a delimiter, add the value
				offset = options.delimiter.size() - 1;
				goto add_value;
			} else if (StringUtil::CharacterIsNewline(buffer[position])) {
				// found a newline, add the row
				goto add_row;
			}
			if (count > quote_pos) {
				// did not find a quote directly at the start of the value, stop looking for the quote now
				goto normal;
			}
			if (quote_pos == options.quote.size()) {
				// found a quote, go to quoted loop and skip the initial quote
				start += options.quote.size();
				goto in_quotes;
			}
		}
	} while (ReadBuffer(start));
	// file ends while scanning for quote/delimiter, go to final state
	goto final_state;
normal:
	/* state: normal parsing state */
	// this state parses the remainder of a non-quoted value until we reach a delimiter or newline
	position++;
	do {
		for (; position < buffer_size; position++) {
			delimiter_search.Match(delimiter_pos, buffer[position]);
			if (delimiter_pos == options.delimiter.size()) {
				offset = options.delimiter.size() - 1;
				goto add_value;
			} else if (StringUtil::CharacterIsNewline(buffer[position])) {
				goto add_row;
			}
		}
	} while (ReadBuffer(start));
	goto final_state;
add_value:
	AddValue(buffer.get() + start, position - start - offset, column, escape_positions);
	// increase position by 1 and move start to the new position
	offset = 0;
	start = ++position;
	if (position >= buffer_size && !ReadBuffer(start)) {
		// file ends right after delimiter, go to final state
		goto final_state;
	}
	goto value_start;
add_row : {
	// check type of newline (\r or \n)
	bool carriage_return = buffer[position] == '\r';
	AddValue(buffer.get() + start, position - start - offset, column, escape_positions);
	finished_chunk = AddRow(insert_chunk, column);
	// increase position by 1 and move start to the new position
	offset = 0;
	start = ++position;
	if (position >= buffer_size && !ReadBuffer(start)) {
		// file ends right after newline, go to final state
		goto final_state;
	}
	if (carriage_return) {
		// \r newline, go to special state that parses an optional \n afterwards
		goto carriage_return;
	} else {
		// \n newline, move to value start
		if (finished_chunk) {
			return true;
		}
		goto value_start;
	}
}
in_quotes:
	/* state: in_quotes */
	// this state parses the remainder of a quoted value
	quote_pos = 0;
	escape_pos = 0;
	position++;
	do {
		for (; position < buffer_size; position++) {
			quote_search.Match(quote_pos, buffer[position]);
			escape_search.Match(escape_pos, buffer[position]);
			if (quote_pos == options.quote.size()) {
				goto unquote;
			} else if (escape_pos == options.escape.size()) {
				escape_positions.push_back(position - start - (options.escape.size() - 1));
				goto handle_escape;
			}
		}
	} while (ReadBuffer(start));
	// still in quoted state at the end of the file, error:
	error_message = StringUtil::Format("Error in file \"%s\" on line %s: unterminated quotes. (%s)", options.file_path,
	                                   GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
	return false;
unquote:
	/* state: unquote */
	// this state handles the state directly after we unquote
	// in this state we expect either another quote (entering the quoted state again, and escaping the quote)
	// or a delimiter/newline, ending the current value and moving on to the next value
	delimiter_pos = 0;
	quote_pos = 0;
	position++;
	if (position >= buffer_size && !ReadBuffer(start)) {
		// file ends right after unquote, go to final state
		offset = options.quote.size();
		goto final_state;
	}
	if (StringUtil::CharacterIsNewline(buffer[position])) {
		// quote followed by newline, add row
		offset = options.quote.size();
		goto add_row;
	}
	do {
		idx_t count = 0;
		for (; position < buffer_size; position++) {
			quote_search.Match(quote_pos, buffer[position]);
			delimiter_search.Match(delimiter_pos, buffer[position]);
			count++;
			if (count > delimiter_pos && count > quote_pos) {
				error_message = StringUtil::Format(
				    "Error in file \"%s\" on line %s: quote should be followed by end of value, end "
				    "of row or another quote. (%s)",
				    options.file_path, GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
				return false;
			}
			if (delimiter_pos == options.delimiter.size()) {
				// quote followed by delimiter, add value
				offset = options.quote.size() + options.delimiter.size() - 1;
				goto add_value;
			} else if (quote_pos == options.quote.size() &&
			           (options.escape.empty() || options.escape == options.quote)) {
				// quote followed by quote, go back to quoted state and add to escape
				escape_positions.push_back(position - start - (options.quote.size() - 1));
				goto in_quotes;
			}
		}
	} while (ReadBuffer(start));
	error_message = StringUtil::Format(
	    "Error in file \"%s\" on line %s: quote should be followed by end of value, end of row or another quote. (%s)",
	    options.file_path, GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
	return false;
handle_escape:
	escape_pos = 0;
	quote_pos = 0;
	position++;
	do {
		idx_t count = 0;
		for (; position < buffer_size; position++) {
			quote_search.Match(quote_pos, buffer[position]);
			escape_search.Match(escape_pos, buffer[position]);
			count++;
			if (count > escape_pos && count > quote_pos) {
				error_message = StringUtil::Format(
				    "Error in file \"%s\" on line %s: neither QUOTE nor ESCAPE is proceeded by ESCAPE. (%s)",
				    options.file_path, GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
				return false;
			}
			if (quote_pos == options.quote.size() || escape_pos == options.escape.size()) {
				// found quote or escape: move back to quoted state
				goto in_quotes;
			}
		}
	} while (ReadBuffer(start));
	error_message =
	    StringUtil::Format("Error in file \"%s\" on line %s: neither QUOTE nor ESCAPE is proceeded by ESCAPE. (%s)",
	                       options.file_path, GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
	return false;
carriage_return:
	/* state: carriage_return */
	// this stage optionally skips a newline (\n) character, which allows \r\n to be interpreted as a single line
	if (buffer[position] == '\n') {
		// newline after carriage return: skip
		start = ++position;
		if (position >= buffer_size && !ReadBuffer(start)) {
			// file ends right after newline, go to final state
			goto final_state;
		}
	}
	if (finished_chunk) {
		return true;
	}
	goto value_start;
final_state:
	if (finished_chunk) {
		return true;
	}
	if (column > 0 || position > start) {
		// remaining values to be added to the chunk
		AddValue(buffer.get() + start, position - start - offset, column, escape_positions);
		finished_chunk = AddRow(insert_chunk, column);
	}
	// final stage, only reached after parsing the file is finished
	// flush the parsed chunk and finalize parsing
	if (mode == ParserMode::PARSING) {
		Flush(insert_chunk);
	}

	end_of_file_reached = true;
	return true;
}

bool BufferedCSVReader::TryParseSimpleCSV(DataChunk &insert_chunk, string &error_message) {
	// used for parsing algorithm
	bool finished_chunk = false;
	idx_t column = 0;
	idx_t offset = 0;
	vector<idx_t> escape_positions;

	// read values into the buffer (if any)
	if (position >= buffer_size) {
		if (!ReadBuffer(start)) {
			return true;
		}
	}
	// start parsing the first value
	goto value_start;
value_start:
	offset = 0;
	/* state: value_start */
	// this state parses the first character of a value
	if (buffer[position] == options.quote[0]) {
		// quote: actual value starts in the next position
		// move to in_quotes state
		start = position + 1;
		goto in_quotes;
	} else {
		// no quote, move to normal parsing state
		start = position;
		goto normal;
	}
normal:
	/* state: normal parsing state */
	// this state parses the remainder of a non-quoted value until we reach a delimiter or newline
	do {
		for (; position < buffer_size; position++) {
			if (buffer[position] == options.delimiter[0]) {
				// delimiter: end the value and add it to the chunk
				goto add_value;
			} else if (StringUtil::CharacterIsNewline(buffer[position])) {
				// newline: add row
				goto add_row;
			}
		}
	} while (ReadBuffer(start));
	// file ends during normal scan: go to end state
	goto final_state;
add_value:
	AddValue(buffer.get() + start, position - start - offset, column, escape_positions);
	// increase position by 1 and move start to the new position
	offset = 0;
	start = ++position;
	if (position >= buffer_size && !ReadBuffer(start)) {
		// file ends right after delimiter, go to final state
		goto final_state;
	}
	goto value_start;
add_row : {
	// check type of newline (\r or \n)
	bool carriage_return = buffer[position] == '\r';
	AddValue(buffer.get() + start, position - start - offset, column, escape_positions);
	finished_chunk = AddRow(insert_chunk, column);
	// increase position by 1 and move start to the new position
	offset = 0;
	start = ++position;
	if (position >= buffer_size && !ReadBuffer(start)) {
		// file ends right after delimiter, go to final state
		goto final_state;
	}
	if (carriage_return) {
		// \r newline, go to special state that parses an optional \n afterwards
		goto carriage_return;
	} else {
		// \n newline, move to value start
		if (finished_chunk) {
			return true;
		}
		goto value_start;
	}
}
in_quotes:
	/* state: in_quotes */
	// this state parses the remainder of a quoted value
	position++;
	do {
		for (; position < buffer_size; position++) {
			if (buffer[position] == options.quote[0]) {
				// quote: move to unquoted state
				goto unquote;
			} else if (buffer[position] == options.escape[0]) {
				// escape: store the escaped position and move to handle_escape state
				escape_positions.push_back(position - start);
				goto handle_escape;
			}
		}
	} while (ReadBuffer(start));
	// still in quoted state at the end of the file, error:
	throw InvalidInputException("Error in file \"%s\" on line %s: unterminated quotes. (%s)", options.file_path,
	                            GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
unquote:
	/* state: unquote */
	// this state handles the state directly after we unquote
	// in this state we expect either another quote (entering the quoted state again, and escaping the quote)
	// or a delimiter/newline, ending the current value and moving on to the next value
	position++;
	if (position >= buffer_size && !ReadBuffer(start)) {
		// file ends right after unquote, go to final state
		offset = 1;
		goto final_state;
	}
	if (buffer[position] == options.quote[0] && (options.escape.empty() || options.escape[0] == options.quote[0])) {
		// escaped quote, return to quoted state and store escape position
		escape_positions.push_back(position - start);
		goto in_quotes;
	} else if (buffer[position] == options.delimiter[0]) {
		// delimiter, add value
		offset = 1;
		goto add_value;
	} else if (StringUtil::CharacterIsNewline(buffer[position])) {
		offset = 1;
		goto add_row;
	} else {
		error_message = StringUtil::Format(
		    "Error in file \"%s\" on line %s: quote should be followed by end of value, end of "
		    "row or another quote. (%s)",
		    options.file_path, GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
		return false;
	}
handle_escape:
	/* state: handle_escape */
	// escape should be followed by a quote or another escape character
	position++;
	if (position >= buffer_size && !ReadBuffer(start)) {
		error_message = StringUtil::Format(
		    "Error in file \"%s\" on line %s: neither QUOTE nor ESCAPE is proceeded by ESCAPE. (%s)", options.file_path,
		    GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
		return false;
	}
	if (buffer[position] != options.quote[0] && buffer[position] != options.escape[0]) {
		error_message = StringUtil::Format(
		    "Error in file \"%s\" on line %s: neither QUOTE nor ESCAPE is proceeded by ESCAPE. (%s)", options.file_path,
		    GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
		return false;
	}
	// escape was followed by quote or escape, go back to quoted state
	goto in_quotes;
carriage_return:
	/* state: carriage_return */
	// this stage optionally skips a newline (\n) character, which allows \r\n to be interpreted as a single line
	if (buffer[position] == '\n') {
		// newline after carriage return: skip
		// increase position by 1 and move start to the new position
		start = ++position;
		if (position >= buffer_size && !ReadBuffer(start)) {
			// file ends right after delimiter, go to final state
			goto final_state;
		}
	}
	if (finished_chunk) {
		return true;
	}
	goto value_start;
final_state:
	if (finished_chunk) {
		return true;
	}

	if (column > 0 || position > start) {
		// remaining values to be added to the chunk
		AddValue(buffer.get() + start, position - start - offset, column, escape_positions);
		finished_chunk = AddRow(insert_chunk, column);
	}

	// final stage, only reached after parsing the file is finished
	// flush the parsed chunk and finalize parsing
	if (mode == ParserMode::PARSING) {
		Flush(insert_chunk);
	}

	end_of_file_reached = true;
	return true;
}

bool BufferedCSVReader::ReadBuffer(idx_t &start) {
	auto old_buffer = move(buffer);

	// the remaining part of the last buffer
	idx_t remaining = buffer_size - start;

	bool large_buffers = mode == ParserMode::PARSING && !file_handle->OnDiskFile() && file_handle->CanSeek();
	idx_t buffer_read_size = large_buffers ? INITIAL_BUFFER_SIZE_LARGE : INITIAL_BUFFER_SIZE;

	while (remaining > buffer_read_size) {
		buffer_read_size *= 2;
	}

	// Check line length
	if (remaining > options.maximum_line_size) {
		throw InvalidInputException("Maximum line size of %llu bytes exceeded!", options.maximum_line_size);
	}

	buffer = unique_ptr<char[]>(new char[buffer_read_size + remaining + 1]);
	buffer_size = remaining + buffer_read_size;
	if (remaining > 0) {
		// remaining from last buffer: copy it here
		memcpy(buffer.get(), old_buffer.get() + start, remaining);
	}
	idx_t read_count = file_handle->Read(buffer.get() + remaining, buffer_read_size);

	bytes_in_chunk += read_count;
	buffer_size = remaining + read_count;
	buffer[buffer_size] = '\0';
	if (old_buffer) {
		cached_buffers.push_back(move(old_buffer));
	}
	start = 0;
	position = remaining;
	if (!bom_checked) {
		bom_checked = true;
		if (read_count >= 3 && buffer[0] == '\xEF' && buffer[1] == '\xBB' && buffer[2] == '\xBF') {
			position += 3;
		}
	}

	return read_count > 0;
}

void BufferedCSVReader::ParseCSV(DataChunk &insert_chunk) {
	// if no auto-detect or auto-detect with jumping samples, we have nothing cached and start from the beginning
	if (cached_chunks.empty()) {
		cached_buffers.clear();
	} else {
		auto &chunk = cached_chunks.front();
		parse_chunk.Move(*chunk);
		cached_chunks.pop();
		Flush(insert_chunk);
		return;
	}

	string error_message;
	if (!TryParseCSV(ParserMode::PARSING, insert_chunk, error_message)) {
		throw InvalidInputException(error_message);
	}
}

bool BufferedCSVReader::TryParseCSV(ParserMode mode) {
	DataChunk dummy_chunk;
	string error_message;
	return TryParseCSV(mode, dummy_chunk, error_message);
}

void BufferedCSVReader::ParseCSV(ParserMode mode) {
	DataChunk dummy_chunk;
	string error_message;
	if (!TryParseCSV(mode, dummy_chunk, error_message)) {
		throw InvalidInputException(error_message);
	}
}

bool BufferedCSVReader::TryParseCSV(ParserMode parser_mode, DataChunk &insert_chunk, string &error_message) {
	mode = parser_mode;

	if (options.quote.size() <= 1 && options.escape.size() <= 1 && options.delimiter.size() == 1) {
		return TryParseSimpleCSV(insert_chunk, error_message);
	} else {
		return TryParseComplexCSV(insert_chunk, error_message);
	}
}

void BufferedCSVReader::AddValue(char *str_val, idx_t length, idx_t &column, vector<idx_t> &escape_positions) {
	if (length == 0 && column == 0) {
		row_empty = true;
	} else {
		row_empty = false;
	}

	if (!sql_types.empty() && column == sql_types.size() && length == 0) {
		// skip a single trailing delimiter in last column
		return;
	}
	if (mode == ParserMode::SNIFFING_DIALECT) {
		column++;
		return;
	}
	if (column >= sql_types.size()) {
		if (options.ignore_errors) {
			error_column_overflow = true;
			return;
		} else {
			throw InvalidInputException("Error on line %s: expected %lld values per row, but got more. (%s)",
			                            GetLineNumberStr(linenr, linenr_estimated).c_str(), sql_types.size(),
			                            options.ToString());
		}
	}

	// insert the line number into the chunk
	idx_t row_entry = parse_chunk.size();

	str_val[length] = '\0';

	// test against null string
	if (!options.force_not_null[column] && strcmp(options.null_str.c_str(), str_val) == 0) {
		FlatVector::SetNull(parse_chunk.data[column], row_entry, true);
	} else {
		auto &v = parse_chunk.data[column];
		auto parse_data = FlatVector::GetData<string_t>(v);
		if (!escape_positions.empty()) {
			// remove escape characters (if any)
			string old_val = str_val;
			string new_val = "";
			idx_t prev_pos = 0;
			for (idx_t i = 0; i < escape_positions.size(); i++) {
				idx_t next_pos = escape_positions[i];
				new_val += old_val.substr(prev_pos, next_pos - prev_pos);

				if (options.escape.empty() || options.escape == options.quote) {
					prev_pos = next_pos + options.quote.size();
				} else {
					prev_pos = next_pos + options.escape.size();
				}
			}
			new_val += old_val.substr(prev_pos, old_val.size() - prev_pos);
			escape_positions.clear();
			parse_data[row_entry] = StringVector::AddStringOrBlob(v, string_t(new_val));
		} else {
			parse_data[row_entry] = string_t(str_val, length);
		}
	}

	// move to the next column
	column++;
}

bool BufferedCSVReader::AddRow(DataChunk &insert_chunk, idx_t &column) {
	linenr++;

	if (row_empty) {
		row_empty = false;
		if (sql_types.size() != 1) {
			column = 0;
			return false;
		}
	}

	// Error forwarded by 'ignore_errors' - originally encountered in 'AddValue'
	if (error_column_overflow) {
		D_ASSERT(options.ignore_errors);
		error_column_overflow = false;
		column = 0;
		return false;
	}

	if (column < sql_types.size() && mode != ParserMode::SNIFFING_DIALECT) {
		if (options.ignore_errors) {
			column = 0;
			return false;
		} else {
			throw InvalidInputException("Error on line %s: expected %lld values per row, but got %d. (%s)",
			                            GetLineNumberStr(linenr, linenr_estimated).c_str(), sql_types.size(), column,
			                            options.ToString());
		}
	}

	if (mode == ParserMode::SNIFFING_DIALECT) {
		sniffed_column_counts.push_back(column);

		if (sniffed_column_counts.size() == options.sample_chunk_size) {
			return true;
		}
	} else {
		parse_chunk.SetCardinality(parse_chunk.size() + 1);
	}

	if (mode == ParserMode::PARSING_HEADER) {
		return true;
	}

	if (mode == ParserMode::SNIFFING_DATATYPES && parse_chunk.size() == options.sample_chunk_size) {
		return true;
	}

	if (mode == ParserMode::PARSING && parse_chunk.size() == STANDARD_VECTOR_SIZE) {
		Flush(insert_chunk);
		return true;
	}

	column = 0;
	return false;
}

void BufferedCSVReader::Flush(DataChunk &insert_chunk) {
	if (parse_chunk.size() == 0) {
		return;
	}

	bool conversion_error_ignored = false;

	// convert the columns in the parsed chunk to the types of the table
	insert_chunk.SetCardinality(parse_chunk);
	for (idx_t col_idx = 0; col_idx < sql_types.size(); col_idx++) {
		if (sql_types[col_idx].id() == LogicalTypeId::VARCHAR) {
			// target type is varchar: no need to convert
			// just test that all strings are valid utf-8 strings
			auto parse_data = FlatVector::GetData<string_t>(parse_chunk.data[col_idx]);
			for (idx_t i = 0; i < parse_chunk.size(); i++) {
				if (!FlatVector::IsNull(parse_chunk.data[col_idx], i)) {
					auto s = parse_data[i];
					auto utf_type = Utf8Proc::Analyze(s.GetDataUnsafe(), s.GetSize());
					if (utf_type == UnicodeType::INVALID) {
						string col_name = to_string(col_idx);
						if (col_idx < col_names.size()) {
							col_name = "\"" + col_names[col_idx] + "\"";
						}
						throw InvalidInputException("Error in file \"%s\" between line %llu and %llu in column \"%s\": "
						                            "file is not valid UTF8. Parser options: %s",
						                            options.file_path, linenr - parse_chunk.size(), linenr, col_name,
						                            options.ToString());
					}
				}
			}
			insert_chunk.data[col_idx].Reference(parse_chunk.data[col_idx]);
		} else {
			string error_message;
			bool success;
			if (options.has_format[LogicalTypeId::DATE] && sql_types[col_idx].id() == LogicalTypeId::DATE) {
				// use the date format to cast the chunk
				success = TryCastDateVector(options, parse_chunk.data[col_idx], insert_chunk.data[col_idx],
				                            parse_chunk.size(), error_message);
			} else if (options.has_format[LogicalTypeId::TIMESTAMP] &&
			           sql_types[col_idx].id() == LogicalTypeId::TIMESTAMP) {
				// use the date format to cast the chunk
				success = TryCastTimestampVector(options, parse_chunk.data[col_idx], insert_chunk.data[col_idx],
				                                 parse_chunk.size(), error_message);
			} else {
				// target type is not varchar: perform a cast
				success = VectorOperations::TryCast(parse_chunk.data[col_idx], insert_chunk.data[col_idx],
				                                    parse_chunk.size(), &error_message);
			}
			if (success) {
				continue;
			}
			if (options.ignore_errors) {
				conversion_error_ignored = true;
				continue;
			}
			string col_name = to_string(col_idx);
			if (col_idx < col_names.size()) {
				col_name = "\"" + col_names[col_idx] + "\"";
			}

			if (options.auto_detect) {
				throw InvalidInputException("%s in column %s, between line %llu and %llu. Parser "
				                            "options: %s. Consider either increasing the sample size "
				                            "(SAMPLE_SIZE=X [X rows] or SAMPLE_SIZE=-1 [all rows]), "
				                            "or skipping column conversion (ALL_VARCHAR=1)",
				                            error_message, col_name, linenr - parse_chunk.size() + 1, linenr,
				                            options.ToString());
			} else {
				throw InvalidInputException("%s between line %llu and %llu in column %s. Parser options: %s ",
				                            error_message, linenr - parse_chunk.size(), linenr, col_name,
				                            options.ToString());
			}
		}
	}
	if (conversion_error_ignored) {
		D_ASSERT(options.ignore_errors);
		SelectionVector succesful_rows;
		succesful_rows.Initialize(parse_chunk.size());
		idx_t sel_size = 0;

		for (idx_t row_idx = 0; row_idx < parse_chunk.size(); row_idx++) {
			bool failed = false;
			for (idx_t column_idx = 0; column_idx < sql_types.size(); column_idx++) {

				auto &inserted_column = insert_chunk.data[column_idx];
				auto &parsed_column = parse_chunk.data[column_idx];

				bool was_already_null = FlatVector::IsNull(parsed_column, row_idx);
				if (!was_already_null && FlatVector::IsNull(inserted_column, row_idx)) {
					failed = true;
					break;
				}
			}
			if (!failed) {
				succesful_rows.set_index(sel_size++, row_idx);
			}
		}
		insert_chunk.Slice(succesful_rows, sel_size);
	}
	parse_chunk.Reset();
}
} // namespace duckdb




#include <algorithm>

namespace duckdb {

class CopyToFunctionGlobalState : public GlobalSinkState {
public:
	explicit CopyToFunctionGlobalState(unique_ptr<GlobalFunctionData> global_state)
	    : rows_copied(0), global_state(move(global_state)) {
	}

	idx_t rows_copied;
	unique_ptr<GlobalFunctionData> global_state;
};

class CopyToFunctionLocalState : public LocalSinkState {
public:
	explicit CopyToFunctionLocalState(unique_ptr<LocalFunctionData> local_state) : local_state(move(local_state)) {
	}
	unique_ptr<LocalFunctionData> local_state;
};

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
void MoveTmpFile(ClientContext &context, const string &tmp_file_path) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto file_path = tmp_file_path.substr(0, tmp_file_path.length() - 4);
	if (fs.FileExists(file_path)) {
		fs.RemoveFile(file_path);
	}
	fs.MoveFile(tmp_file_path, file_path);
}

PhysicalCopyToFile::PhysicalCopyToFile(vector<LogicalType> types, CopyFunction function_p,
                                       unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::COPY_TO_FILE, move(types), estimated_cardinality),
      function(move(function_p)), bind_data(move(bind_data)) {
}

SinkResultType PhysicalCopyToFile::Sink(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate,
                                        DataChunk &input) const {
	auto &g = (CopyToFunctionGlobalState &)gstate;
	auto &l = (CopyToFunctionLocalState &)lstate;

	g.rows_copied += input.size();
	function.copy_to_sink(context, *bind_data, *g.global_state, *l.local_state, input);
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalCopyToFile::Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const {
	auto &g = (CopyToFunctionGlobalState &)gstate;
	auto &l = (CopyToFunctionLocalState &)lstate;

	if (function.copy_to_combine) {
		function.copy_to_combine(context, *bind_data, *g.global_state, *l.local_state);
	}
}

SinkFinalizeType PhysicalCopyToFile::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                              GlobalSinkState &gstate_p) const {
	auto &gstate = (CopyToFunctionGlobalState &)gstate_p;
	if (function.copy_to_finalize) {
		function.copy_to_finalize(context, *bind_data, *gstate.global_state);

		if (use_tmp_file) {
			MoveTmpFile(context, file_path);
		}
	}
	return SinkFinalizeType::READY;
}

unique_ptr<LocalSinkState> PhysicalCopyToFile::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<CopyToFunctionLocalState>(function.copy_to_initialize_local(context, *bind_data));
}
unique_ptr<GlobalSinkState> PhysicalCopyToFile::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<CopyToFunctionGlobalState>(function.copy_to_initialize_global(context, *bind_data, file_path));
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CopyToFileState : public GlobalSourceState {
public:
	CopyToFileState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCopyToFile::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<CopyToFileState>();
}

void PhysicalCopyToFile::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                 LocalSourceState &lstate) const {
	auto &state = (CopyToFileState &)gstate;
	auto &g = (CopyToFunctionGlobalState &)*sink_state;
	if (state.finished) {
		return;
	}

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(g.rows_copied));
	state.finished = true;
}

} // namespace duckdb








namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class DeleteGlobalState : public GlobalSinkState {
public:
	explicit DeleteGlobalState(Allocator &allocator)
	    : deleted_count(0), return_chunk_collection(allocator), returned_chunk_count(0) {
	}

	mutex delete_lock;
	idx_t deleted_count;
	ChunkCollection return_chunk_collection;
	idx_t returned_chunk_count;
};

class DeleteLocalState : public LocalSinkState {
public:
	DeleteLocalState(Allocator &allocator, const vector<LogicalType> &table_types) {
		delete_chunk.Initialize(allocator, table_types);
	}
	DataChunk delete_chunk;
};

SinkResultType PhysicalDelete::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                    DataChunk &input) const {
	auto &gstate = (DeleteGlobalState &)state;
	auto &ustate = (DeleteLocalState &)lstate;

	// get rows and
	auto &transaction = Transaction::GetTransaction(context.client);
	auto &row_identifiers = input.data[row_id_index];

	vector<column_t> column_ids;
	for (idx_t i = 0; i < table.column_definitions.size(); i++) {
		column_ids.emplace_back(i);
	};
	auto cfs = ColumnFetchState();

	lock_guard<mutex> delete_guard(gstate.delete_lock);
	if (return_chunk) {
		row_identifiers.Normalify(input.size());
		table.Fetch(transaction, ustate.delete_chunk, column_ids, row_identifiers, input.size(), cfs);
		gstate.return_chunk_collection.Append(ustate.delete_chunk);
	}
	gstate.deleted_count += table.Delete(tableref, context.client, row_identifiers, input.size());

	return SinkResultType::NEED_MORE_INPUT;
}

unique_ptr<GlobalSinkState> PhysicalDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<DeleteGlobalState>(Allocator::Get(context));
}

unique_ptr<LocalSinkState> PhysicalDelete::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<DeleteLocalState>(Allocator::Get(context.client), table.GetTypes());
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class DeleteSourceState : public GlobalSourceState {
public:
	DeleteSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalDelete::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<DeleteSourceState>();
}

void PhysicalDelete::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                             LocalSourceState &lstate) const {
	auto &state = (DeleteSourceState &)gstate;
	auto &g = (DeleteGlobalState &)*sink_state;
	if (state.finished) {
		return;
	}

	if (!return_chunk) {
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(g.deleted_count));
		state.finished = true;
	}

	idx_t chunk_return = g.returned_chunk_count;
	if (chunk_return >= g.return_chunk_collection.Chunks().size()) {
		return;
	}
	chunk.Reference(g.return_chunk_collection.GetChunk(chunk_return));
	chunk.SetCardinality((g.return_chunk_collection.GetChunk(chunk_return)).size());
	g.returned_chunk_count += 1;
	if (g.returned_chunk_count >= g.return_chunk_collection.Chunks().size()) {
		state.finished = true;
	}
}

} // namespace duckdb










#include <algorithm>
#include <sstream>

namespace duckdb {

using std::stringstream;

static void WriteCatalogEntries(stringstream &ss, vector<CatalogEntry *> &entries) {
	for (auto &entry : entries) {
		if (entry->internal) {
			continue;
		}
		ss << entry->ToSQL() << std::endl;
	}
	ss << std::endl;
}

static void WriteStringStreamToFile(FileSystem &fs, FileOpener *opener, stringstream &ss, const string &path) {
	auto ss_string = ss.str();
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW,
	                          FileLockType::WRITE_LOCK, FileSystem::DEFAULT_COMPRESSION, opener);
	fs.Write(*handle, (void *)ss_string.c_str(), ss_string.size());
	handle.reset();
}

static void WriteValueAsSQL(stringstream &ss, Value &val) {
	if (val.type().IsNumeric()) {
		ss << val.ToString();
	} else {
		ss << "'" << val.ToString() << "'";
	}
}

static void WriteCopyStatement(FileSystem &fs, stringstream &ss, TableCatalogEntry *table, CopyInfo &info,
                               ExportedTableData &exported_table, CopyFunction const &function) {
	ss << "COPY ";

	if (exported_table.schema_name != DEFAULT_SCHEMA) {
		ss << KeywordHelper::WriteOptionallyQuoted(exported_table.schema_name) << ".";
	}

	ss << KeywordHelper::WriteOptionallyQuoted(exported_table.table_name) << " FROM '" << exported_table.file_path
	   << "' (";

	// write the copy options
	ss << "FORMAT '" << info.format << "'";
	if (info.format == "csv") {
		// insert default csv options, if not specified
		if (info.options.find("header") == info.options.end()) {
			info.options["header"].push_back(Value::INTEGER(0));
		}
		if (info.options.find("delimiter") == info.options.end() && info.options.find("sep") == info.options.end() &&
		    info.options.find("delim") == info.options.end()) {
			info.options["delimiter"].push_back(Value(","));
		}
		if (info.options.find("quote") == info.options.end()) {
			info.options["quote"].push_back(Value("\""));
		}
	}
	for (auto &copy_option : info.options) {
		ss << ", " << copy_option.first << " ";
		if (copy_option.second.size() == 1) {
			WriteValueAsSQL(ss, copy_option.second[0]);
		} else {
			// FIXME handle multiple options
			throw NotImplementedException("FIXME: serialize list of options");
		}
	}
	ss << ");" << std::endl;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class ExportSourceState : public GlobalSourceState {
public:
	ExportSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalExport::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<ExportSourceState>();
}

void PhysicalExport::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                             LocalSourceState &lstate) const {
	auto &state = (ExportSourceState &)gstate;
	if (state.finished) {
		return;
	}

	auto &ccontext = context.client;
	auto &fs = FileSystem::GetFileSystem(ccontext);
	auto *opener = FileSystem::GetFileOpener(ccontext);

	// gather all catalog types to export
	vector<CatalogEntry *> schemas;
	vector<CatalogEntry *> custom_types;
	vector<CatalogEntry *> sequences;
	vector<CatalogEntry *> tables;
	vector<CatalogEntry *> views;
	vector<CatalogEntry *> indexes;
	vector<CatalogEntry *> macros;

	auto schema_list = Catalog::GetCatalog(ccontext).schemas->GetEntries<SchemaCatalogEntry>(context.client);
	for (auto &schema : schema_list) {
		if (!schema->internal) {
			schemas.push_back(schema);
		}
		schema->Scan(context.client, CatalogType::TABLE_ENTRY, [&](CatalogEntry *entry) {
			if (entry->internal) {
				return;
			}
			if (entry->type != CatalogType::TABLE_ENTRY) {
				views.push_back(entry);
			}
		});
		schema->Scan(context.client, CatalogType::SEQUENCE_ENTRY,
		             [&](CatalogEntry *entry) { sequences.push_back(entry); });
		schema->Scan(context.client, CatalogType::TYPE_ENTRY,
		             [&](CatalogEntry *entry) { custom_types.push_back(entry); });
		schema->Scan(context.client, CatalogType::INDEX_ENTRY, [&](CatalogEntry *entry) { indexes.push_back(entry); });
		schema->Scan(context.client, CatalogType::MACRO_ENTRY, [&](CatalogEntry *entry) {
			if (!entry->internal && entry->type == CatalogType::MACRO_ENTRY) {
				macros.push_back(entry);
			}
		});
		schema->Scan(context.client, CatalogType::TABLE_MACRO_ENTRY, [&](CatalogEntry *entry) {
			if (!entry->internal && entry->type == CatalogType::TABLE_MACRO_ENTRY) {
				macros.push_back(entry);
			}
		});
	}

	// consider the order of tables because of foreign key constraint
	for (idx_t i = 0; i < exported_tables.data.size(); i++) {
		tables.push_back((CatalogEntry *)exported_tables.data[i].entry);
	}

	// order macro's by timestamp so nested macro's are imported nicely
	sort(macros.begin(), macros.end(),
	     [](const CatalogEntry *lhs, const CatalogEntry *rhs) { return lhs->oid < rhs->oid; });

	// write the schema.sql file
	// export order is SCHEMA -> SEQUENCE -> TABLE -> VIEW -> INDEX

	stringstream ss;
	WriteCatalogEntries(ss, schemas);
	WriteCatalogEntries(ss, custom_types);
	WriteCatalogEntries(ss, sequences);
	WriteCatalogEntries(ss, tables);
	WriteCatalogEntries(ss, views);
	WriteCatalogEntries(ss, indexes);
	WriteCatalogEntries(ss, macros);

	WriteStringStreamToFile(fs, opener, ss, fs.JoinPath(info->file_path, "schema.sql"));

	// write the load.sql file
	// for every table, we write COPY INTO statement with the specified options
	stringstream load_ss;
	for (idx_t i = 0; i < exported_tables.data.size(); i++) {
		auto &table = exported_tables.data[i].entry;
		auto exported_table_info = exported_tables.data[i].table_data;
		WriteCopyStatement(fs, load_ss, table, *info, exported_table_info, function);
	}
	WriteStringStreamToFile(fs, opener, load_ss, fs.JoinPath(info->file_path, "load.sql"));
	state.finished = true;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PhysicalExport::Sink(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate,
                                    DataChunk &input) const {
	// nop
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalExport::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
	// EXPORT has an optional child
	// we only need to schedule child pipelines if there is a child
	state.SetPipelineSource(current, this);
	if (children.empty()) {
		return;
	}
	PhysicalOperator::BuildPipelines(executor, current, state);
}

vector<const PhysicalOperator *> PhysicalExport::GetSources() const {
	return {this};
}

} // namespace duckdb









namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class InsertGlobalState : public GlobalSinkState {
public:
	explicit InsertGlobalState(Allocator &allocator)
	    : insert_count(0), return_chunk_collection(allocator), returned_chunk_count(0) {
	}

	mutex lock;
	idx_t insert_count;
	ChunkCollection return_chunk_collection;
	idx_t returned_chunk_count;
};

class InsertLocalState : public LocalSinkState {
public:
	InsertLocalState(Allocator &allocator, const vector<LogicalType> &types,
	                 const vector<unique_ptr<Expression>> &bound_defaults)
	    : default_executor(allocator, bound_defaults) {
		insert_chunk.Initialize(allocator, types);
	}

	DataChunk insert_chunk;
	ExpressionExecutor default_executor;
};

PhysicalInsert::PhysicalInsert(vector<LogicalType> types, TableCatalogEntry *table, vector<idx_t> column_index_map,
                               vector<unique_ptr<Expression>> bound_defaults, idx_t estimated_cardinality,
                               bool return_chunk)
    : PhysicalOperator(PhysicalOperatorType::INSERT, move(types), estimated_cardinality),
      column_index_map(std::move(column_index_map)), table(table), bound_defaults(move(bound_defaults)),
      return_chunk(return_chunk) {
}

SinkResultType PhysicalInsert::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                    DataChunk &chunk) const {
	auto &gstate = (InsertGlobalState &)state;
	auto &istate = (InsertLocalState &)lstate;

	chunk.Normalify();
	istate.default_executor.SetChunk(chunk);

	istate.insert_chunk.Reset();
	istate.insert_chunk.SetCardinality(chunk);

	if (!column_index_map.empty()) {
		// columns specified by the user, use column_index_map
		for (idx_t i = 0; i < table->columns.size(); i++) {
			auto &col = table->columns[i];
			if (col.Generated()) {
				continue;
			}
			auto storage_idx = col.StorageOid();
			if (column_index_map[i] == DConstants::INVALID_INDEX) {
				// insert default value
				istate.default_executor.ExecuteExpression(i, istate.insert_chunk.data[storage_idx]);
			} else {
				// get value from child chunk
				D_ASSERT((idx_t)column_index_map[i] < chunk.ColumnCount());
				D_ASSERT(istate.insert_chunk.data[storage_idx].GetType() == chunk.data[column_index_map[i]].GetType());
				istate.insert_chunk.data[storage_idx].Reference(chunk.data[column_index_map[i]]);
			}
		}
	} else {
		// no columns specified, just append directly
		for (idx_t i = 0; i < istate.insert_chunk.ColumnCount(); i++) {
			D_ASSERT(istate.insert_chunk.data[i].GetType() == chunk.data[i].GetType());
			istate.insert_chunk.data[i].Reference(chunk.data[i]);
		}
	}

	lock_guard<mutex> glock(gstate.lock);
	table->storage->Append(*table, context.client, istate.insert_chunk);

	if (return_chunk) {
		gstate.return_chunk_collection.Append(istate.insert_chunk);
	}

	gstate.insert_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

unique_ptr<GlobalSinkState> PhysicalInsert::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<InsertGlobalState>(Allocator::Get(context));
}

unique_ptr<LocalSinkState> PhysicalInsert::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<InsertLocalState>(Allocator::Get(context.client), table->GetTypes(), bound_defaults);
}

void PhysicalInsert::Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const {
	auto &state = (InsertLocalState &)lstate;
	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(this, &state.default_executor, "default_executor", 1);
	client_profiler.Flush(context.thread.profiler);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class InsertSourceState : public GlobalSourceState {
public:
	InsertSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalInsert::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<InsertSourceState>();
}

void PhysicalInsert::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                             LocalSourceState &lstate) const {
	auto &state = (InsertSourceState &)gstate;
	auto &insert_gstate = (InsertGlobalState &)*sink_state;
	if (state.finished) {
		return;
	}
	if (!return_chunk) {
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.insert_count));
		state.finished = true;
	}

	idx_t chunk_return = insert_gstate.returned_chunk_count;
	if (chunk_return >= insert_gstate.return_chunk_collection.Chunks().size()) {
		return;
	}

	chunk.Reference(insert_gstate.return_chunk_collection.GetChunk(chunk_return));
	chunk.SetCardinality((insert_gstate.return_chunk_collection.GetChunk(chunk_return)).size());
	insert_gstate.returned_chunk_count += 1;
	if (insert_gstate.returned_chunk_count >= insert_gstate.return_chunk_collection.Chunks().size()) {
		state.finished = true;
	}
}

} // namespace duckdb










namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class UpdateGlobalState : public GlobalSinkState {
public:
	explicit UpdateGlobalState(Allocator &allocator)
	    : updated_count(0), return_chunk_collection(allocator), returned_chunk_count(0) {
	}

	mutex lock;
	idx_t updated_count;
	unordered_set<row_t> updated_columns;
	ChunkCollection return_chunk_collection;
	idx_t returned_chunk_count;
};

class UpdateLocalState : public LocalSinkState {
public:
	UpdateLocalState(Allocator &allocator, const vector<unique_ptr<Expression>> &expressions,
	                 const vector<LogicalType> &table_types, const vector<unique_ptr<Expression>> &bound_defaults)
	    : default_executor(allocator, bound_defaults) {
		// initialize the update chunk
		vector<LogicalType> update_types;
		update_types.reserve(expressions.size());
		for (auto &expr : expressions) {
			update_types.push_back(expr->return_type);
		}
		update_chunk.Initialize(allocator, update_types);
		// initialize the mock chunk
		mock_chunk.Initialize(allocator, table_types);
	}

	DataChunk update_chunk;
	DataChunk mock_chunk;
	ExpressionExecutor default_executor;
};

SinkResultType PhysicalUpdate::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                    DataChunk &chunk) const {
	auto &gstate = (UpdateGlobalState &)state;
	auto &ustate = (UpdateLocalState &)lstate;

	DataChunk &update_chunk = ustate.update_chunk;
	DataChunk &mock_chunk = ustate.mock_chunk;

	chunk.Normalify();
	ustate.default_executor.SetChunk(chunk);

	// update data in the base table
	// the row ids are given to us as the last column of the child chunk
	auto &row_ids = chunk.data[chunk.ColumnCount() - 1];
	update_chunk.SetCardinality(chunk);
	for (idx_t i = 0; i < expressions.size(); i++) {
		if (expressions[i]->type == ExpressionType::VALUE_DEFAULT) {
			// default expression, set to the default value of the column
			ustate.default_executor.ExecuteExpression(columns[i], update_chunk.data[i]);
		} else {
			D_ASSERT(expressions[i]->type == ExpressionType::BOUND_REF);
			// index into child chunk
			auto &binding = (BoundReferenceExpression &)*expressions[i];
			update_chunk.data[i].Reference(chunk.data[binding.index]);
		}
	}

	lock_guard<mutex> glock(gstate.lock);
	if (update_is_del_and_insert) {
		// index update or update on complex type, perform a delete and an append instead

		// figure out which rows have not yet been deleted in this update
		// this is required since we might see the same row_id multiple times
		// in the case of an UPDATE query that e.g. has joins
		auto row_id_data = FlatVector::GetData<row_t>(row_ids);
		SelectionVector sel(STANDARD_VECTOR_SIZE);
		idx_t update_count = 0;
		for (idx_t i = 0; i < update_chunk.size(); i++) {
			auto row_id = row_id_data[i];
			if (gstate.updated_columns.find(row_id) == gstate.updated_columns.end()) {
				gstate.updated_columns.insert(row_id);
				sel.set_index(update_count++, i);
			}
		}
		if (update_count != update_chunk.size()) {
			// we need to slice here
			update_chunk.Slice(sel, update_count);
		}
		table.Delete(tableref, context.client, row_ids, update_chunk.size());
		// for the append we need to arrange the columns in a specific manner (namely the "standard table order")
		mock_chunk.SetCardinality(update_chunk);
		for (idx_t i = 0; i < columns.size(); i++) {
			mock_chunk.data[columns[i]].Reference(update_chunk.data[i]);
		}
		table.Append(tableref, context.client, mock_chunk);
	} else {
		if (return_chunk) {
			mock_chunk.SetCardinality(update_chunk);
			for (idx_t i = 0; i < columns.size(); i++) {
				mock_chunk.data[columns[i]].Reference(update_chunk.data[i]);
			}
		}
		table.Update(tableref, context.client, row_ids, columns, update_chunk);
	}

	if (return_chunk) {
		gstate.return_chunk_collection.Append(mock_chunk);
	}

	gstate.updated_count += chunk.size();

	return SinkResultType::NEED_MORE_INPUT;
}

unique_ptr<GlobalSinkState> PhysicalUpdate::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<UpdateGlobalState>(Allocator::Get(context));
}

unique_ptr<LocalSinkState> PhysicalUpdate::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<UpdateLocalState>(Allocator::Get(context.client), expressions, table.GetTypes(), bound_defaults);
}

void PhysicalUpdate::Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const {
	auto &state = (UpdateLocalState &)lstate;
	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(this, &state.default_executor, "default_executor", 1);
	client_profiler.Flush(context.thread.profiler);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class UpdateSourceState : public GlobalSourceState {
public:
	UpdateSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalUpdate::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<UpdateSourceState>();
}

void PhysicalUpdate::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                             LocalSourceState &lstate) const {
	auto &state = (UpdateSourceState &)gstate;
	auto &g = (UpdateGlobalState &)*sink_state;
	if (state.finished) {
		return;
	}
	if (!return_chunk) {
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(g.updated_count));
		state.finished = true;
	}

	idx_t chunk_return = g.returned_chunk_count;
	if (chunk_return >= g.return_chunk_collection.Chunks().size()) {
		return;
	}
	chunk.Reference(g.return_chunk_collection.GetChunk(chunk_return));
	chunk.SetCardinality((g.return_chunk_collection.GetChunk(chunk_return)).size());
	g.returned_chunk_count += 1;
	if (g.returned_chunk_count >= g.return_chunk_collection.Chunks().size()) {
		state.finished = true;
	}
}

} // namespace duckdb




namespace duckdb {

class ProjectionState : public OperatorState {
public:
	explicit ProjectionState(ExecutionContext &context, const vector<unique_ptr<Expression>> &expressions)
	    : executor(Allocator::Get(context.client), expressions) {
	}

	ExpressionExecutor executor;

public:
	void Finalize(PhysicalOperator *op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, &executor, "projection", 0);
	}
};

PhysicalProjection::PhysicalProjection(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                                       idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::PROJECTION, move(types), estimated_cardinality),
      select_list(move(select_list)) {
}

OperatorResultType PhysicalProjection::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                               GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = (ProjectionState &)state_p;
	state.executor.Execute(input, chunk);
	return OperatorResultType::NEED_MORE_INPUT;
}

unique_ptr<OperatorState> PhysicalProjection::GetOperatorState(ExecutionContext &context) const {
	return make_unique<ProjectionState>(context, select_list);
}

string PhysicalProjection::ParamsToString() const {
	string extra_info;
	for (auto &expr : select_list) {
		extra_info += expr->GetName() + "\n";
	}
	return extra_info;
}

} // namespace duckdb


namespace duckdb {

class TableInOutLocalState : public OperatorState {
public:
	TableInOutLocalState() {
	}

	unique_ptr<LocalTableFunctionState> local_state;
};

class TableInOutGlobalState : public GlobalOperatorState {
public:
	TableInOutGlobalState() {
	}

	unique_ptr<GlobalTableFunctionState> global_state;
};

PhysicalTableInOutFunction::PhysicalTableInOutFunction(vector<LogicalType> types, TableFunction function_p,
                                                       unique_ptr<FunctionData> bind_data_p,
                                                       vector<column_t> column_ids_p, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::INOUT_FUNCTION, move(types), estimated_cardinality),
      function(move(function_p)), bind_data(move(bind_data_p)), column_ids(move(column_ids_p)) {
}

unique_ptr<OperatorState> PhysicalTableInOutFunction::GetOperatorState(ExecutionContext &context) const {
	auto &gstate = (TableInOutGlobalState &)*op_state;
	auto result = make_unique<TableInOutLocalState>();
	if (function.init_local) {
		TableFunctionInitInput input(bind_data.get(), column_ids, nullptr);
		result->local_state = function.init_local(context, input, gstate.global_state.get());
	}
	return move(result);
}

unique_ptr<GlobalOperatorState> PhysicalTableInOutFunction::GetGlobalOperatorState(ClientContext &context) const {
	auto result = make_unique<TableInOutGlobalState>();
	if (function.init_global) {
		TableFunctionInitInput input(bind_data.get(), column_ids, nullptr);
		result->global_state = function.init_global(context, input);
	}
	return move(result);
}

OperatorResultType PhysicalTableInOutFunction::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                       GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &gstate = (TableInOutGlobalState &)gstate_p;
	auto &state = (TableInOutLocalState &)state_p;
	TableFunctionInput data(bind_data.get(), state.local_state.get(), gstate.global_state.get());
	return function.in_out_function(context, data, input, chunk);
}

} // namespace duckdb








namespace duckdb {

class UnnestOperatorState : public OperatorState {
public:
	UnnestOperatorState(Allocator &allocator, const vector<unique_ptr<Expression>> &select_list)
	    : parent_position(0), list_position(0), list_length(-1), first_fetch(true), executor(allocator) {
		vector<LogicalType> list_data_types;
		for (auto &exp : select_list) {
			D_ASSERT(exp->type == ExpressionType::BOUND_UNNEST);
			auto bue = (BoundUnnestExpression *)exp.get();
			list_data_types.push_back(bue->child->return_type);
			executor.AddExpression(*bue->child.get());
		}
		list_data.Initialize(allocator, list_data_types);

		list_vector_data.resize(list_data.ColumnCount());
		list_child_data.resize(list_data.ColumnCount());
	}

	idx_t parent_position;
	idx_t list_position;
	int64_t list_length;
	bool first_fetch;

	ExpressionExecutor executor;
	DataChunk list_data;
	vector<VectorData> list_vector_data;
	vector<VectorData> list_child_data;
};

// this implements a sorted window functions variant
PhysicalUnnest::PhysicalUnnest(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                               idx_t estimated_cardinality, PhysicalOperatorType type)
    : PhysicalOperator(type, move(types), estimated_cardinality), select_list(std::move(select_list)) {
	D_ASSERT(!this->select_list.empty());
}

static void UnnestNull(idx_t start, idx_t end, Vector &result) {
	if (result.GetType().InternalType() == PhysicalType::STRUCT) {
		auto &children = StructVector::GetEntries(result);
		for (auto &child : children) {
			UnnestNull(start, end, *child);
		}
	}
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = start; i < end; i++) {
		validity.SetInvalid(i);
	}
	if (result.GetType().InternalType() == PhysicalType::STRUCT) {
		auto &struct_children = StructVector::GetEntries(result);
		for (auto &child : struct_children) {
			UnnestNull(start, end, *child);
		}
	}
}

template <class T>
static void TemplatedUnnest(VectorData &vdata, idx_t start, idx_t end, Vector &result) {
	auto source_data = (T *)vdata.data;
	auto &source_mask = vdata.validity;
	auto result_data = FlatVector::GetData<T>(result);
	auto &result_mask = FlatVector::Validity(result);

	for (idx_t i = start; i < end; i++) {
		auto source_idx = vdata.sel->get_index(i);
		auto target_idx = i - start;
		if (source_mask.RowIsValid(source_idx)) {
			result_data[target_idx] = source_data[source_idx];
			result_mask.SetValid(target_idx);
		} else {
			result_mask.SetInvalid(target_idx);
		}
	}
}

static void UnnestValidity(VectorData &vdata, idx_t start, idx_t end, Vector &result) {
	auto &source_mask = vdata.validity;
	auto &result_mask = FlatVector::Validity(result);

	for (idx_t i = start; i < end; i++) {
		auto source_idx = vdata.sel->get_index(i);
		auto target_idx = i - start;
		result_mask.Set(target_idx, source_mask.RowIsValid(source_idx));
	}
}

static void UnnestVector(VectorData &vdata, Vector &source, idx_t list_size, idx_t start, idx_t end, Vector &result) {
	switch (result.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedUnnest<int8_t>(vdata, start, end, result);
		break;
	case PhysicalType::INT16:
		TemplatedUnnest<int16_t>(vdata, start, end, result);
		break;
	case PhysicalType::INT32:
		TemplatedUnnest<int32_t>(vdata, start, end, result);
		break;
	case PhysicalType::INT64:
		TemplatedUnnest<int64_t>(vdata, start, end, result);
		break;
	case PhysicalType::INT128:
		TemplatedUnnest<hugeint_t>(vdata, start, end, result);
		break;
	case PhysicalType::UINT8:
		TemplatedUnnest<uint8_t>(vdata, start, end, result);
		break;
	case PhysicalType::UINT16:
		TemplatedUnnest<uint16_t>(vdata, start, end, result);
		break;
	case PhysicalType::UINT32:
		TemplatedUnnest<uint32_t>(vdata, start, end, result);
		break;
	case PhysicalType::UINT64:
		TemplatedUnnest<uint64_t>(vdata, start, end, result);
		break;
	case PhysicalType::FLOAT:
		TemplatedUnnest<float>(vdata, start, end, result);
		break;
	case PhysicalType::DOUBLE:
		TemplatedUnnest<double>(vdata, start, end, result);
		break;
	case PhysicalType::INTERVAL:
		TemplatedUnnest<interval_t>(vdata, start, end, result);
		break;
	case PhysicalType::VARCHAR:
		TemplatedUnnest<string_t>(vdata, start, end, result);
		break;
	case PhysicalType::LIST: {
		auto &target = ListVector::GetEntry(result);
		target.Reference(ListVector::GetEntry(source));
		ListVector::SetListSize(result, ListVector::GetListSize(source));
		TemplatedUnnest<list_entry_t>(vdata, start, end, result);
		break;
	}
	case PhysicalType::STRUCT: {
		auto &source_entries = StructVector::GetEntries(source);
		auto &target_entries = StructVector::GetEntries(result);
		UnnestValidity(vdata, start, end, result);
		for (idx_t i = 0; i < source_entries.size(); i++) {
			VectorData sdata;
			source_entries[i]->Orrify(list_size, sdata);
			UnnestVector(sdata, *source_entries[i], list_size, start, end, *target_entries[i]);
		}
		break;
	}
	default:
		throw InternalException("Unimplemented type for UNNEST");
	}
}

unique_ptr<OperatorState> PhysicalUnnest::GetOperatorState(ExecutionContext &context) const {
	return PhysicalUnnest::GetState(context, select_list);
}

unique_ptr<OperatorState> PhysicalUnnest::GetState(ExecutionContext &context,
                                                   const vector<unique_ptr<Expression>> &select_list) {
	return make_unique<UnnestOperatorState>(Allocator::Get(context.client), select_list);
}

OperatorResultType PhysicalUnnest::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                   OperatorState &state_p,
                                                   const vector<unique_ptr<Expression>> &select_list,
                                                   bool include_input) {
	auto &state = (UnnestOperatorState &)state_p;
	do {
		if (state.first_fetch) {
			// get the list data to unnest
			state.list_data.Reset();
			state.executor.Execute(input, state.list_data);

			// paranoia aplenty
			state.list_data.Verify();
			D_ASSERT(input.size() == state.list_data.size());
			D_ASSERT(state.list_data.ColumnCount() == select_list.size());
			D_ASSERT(state.list_vector_data.size() == state.list_data.ColumnCount());
			D_ASSERT(state.list_child_data.size() == state.list_data.ColumnCount());

			// initialize VectorData object so the nullmask can accessed
			for (idx_t col_idx = 0; col_idx < state.list_data.ColumnCount(); col_idx++) {
				auto &list_vector = state.list_data.data[col_idx];
				list_vector.Orrify(state.list_data.size(), state.list_vector_data[col_idx]);

				if (list_vector.GetType() == LogicalType::SQLNULL) {
					// UNNEST(NULL)
					auto &child_vector = list_vector;
					child_vector.Orrify(0, state.list_child_data[col_idx]);
				} else {
					auto list_size = ListVector::GetListSize(list_vector);
					auto &child_vector = ListVector::GetEntry(list_vector);
					child_vector.Orrify(list_size, state.list_child_data[col_idx]);
				}
			}
			state.first_fetch = false;
		}
		if (state.parent_position >= input.size()) {
			// finished with this input chunk
			state.parent_position = 0;
			state.list_position = 0;
			state.list_length = -1;
			state.first_fetch = true;
			return OperatorResultType::NEED_MORE_INPUT;
		}

		// need to figure out how many times we need to repeat for current row
		if (state.list_length < 0) {
			for (idx_t col_idx = 0; col_idx < state.list_data.ColumnCount(); col_idx++) {
				auto &vdata = state.list_vector_data[col_idx];
				auto current_idx = vdata.sel->get_index(state.parent_position);

				int64_t list_length;
				// deal with NULL values
				if (!vdata.validity.RowIsValid(current_idx)) {
					list_length = 0;
				} else {
					auto list_data = (list_entry_t *)vdata.data;
					auto list_entry = list_data[current_idx];
					list_length = (int64_t)list_entry.length;
				}

				if (list_length > state.list_length) {
					state.list_length = list_length;
				}
			}
		}

		D_ASSERT(state.list_length >= 0);

		auto this_chunk_len = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.list_length - state.list_position);

		// first cols are from child, last n cols from unnest
		chunk.SetCardinality(this_chunk_len);

		idx_t output_offset = 0;
		if (include_input) {
			for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
				ConstantVector::Reference(chunk.data[col_idx], input.data[col_idx], state.parent_position,
				                          input.size());
			}
			output_offset = input.ColumnCount();
		}

		for (idx_t col_idx = 0; col_idx < state.list_data.ColumnCount(); col_idx++) {
			auto &result_vector = chunk.data[col_idx + output_offset];

			if (state.list_data.data[col_idx].GetType() == LogicalType::SQLNULL) {
				// UNNEST(NULL)
				chunk.SetCardinality(0);
			} else {
				auto &vdata = state.list_vector_data[col_idx];
				auto &child_data = state.list_child_data[col_idx];
				auto current_idx = vdata.sel->get_index(state.parent_position);

				auto list_data = (list_entry_t *)vdata.data;
				auto list_entry = list_data[current_idx];

				idx_t list_count;
				if (state.list_position >= list_entry.length) {
					list_count = 0;
				} else {
					list_count = MinValue<idx_t>(this_chunk_len, list_entry.length - state.list_position);
				}

				if (list_entry.length > state.list_position) {
					if (!vdata.validity.RowIsValid(current_idx)) {
						UnnestNull(0, list_count, result_vector);
					} else {
						auto &list_vector = state.list_data.data[col_idx];
						auto &child_vector = ListVector::GetEntry(list_vector);
						auto list_size = ListVector::GetListSize(list_vector);

						auto base_offset = list_entry.offset + state.list_position;
						UnnestVector(child_data, child_vector, list_size, base_offset, base_offset + list_count,
						             result_vector);
					}
				}

				UnnestNull(list_count, this_chunk_len, result_vector);
			}
		}

		state.list_position += this_chunk_len;
		if ((int64_t)state.list_position == state.list_length) {
			state.parent_position++;
			state.list_length = -1;
			state.list_position = 0;
		}

		chunk.Verify();
	} while (chunk.size() == 0);
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

OperatorResultType PhysicalUnnest::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                           GlobalOperatorState &gstate, OperatorState &state) const {
	return ExecuteInternal(context, input, chunk, state, select_list);
}

} // namespace duckdb




namespace duckdb {

class PhysicalChunkScanState : public GlobalSourceState {
public:
	explicit PhysicalChunkScanState() : chunk_index(0) {
	}

	//! The current position in the scan
	idx_t chunk_index;
};

unique_ptr<GlobalSourceState> PhysicalChunkScan::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<PhysicalChunkScanState>();
}

void PhysicalChunkScan::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                LocalSourceState &lstate) const {
	auto &state = (PhysicalChunkScanState &)gstate;
	D_ASSERT(collection);
	if (collection->Count() == 0) {
		return;
	}
	D_ASSERT(chunk.GetTypes() == collection->Types());
	if (state.chunk_index >= collection->ChunkCount()) {
		return;
	}
	auto &collection_chunk = collection->GetChunk(state.chunk_index);
	chunk.Reference(collection_chunk);
	state.chunk_index++;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalChunkScan::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
	// check if there is any additional action we need to do depending on the type
	switch (type) {
	case PhysicalOperatorType::DELIM_SCAN: {
		auto entry = state.delim_join_dependencies.find(this);
		D_ASSERT(entry != state.delim_join_dependencies.end());
		// this chunk scan introduces a dependency to the current pipeline
		// namely a dependency on the duplicate elimination pipeline to finish
		auto delim_dependency = entry->second->shared_from_this();
		auto delim_sink = state.GetPipelineSink(*delim_dependency);
		D_ASSERT(delim_sink);
		D_ASSERT(delim_sink->type == PhysicalOperatorType::DELIM_JOIN);
		auto &delim_join = (PhysicalDelimJoin &)*delim_sink;
		current.AddDependency(delim_dependency);
		state.SetPipelineSource(current, (PhysicalOperator *)delim_join.distinct.get());
		return;
	}
	case PhysicalOperatorType::RECURSIVE_CTE_SCAN:
		if (!state.recursive_cte) {
			throw InternalException("Recursive CTE scan found without recursive CTE node");
		}
		break;
	default:
		break;
	}
	D_ASSERT(children.empty());
	state.SetPipelineSource(current, this);
}

} // namespace duckdb


namespace duckdb {

class DummyScanState : public GlobalSourceState {
public:
	DummyScanState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalDummyScan::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<DummyScanState>();
}

void PhysicalDummyScan::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                LocalSourceState &lstate) const {
	auto &state = (DummyScanState &)gstate;
	if (state.finished) {
		return;
	}
	// return a single row on the first call to the dummy scan
	chunk.SetCardinality(1);
	state.finished = true;
}

} // namespace duckdb


namespace duckdb {

void PhysicalEmptyResult::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                  LocalSourceState &lstate) const {
}

} // namespace duckdb




namespace duckdb {

class ExpressionScanState : public OperatorState {
public:
	explicit ExpressionScanState(Allocator &allocator, const PhysicalExpressionScan &op) : expression_index(0) {
		temp_chunk.Initialize(allocator, op.GetTypes());
	}

	//! The current position in the scan
	idx_t expression_index;
	//! Temporary chunk for evaluating expressions
	DataChunk temp_chunk;
};

unique_ptr<OperatorState> PhysicalExpressionScan::GetOperatorState(ExecutionContext &context) const {
	return make_unique<ExpressionScanState>(Allocator::Get(context.client), *this);
}

OperatorResultType PhysicalExpressionScan::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                   GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = (ExpressionScanState &)state_p;

	for (; chunk.size() + input.size() <= STANDARD_VECTOR_SIZE && state.expression_index < expressions.size();
	     state.expression_index++) {
		state.temp_chunk.Reset();
		EvaluateExpression(Allocator::Get(context.client), state.expression_index, &input, state.temp_chunk);
		chunk.Append(state.temp_chunk);
	}
	if (state.expression_index < expressions.size()) {
		return OperatorResultType::HAVE_MORE_OUTPUT;
	} else {
		state.expression_index = 0;
		return OperatorResultType::NEED_MORE_INPUT;
	}
}

void PhysicalExpressionScan::EvaluateExpression(Allocator &allocator, idx_t expression_idx, DataChunk *child_chunk,
                                                DataChunk &result) const {
	ExpressionExecutor executor(allocator, expressions[expression_idx]);
	if (child_chunk) {
		child_chunk->Verify();
		executor.Execute(*child_chunk, result);
	} else {
		executor.Execute(result);
	}
}

bool PhysicalExpressionScan::IsFoldable() const {
	for (auto &expr_list : expressions) {
		for (auto &expr : expr_list) {
			if (!expr->IsFoldable()) {
				return false;
			}
		}
	}
	return true;
}

} // namespace duckdb







#include <utility>

namespace duckdb {

PhysicalTableScan::PhysicalTableScan(vector<LogicalType> types, TableFunction function_p,
                                     unique_ptr<FunctionData> bind_data_p, vector<column_t> column_ids_p,
                                     vector<string> names_p, unique_ptr<TableFilterSet> table_filters_p,
                                     idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::TABLE_SCAN, move(types), estimated_cardinality),
      function(move(function_p)), bind_data(move(bind_data_p)), column_ids(move(column_ids_p)), names(move(names_p)),
      table_filters(move(table_filters_p)) {
}

class TableScanGlobalSourceState : public GlobalSourceState {
public:
	TableScanGlobalSourceState(ClientContext &context, const PhysicalTableScan &op) {
		if (op.function.init_global) {
			TableFunctionInitInput input(op.bind_data.get(), op.column_ids, op.table_filters.get());
			global_state = op.function.init_global(context, input);
			if (global_state) {
				max_threads = global_state->MaxThreads();
			}
		} else {
			max_threads = 1;
		}
	}

	idx_t max_threads = 0;
	unique_ptr<GlobalTableFunctionState> global_state;

	idx_t MaxThreads() override {
		return max_threads;
	}
};

class TableScanLocalSourceState : public LocalSourceState {
public:
	TableScanLocalSourceState(ExecutionContext &context, TableScanGlobalSourceState &gstate,
	                          const PhysicalTableScan &op) {
		if (op.function.init_local) {
			TableFunctionInitInput input(op.bind_data.get(), op.column_ids, op.table_filters.get());
			local_state = op.function.init_local(context, input, gstate.global_state.get());
		}
	}

	unique_ptr<LocalTableFunctionState> local_state;
};

unique_ptr<LocalSourceState> PhysicalTableScan::GetLocalSourceState(ExecutionContext &context,
                                                                    GlobalSourceState &gstate) const {
	return make_unique<TableScanLocalSourceState>(context, (TableScanGlobalSourceState &)gstate, *this);
}

unique_ptr<GlobalSourceState> PhysicalTableScan::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<TableScanGlobalSourceState>(context, *this);
}

void PhysicalTableScan::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                LocalSourceState &lstate) const {
	D_ASSERT(!column_ids.empty());
	auto &gstate = (TableScanGlobalSourceState &)gstate_p;
	auto &state = (TableScanLocalSourceState &)lstate;

	TableFunctionInput data(bind_data.get(), state.local_state.get(), gstate.global_state.get());
	function.function(context.client, data, chunk);
}

double PhysicalTableScan::GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const {
	auto &gstate = (TableScanGlobalSourceState &)gstate_p;
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
	auto &gstate = (TableScanGlobalSourceState &)gstate_p;
	auto &state = (TableScanLocalSourceState &)lstate;
	return function.get_batch_index(context.client, bind_data.get(), state.local_state.get(),
	                                gstate.global_state.get());
}

string PhysicalTableScan::GetName() const {
	return StringUtil::Upper(function.name);
}

string PhysicalTableScan::ParamsToString() const {
	string result;
	if (function.to_string) {
		result = function.to_string(bind_data.get());
		result += "\n[INFOSEPARATOR]\n";
	}
	if (function.projection_pushdown) {
		for (idx_t i = 0; i < column_ids.size(); i++) {
			if (column_ids[i] < names.size()) {
				if (i > 0) {
					result += "\n";
				}
				result += names[column_ids[i]];
			}
		}
	}
	if (function.filter_pushdown && table_filters) {
		result += "\n[INFOSEPARATOR]\n";
		result += "Filters: ";
		for (auto &f : table_filters->filters) {
			auto &column_index = f.first;
			auto &filter = f.second;
			if (column_index < names.size()) {
				result += filter->ToString(names[column_ids[column_index]]);
				result += "\n";
			}
		}
	}
	return result;
}

bool PhysicalTableScan::Equals(const PhysicalOperator &other_p) const {
	if (type != other_p.type) {
		return false;
	}
	auto &other = (PhysicalTableScan &)other_p;
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

} // namespace duckdb




namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class AlterSourceState : public GlobalSourceState {
public:
	AlterSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalAlter::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<AlterSourceState>();
}

void PhysicalAlter::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                            LocalSourceState &lstate) const {
	auto &state = (AlterSourceState &)gstate;
	if (state.finished) {
		return;
	}
	auto &catalog = Catalog::GetCatalog(context.client);
	catalog.Alter(context.client, info.get());
	state.finished = true;
}

} // namespace duckdb





namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateFunctionSourceState : public GlobalSourceState {
public:
	CreateFunctionSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCreateFunction::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<CreateFunctionSourceState>();
}

void PhysicalCreateFunction::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                     LocalSourceState &lstate) const {
	auto &state = (CreateFunctionSourceState &)gstate;
	if (state.finished) {
		return;
	}
	Catalog::GetCatalog(context.client).CreateFunction(context.client, info.get());
	state.finished = true;
}

} // namespace duckdb







namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateIndexSourceState : public GlobalSourceState {
public:
	CreateIndexSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCreateIndex::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<CreateIndexSourceState>();
}

void PhysicalCreateIndex::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                  LocalSourceState &lstate) const {
	auto &state = (CreateIndexSourceState &)gstate;
	if (state.finished) {
		return;
	}
	if (column_ids.empty()) {
		throw BinderException("CREATE INDEX does not refer to any columns in the base table!");
	}

	auto &schema = *table.schema;
	auto index_entry = (IndexCatalogEntry *)schema.CreateIndex(context.client, info.get(), &table);
	if (!index_entry) {
		// index already exists, but error ignored because of IF NOT EXISTS
		return;
	}

	unique_ptr<Index> index;
	switch (info->index_type) {
	case IndexType::ART: {
		index = make_unique<ART>(column_ids, unbound_expressions,
		                         info->unique ? IndexConstraintType::UNIQUE : IndexConstraintType::NONE);
		break;
	}
	default:
		throw InternalException("Unimplemented index type");
	}
	index_entry->index = index.get();
	index_entry->info = table.storage->info;
	table.storage->AddIndex(move(index), expressions);

	chunk.SetCardinality(0);
	state.finished = true;
}

} // namespace duckdb



namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateSchemaSourceState : public GlobalSourceState {
public:
	CreateSchemaSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCreateSchema::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<CreateSchemaSourceState>();
}

void PhysicalCreateSchema::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                   LocalSourceState &lstate) const {
	auto &state = (CreateSchemaSourceState &)gstate;
	if (state.finished) {
		return;
	}
	Catalog::GetCatalog(context.client).CreateSchema(context.client, info.get());
	state.finished = true;
}

} // namespace duckdb



namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateSequenceSourceState : public GlobalSourceState {
public:
	CreateSequenceSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCreateSequence::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<CreateSequenceSourceState>();
}

void PhysicalCreateSequence::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                     LocalSourceState &lstate) const {
	auto &state = (CreateSequenceSourceState &)gstate;
	if (state.finished) {
		return;
	}
	Catalog::GetCatalog(context.client).CreateSequence(context.client, info.get());
	state.finished = true;
}

} // namespace duckdb







namespace duckdb {

PhysicalCreateTable::PhysicalCreateTable(LogicalOperator &op, SchemaCatalogEntry *schema,
                                         unique_ptr<BoundCreateTableInfo> info, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::CREATE_TABLE, op.types, estimated_cardinality), schema(schema),
      info(move(info)) {
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateTableSourceState : public GlobalSourceState {
public:
	CreateTableSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCreateTable::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<CreateTableSourceState>();
}

void PhysicalCreateTable::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                  LocalSourceState &lstate) const {
	auto &state = (CreateTableSourceState &)gstate;
	if (state.finished) {
		return;
	}
	auto &catalog = Catalog::GetCatalog(context.client);
	catalog.CreateTable(context.client, schema, info.get());
	state.finished = true;
}

} // namespace duckdb






namespace duckdb {

PhysicalCreateTableAs::PhysicalCreateTableAs(LogicalOperator &op, SchemaCatalogEntry *schema,
                                             unique_ptr<BoundCreateTableInfo> info, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::CREATE_TABLE_AS, op.types, estimated_cardinality), schema(schema),
      info(move(info)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class CreateTableAsGlobalState : public GlobalSinkState {
public:
	CreateTableAsGlobalState() {
		inserted_count = 0;
	}

	mutex append_lock;
	TableCatalogEntry *table;
	int64_t inserted_count;
};

unique_ptr<GlobalSinkState> PhysicalCreateTableAs::GetGlobalSinkState(ClientContext &context) const {
	auto sink = make_unique<CreateTableAsGlobalState>();
	auto &catalog = Catalog::GetCatalog(context);
	sink->table = (TableCatalogEntry *)catalog.CreateTable(context, schema, info.get());
	return move(sink);
}

SinkResultType PhysicalCreateTableAs::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate_p,
                                           DataChunk &input) const {
	auto &sink = (CreateTableAsGlobalState &)state;
	if (sink.table) {
		lock_guard<mutex> client_guard(sink.append_lock);
		sink.table->storage->Append(*sink.table, context.client, input);
		sink.inserted_count += input.size();
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateTableAsSourceState : public GlobalSourceState {
public:
	CreateTableAsSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCreateTableAs::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<CreateTableAsSourceState>();
}

void PhysicalCreateTableAs::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                    LocalSourceState &lstate) const {
	auto &state = (CreateTableAsSourceState &)gstate;
	auto &sink = (CreateTableAsGlobalState &)*sink_state;
	if (state.finished) {
		return;
	}
	if (sink.table) {
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(sink.inserted_count));
	}
	state.finished = true;
}

} // namespace duckdb




namespace duckdb {

PhysicalCreateType::PhysicalCreateType(unique_ptr<CreateTypeInfo> info, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::CREATE_TYPE, {LogicalType::BIGINT}, estimated_cardinality),
      info(move(info)) {
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateTypeSourceState : public GlobalSourceState {
public:
	CreateTypeSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCreateType::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<CreateTypeSourceState>();
}

void PhysicalCreateType::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                 LocalSourceState &lstate) const {
	auto &state = (CreateTypeSourceState &)gstate;
	if (state.finished) {
		return;
	}
	auto &catalog = Catalog::GetCatalog(context.client);
	catalog.CreateType(context.client, info.get());
	state.finished = true;
}

} // namespace duckdb



namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateViewSourceState : public GlobalSourceState {
public:
	CreateViewSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCreateView::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<CreateViewSourceState>();
}

void PhysicalCreateView::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                 LocalSourceState &lstate) const {
	auto &state = (CreateViewSourceState &)gstate;
	if (state.finished) {
		return;
	}
	Catalog::GetCatalog(context.client).CreateView(context.client, info.get());
	state.finished = true;
}

} // namespace duckdb



namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class DropSourceState : public GlobalSourceState {
public:
	DropSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalDrop::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<DropSourceState>();
}

void PhysicalDrop::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                           LocalSourceState &lstate) const {
	auto &state = (DropSourceState &)gstate;
	if (state.finished) {
		return;
	}
	switch (info->type) {
	case CatalogType::PREPARED_STATEMENT: {
		// DEALLOCATE silently ignores errors
		auto &statements = ClientData::Get(context.client).prepared_statements;
		if (statements.find(info->name) != statements.end()) {
			statements.erase(info->name);
		}
		break;
	}
	default:
		Catalog::GetCatalog(context.client).DropEntry(context.client, info.get());
		break;
	}
	state.finished = true;
}

} // namespace duckdb













namespace duckdb {

PhysicalRecursiveCTE::PhysicalRecursiveCTE(vector<LogicalType> types, bool union_all, unique_ptr<PhysicalOperator> top,
                                           unique_ptr<PhysicalOperator> bottom, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::RECURSIVE_CTE, move(types), estimated_cardinality), union_all(union_all) {
	children.push_back(move(top));
	children.push_back(move(bottom));
}

PhysicalRecursiveCTE::~PhysicalRecursiveCTE() {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class RecursiveCTEState : public GlobalSinkState {
public:
	explicit RecursiveCTEState(ClientContext &context, const PhysicalRecursiveCTE &op)
	    : intermediate_table(Allocator::Get(context)), new_groups(STANDARD_VECTOR_SIZE) {
		ht = make_unique<GroupedAggregateHashTable>(Allocator::Get(context), BufferManager::GetBufferManager(context),
		                                            op.types, vector<LogicalType>(),
		                                            vector<BoundAggregateExpression *>());
	}

	unique_ptr<GroupedAggregateHashTable> ht;

	bool intermediate_empty = true;
	ChunkCollection intermediate_table;
	idx_t chunk_idx = 0;
	SelectionVector new_groups;
};

unique_ptr<GlobalSinkState> PhysicalRecursiveCTE::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<RecursiveCTEState>(context, *this);
}

idx_t PhysicalRecursiveCTE::ProbeHT(DataChunk &chunk, RecursiveCTEState &state) const {
	Vector dummy_addresses(LogicalType::POINTER);

	// Use the HT to eliminate duplicate rows
	idx_t new_group_count = state.ht->FindOrCreateGroups(chunk, dummy_addresses, state.new_groups);

	// we only return entries we have not seen before (i.e. new groups)
	chunk.Slice(state.new_groups, new_group_count);

	return new_group_count;
}

SinkResultType PhysicalRecursiveCTE::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                          DataChunk &input) const {
	auto &gstate = (RecursiveCTEState &)state;
	if (!union_all) {
		idx_t match_count = ProbeHT(input, gstate);
		if (match_count > 0) {
			gstate.intermediate_table.Append(input);
		}
	} else {
		gstate.intermediate_table.Append(input);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
void PhysicalRecursiveCTE::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                   LocalSourceState &lstate) const {
	auto &gstate = (RecursiveCTEState &)*sink_state;
	while (chunk.size() == 0) {
		if (gstate.chunk_idx < gstate.intermediate_table.ChunkCount()) {
			// scan any chunks we have collected so far
			chunk.Reference(gstate.intermediate_table.GetChunk(gstate.chunk_idx));
			gstate.chunk_idx++;
			break;
		} else {
			// we have run out of chunks
			// now we need to recurse
			// we set up the working table as the data we gathered in this iteration of the recursion
			working_table->Reset();
			working_table->Merge(gstate.intermediate_table);
			// and we clear the intermediate table
			gstate.intermediate_table.Reset();
			gstate.chunk_idx = 0;
			// now we need to re-execute all of the pipelines that depend on the recursion
			ExecuteRecursivePipelines(context);

			// check if we obtained any results
			// if not, we are done
			if (gstate.intermediate_table.Count() == 0) {
				break;
			}
		}
	}
}

void PhysicalRecursiveCTE::ExecuteRecursivePipelines(ExecutionContext &context) const {
	if (pipelines.empty()) {
		throw InternalException("Missing pipelines for recursive CTE");
	}

	for (auto &pipeline : pipelines) {
		auto sink = pipeline->GetSink();
		if (sink != this) {
			// reset the sink state for any intermediate sinks
			sink->sink_state = sink->GetGlobalSinkState(context.client);
		}
		for (auto &op : pipeline->GetOperators()) {
			if (op) {
				op->op_state = op->GetGlobalOperatorState(context.client);
			}
		}
		pipeline->Reset();
	}
	auto &executor = pipelines[0]->executor;

	vector<shared_ptr<Event>> events;
	executor.ReschedulePipelines(pipelines, events);

	while (true) {
		executor.WorkOnTasks();
		if (executor.HasError()) {
			executor.ThrowException();
		}
		bool finished = true;
		for (auto &event : events) {
			if (!event->IsFinished()) {
				finished = false;
				break;
			}
		}
		if (finished) {
			// all pipelines finished: done!
			break;
		}
	}
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalRecursiveCTE::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
	op_state.reset();
	sink_state.reset();

	// recursive CTE
	state.SetPipelineSource(current, this);
	// the LHS of the recursive CTE is our initial state
	// we build this pipeline as normal
	auto pipeline_child = children[0].get();
	// for the RHS, we gather all pipelines that depend on the recursive cte
	// these pipelines need to be rerun
	if (state.recursive_cte) {
		throw InternalException("Recursive CTE detected WITHIN a recursive CTE node");
	}
	state.recursive_cte = this;

	auto recursive_pipeline = make_shared<Pipeline>(executor);
	state.SetPipelineSink(*recursive_pipeline, this);
	children[1]->BuildPipelines(executor, *recursive_pipeline, state);

	pipelines.push_back(move(recursive_pipeline));

	state.recursive_cte = nullptr;

	BuildChildPipeline(executor, current, state, pipeline_child);
}

vector<const PhysicalOperator *> PhysicalRecursiveCTE::GetSources() const {
	return {this};
}

} // namespace duckdb




namespace duckdb {

PhysicalUnion::PhysicalUnion(vector<LogicalType> types, unique_ptr<PhysicalOperator> top,
                             unique_ptr<PhysicalOperator> bottom, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::UNION, move(types), estimated_cardinality) {
	children.push_back(move(top));
	children.push_back(move(bottom));
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalUnion::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
	if (state.recursive_cte) {
		throw NotImplementedException("UNIONS are not supported in recursive CTEs yet");
	}
	op_state.reset();
	sink_state.reset();

	auto union_pipeline = make_shared<Pipeline>(executor);
	auto pipeline_ptr = union_pipeline.get();
	auto &child_pipelines = state.GetChildPipelines(executor);
	auto &child_dependencies = state.GetChildDependencies(executor);
	auto &union_pipelines = state.GetUnionPipelines(executor);
	// set up dependencies for any child pipelines to this union pipeline
	auto child_entry = child_pipelines.find(&current);
	if (child_entry != child_pipelines.end()) {
		for (auto &current_child : child_entry->second) {
			D_ASSERT(child_dependencies.find(current_child.get()) != child_dependencies.end());
			child_dependencies[current_child.get()].push_back(pipeline_ptr);
		}
	}
	// for the current pipeline, continue building on the LHS
	state.SetPipelineOperators(*union_pipeline, state.GetPipelineOperators(current));
	children[0]->BuildPipelines(executor, current, state);
	// insert the union pipeline as a union pipeline of the current node
	union_pipelines[&current].push_back(move(union_pipeline));

	// for the union pipeline, build on the RHS
	state.SetPipelineSink(*pipeline_ptr, state.GetPipelineSink(current));
	children[1]->BuildPipelines(executor, *pipeline_ptr, state);
}

vector<const PhysicalOperator *> PhysicalUnion::GetSources() const {
	vector<const PhysicalOperator *> result;
	for (auto &child : children) {
		auto child_sources = child->GetSources();
		result.insert(result.end(), child_sources.begin(), child_sources.end());
	}
	return result;
}

} // namespace duckdb


namespace duckdb {

static idx_t PartitionInfoNPartitions(const idx_t n_partitions_upper_bound) {
	idx_t n_partitions = 1;
	while (n_partitions <= n_partitions_upper_bound / 2) {
		n_partitions *= 2;
		if (n_partitions >= 256) {
			break;
		}
	}
	return n_partitions;
}

static idx_t PartitionInfoRadixBits(const idx_t n_partitions) {
	idx_t radix_bits = 0;
	auto radix_partitions_copy = n_partitions;
	while (radix_partitions_copy - 1) {
		radix_bits++;
		radix_partitions_copy >>= 1;
	}
	return radix_bits;
}

static hash_t PartitionInfoRadixMask(const idx_t radix_bits, const idx_t radix_shift) {
	hash_t radix_mask = 0;
	// we use the fifth byte of the 64 bit hash as radix source
	for (idx_t i = 0; i < radix_bits; i++) {
		radix_mask = (radix_mask << 1) | 1;
	}
	radix_mask <<= radix_shift;
	return radix_mask;
}

RadixPartitionInfo::RadixPartitionInfo(const idx_t n_partitions_upper_bound)
    : n_partitions(PartitionInfoNPartitions(n_partitions_upper_bound)),
      radix_bits(PartitionInfoRadixBits(n_partitions)), radix_mask(PartitionInfoRadixMask(radix_bits, RADIX_SHIFT)) {

	// finalize_threads needs to be a power of 2
	D_ASSERT(n_partitions > 0);
	D_ASSERT(n_partitions <= 256);
	D_ASSERT((n_partitions & (n_partitions - 1)) == 0);
	D_ASSERT(radix_bits <= 8);
}

PartitionableHashTable::PartitionableHashTable(Allocator &allocator, BufferManager &buffer_manager_p,
                                               RadixPartitionInfo &partition_info_p, vector<LogicalType> group_types_p,
                                               vector<LogicalType> payload_types_p,
                                               vector<BoundAggregateExpression *> bindings_p)
    : allocator(allocator), buffer_manager(buffer_manager_p), group_types(move(group_types_p)),
      payload_types(move(payload_types_p)), bindings(move(bindings_p)), is_partitioned(false),
      partition_info(partition_info_p), hashes(LogicalType::HASH), hashes_subset(LogicalType::HASH) {

	sel_vectors.resize(partition_info.n_partitions);
	sel_vector_sizes.resize(partition_info.n_partitions);
	group_subset.Initialize(allocator, group_types);
	if (!payload_types.empty()) {
		payload_subset.Initialize(allocator, payload_types);
	}

	for (hash_t r = 0; r < partition_info.n_partitions; r++) {
		sel_vectors[r].Initialize();
	}
}

idx_t PartitionableHashTable::ListAddChunk(HashTableList &list, DataChunk &groups, Vector &group_hashes,
                                           DataChunk &payload) {
	if (list.empty() || list.back()->Size() + groups.size() > list.back()->MaxCapacity()) {
		if (!list.empty()) {
			// early release first part of ht and prevent adding of more data
			list.back()->Finalize();
		}
		list.push_back(make_unique<GroupedAggregateHashTable>(allocator, buffer_manager, group_types, payload_types,
		                                                      bindings, HtEntryType::HT_WIDTH_32));
	}
	return list.back()->AddChunk(groups, group_hashes, payload);
}

idx_t PartitionableHashTable::AddChunk(DataChunk &groups, DataChunk &payload, bool do_partition) {
	groups.Hash(hashes);

	// we partition when we are asked to or when the unpartitioned ht runs out of space
	if (!IsPartitioned() && do_partition) {
		Partition();
	}

	if (!IsPartitioned()) {
		return ListAddChunk(unpartitioned_hts, groups, hashes, payload);
	}

	// makes no sense to do this with 1 partition
	D_ASSERT(partition_info.n_partitions > 0);

	for (hash_t r = 0; r < partition_info.n_partitions; r++) {
		sel_vector_sizes[r] = 0;
	}

	hashes.Normalify(groups.size());
	auto hashes_ptr = FlatVector::GetData<hash_t>(hashes);

	for (idx_t i = 0; i < groups.size(); i++) {
		auto partition = (hashes_ptr[i] & partition_info.radix_mask) >> partition_info.RADIX_SHIFT;
		D_ASSERT(partition < partition_info.n_partitions);
		sel_vectors[partition].set_index(sel_vector_sizes[partition]++, i);
	}

#ifdef DEBUG
	// make sure we have lost no rows
	idx_t total_count = 0;
	for (idx_t r = 0; r < partition_info.n_partitions; r++) {
		total_count += sel_vector_sizes[r];
	}
	D_ASSERT(total_count == groups.size());
#endif
	idx_t group_count = 0;
	for (hash_t r = 0; r < partition_info.n_partitions; r++) {
		group_subset.Slice(groups, sel_vectors[r], sel_vector_sizes[r]);
		payload_subset.Slice(payload, sel_vectors[r], sel_vector_sizes[r]);
		hashes_subset.Slice(hashes, sel_vectors[r], sel_vector_sizes[r]);

		group_count += ListAddChunk(radix_partitioned_hts[r], group_subset, hashes_subset, payload_subset);
	}
	return group_count;
}

void PartitionableHashTable::Partition() {
	D_ASSERT(!IsPartitioned());
	D_ASSERT(radix_partitioned_hts.size() == 0);
	D_ASSERT(partition_info.n_partitions > 1);

	vector<GroupedAggregateHashTable *> partition_hts(partition_info.n_partitions);
	for (auto &unpartitioned_ht : unpartitioned_hts) {
		for (idx_t r = 0; r < partition_info.n_partitions; r++) {
			radix_partitioned_hts[r].push_back(make_unique<GroupedAggregateHashTable>(
			    allocator, buffer_manager, group_types, payload_types, bindings, HtEntryType::HT_WIDTH_32));
			partition_hts[r] = radix_partitioned_hts[r].back().get();
		}
		unpartitioned_ht->Partition(partition_hts, partition_info.radix_mask, partition_info.RADIX_SHIFT);
		unpartitioned_ht.reset();
	}
	unpartitioned_hts.clear();
	is_partitioned = true;
}

bool PartitionableHashTable::IsPartitioned() {
	return is_partitioned;
}

HashTableList PartitionableHashTable::GetPartition(idx_t partition) {
	D_ASSERT(IsPartitioned());
	D_ASSERT(partition < partition_info.n_partitions);
	D_ASSERT(radix_partitioned_hts.size() > partition);
	return move(radix_partitioned_hts[partition]);
}
HashTableList PartitionableHashTable::GetUnpartitioned() {
	D_ASSERT(!IsPartitioned());
	return move(unpartitioned_hts);
}

void PartitionableHashTable::Finalize() {
	if (IsPartitioned()) {
		for (auto &ht_list : radix_partitioned_hts) {
			for (auto &ht : ht_list.second) {
				D_ASSERT(ht);
				ht->Finalize();
			}
		}
	} else {
		for (auto &ht : unpartitioned_hts) {
			D_ASSERT(ht);
			ht->Finalize();
		}
	}
}

} // namespace duckdb




namespace duckdb {

PerfectAggregateHashTable::PerfectAggregateHashTable(Allocator &allocator, BufferManager &buffer_manager,
                                                     const vector<LogicalType> &group_types_p,
                                                     vector<LogicalType> payload_types_p,
                                                     vector<AggregateObject> aggregate_objects_p,
                                                     vector<Value> group_minima_p, vector<idx_t> required_bits_p)
    : BaseAggregateHashTable(allocator, aggregate_objects_p, buffer_manager, move(payload_types_p)),
      addresses(LogicalType::POINTER), required_bits(move(required_bits_p)), total_required_bits(0),
      group_minima(move(group_minima_p)), sel(STANDARD_VECTOR_SIZE) {
	for (auto &group_bits : required_bits) {
		total_required_bits += group_bits;
	}
	// the total amount of groups we allocate space for is 2^required_bits
	total_groups = 1 << total_required_bits;
	// we don't need to store the groups in a perfect hash table, since the group keys can be deduced by their location
	grouping_columns = group_types_p.size();
	layout.Initialize(move(aggregate_objects_p));
	tuple_size = layout.GetRowWidth();

	// allocate and null initialize the data
	owned_data = unique_ptr<data_t[]>(new data_t[tuple_size * total_groups]);
	data = owned_data.get();

	// set up the empty payloads for every tuple, and initialize the "occupied" flag to false
	group_is_set = unique_ptr<bool[]>(new bool[total_groups]);
	memset(group_is_set.get(), 0, total_groups * sizeof(bool));
}

PerfectAggregateHashTable::~PerfectAggregateHashTable() {
	Destroy();
}

template <class T>
static void ComputeGroupLocationTemplated(VectorData &group_data, Value &min, uintptr_t *address_data,
                                          idx_t current_shift, idx_t count) {
	auto data = (T *)group_data.data;
	auto min_val = min.GetValueUnsafe<T>();
	if (!group_data.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto index = group_data.sel->get_index(i);
			// check if the value is NULL
			// NULL groups are considered as "0" in the hash table
			// that is to say, they have no effect on the position of the element (because 0 << shift is 0)
			// we only need to handle non-null values here
			if (group_data.validity.RowIsValid(index)) {
				D_ASSERT(data[index] >= min_val);
				uintptr_t adjusted_value = (data[index] - min_val) + 1;
				address_data[i] += adjusted_value << current_shift;
			}
		}
	} else {
		// no null values: we can directly compute the addresses
		for (idx_t i = 0; i < count; i++) {
			auto index = group_data.sel->get_index(i);
			uintptr_t adjusted_value = (data[index] - min_val) + 1;
			address_data[i] += adjusted_value << current_shift;
		}
	}
}

static void ComputeGroupLocation(Vector &group, Value &min, uintptr_t *address_data, idx_t current_shift, idx_t count) {
	VectorData vdata;
	group.Orrify(count, vdata);

	switch (group.GetType().InternalType()) {
	case PhysicalType::INT8:
		ComputeGroupLocationTemplated<int8_t>(vdata, min, address_data, current_shift, count);
		break;
	case PhysicalType::INT16:
		ComputeGroupLocationTemplated<int16_t>(vdata, min, address_data, current_shift, count);
		break;
	case PhysicalType::INT32:
		ComputeGroupLocationTemplated<int32_t>(vdata, min, address_data, current_shift, count);
		break;
	case PhysicalType::INT64:
		ComputeGroupLocationTemplated<int64_t>(vdata, min, address_data, current_shift, count);
		break;
	default:
		throw InternalException("Unsupported group type for perfect aggregate hash table");
	}
}

void PerfectAggregateHashTable::AddChunk(DataChunk &groups, DataChunk &payload) {
	// first we need to find the location in the HT of each of the groups
	auto address_data = FlatVector::GetData<uintptr_t>(addresses);
	// zero-initialize the address data
	memset(address_data, 0, groups.size() * sizeof(uintptr_t));
	D_ASSERT(groups.ColumnCount() == group_minima.size());

	// then compute the actual group location by iterating over each of the groups
	idx_t current_shift = total_required_bits;
	for (idx_t i = 0; i < groups.ColumnCount(); i++) {
		current_shift -= required_bits[i];
		ComputeGroupLocation(groups.data[i], group_minima[i], address_data, current_shift, groups.size());
	}
	// now we have the HT entry number for every tuple
	// compute the actual pointer to the data by adding it to the base HT pointer and multiplying by the tuple size
	idx_t needs_init = 0;
	for (idx_t i = 0; i < groups.size(); i++) {
		D_ASSERT(address_data[i] < total_groups);
		const auto group = address_data[i];
		address_data[i] = uintptr_t(data) + address_data[i] * tuple_size;
		if (!group_is_set[group]) {
			group_is_set[group] = true;
			sel.set_index(needs_init++, i);
			if (needs_init == STANDARD_VECTOR_SIZE) {
				RowOperations::InitializeStates(layout, addresses, sel, needs_init);
				needs_init = 0;
			}
		}
	}
	RowOperations::InitializeStates(layout, addresses, sel, needs_init);

	// after finding the group location we update the aggregates
	idx_t payload_idx = 0;
	auto &aggregates = layout.GetAggregates();
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggregate = aggregates[aggr_idx];
		auto input_count = (idx_t)aggregate.child_count;
		if (aggregate.filter) {
			RowOperations::UpdateFilteredStates(filter_set.GetFilterData(aggr_idx), aggregate, addresses, payload,
			                                    payload_idx);
		} else {
			RowOperations::UpdateStates(aggregate, addresses, payload, payload_idx, payload.size());
		}
		// move to the next aggregate
		payload_idx += input_count;
		VectorOperations::AddInPlace(addresses, aggregate.payload_size, payload.size());
	}
}

void PerfectAggregateHashTable::Combine(PerfectAggregateHashTable &other) {
	D_ASSERT(total_groups == other.total_groups);
	D_ASSERT(tuple_size == other.tuple_size);

	Vector source_addresses(LogicalType::POINTER);
	Vector target_addresses(LogicalType::POINTER);
	auto source_addresses_ptr = FlatVector::GetData<data_ptr_t>(source_addresses);
	auto target_addresses_ptr = FlatVector::GetData<data_ptr_t>(target_addresses);

	// iterate over all entries of both hash tables and call combine for all entries that can be combined
	data_ptr_t source_ptr = other.data;
	data_ptr_t target_ptr = data;
	idx_t combine_count = 0;
	idx_t reinit_count = 0;
	const auto &reinit_sel = *FlatVector::IncrementalSelectionVector();
	for (idx_t i = 0; i < total_groups; i++) {
		auto has_entry_source = other.group_is_set[i];
		// we only have any work to do if the source has an entry for this group
		if (has_entry_source) {
			auto has_entry_target = group_is_set[i];
			if (has_entry_target) {
				// both source and target have an entry: need to combine
				source_addresses_ptr[combine_count] = source_ptr;
				target_addresses_ptr[combine_count] = target_ptr;
				combine_count++;
				if (combine_count == STANDARD_VECTOR_SIZE) {
					RowOperations::CombineStates(layout, source_addresses, target_addresses, combine_count);
					combine_count = 0;
				}
			} else {
				group_is_set[i] = true;
				// only source has an entry for this group: we can just memcpy it over
				memcpy(target_ptr, source_ptr, tuple_size);
				// we clear this entry in the other HT as we "consume" the entry here
				other.group_is_set[i] = false;
			}
		}
		source_ptr += tuple_size;
		target_ptr += tuple_size;
	}
	RowOperations::CombineStates(layout, source_addresses, target_addresses, combine_count);
	RowOperations::InitializeStates(layout, addresses, reinit_sel, reinit_count);
}

template <class T>
static void ReconstructGroupVectorTemplated(uint32_t group_values[], Value &min, idx_t mask, idx_t shift,
                                            idx_t entry_count, Vector &result) {
	auto data = FlatVector::GetData<T>(result);
	auto &validity_mask = FlatVector::Validity(result);
	auto min_data = min.GetValueUnsafe<T>();
	for (idx_t i = 0; i < entry_count; i++) {
		// extract the value of this group from the total group index
		auto group_index = (group_values[i] >> shift) & mask;
		if (group_index == 0) {
			// if it is 0, the value is NULL
			validity_mask.SetInvalid(i);
		} else {
			// otherwise we add the value (minus 1) to the min value
			data[i] = min_data + group_index - 1;
		}
	}
}

static void ReconstructGroupVector(uint32_t group_values[], Value &min, idx_t required_bits, idx_t shift,
                                   idx_t entry_count, Vector &result) {
	// construct the mask for this entry
	idx_t mask = (1 << required_bits) - 1;
	switch (result.GetType().InternalType()) {
	case PhysicalType::INT8:
		ReconstructGroupVectorTemplated<int8_t>(group_values, min, mask, shift, entry_count, result);
		break;
	case PhysicalType::INT16:
		ReconstructGroupVectorTemplated<int16_t>(group_values, min, mask, shift, entry_count, result);
		break;
	case PhysicalType::INT32:
		ReconstructGroupVectorTemplated<int32_t>(group_values, min, mask, shift, entry_count, result);
		break;
	case PhysicalType::INT64:
		ReconstructGroupVectorTemplated<int64_t>(group_values, min, mask, shift, entry_count, result);
		break;
	default:
		throw InternalException("Invalid type for perfect aggregate HT group");
	}
}

void PerfectAggregateHashTable::Scan(idx_t &scan_position, DataChunk &result) {
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);
	uint32_t group_values[STANDARD_VECTOR_SIZE];

	// iterate over the HT until we either have exhausted the entire HT, or
	idx_t entry_count = 0;
	for (; scan_position < total_groups; scan_position++) {
		if (group_is_set[scan_position]) {
			// this group is set: add it to the set of groups to extract
			data_pointers[entry_count] = data + tuple_size * scan_position;
			group_values[entry_count] = scan_position;
			entry_count++;
			if (entry_count == STANDARD_VECTOR_SIZE) {
				scan_position++;
				break;
			}
		}
	}
	if (entry_count == 0) {
		// no entries found
		return;
	}
	// first reconstruct the groups from the group index
	idx_t shift = total_required_bits;
	for (idx_t i = 0; i < grouping_columns; i++) {
		shift -= required_bits[i];
		ReconstructGroupVector(group_values, group_minima[i], required_bits[i], shift, entry_count, result.data[i]);
	}
	// then construct the payloads
	result.SetCardinality(entry_count);
	RowOperations::FinalizeStates(layout, addresses, result, grouping_columns);
}

void PerfectAggregateHashTable::Destroy() {
	// check if there is any destructor to call
	bool has_destructor = false;
	for (auto &aggr : layout.GetAggregates()) {
		if (aggr.function.destructor) {
			has_destructor = true;
		}
	}
	if (!has_destructor) {
		return;
	}
	// there are aggregates with destructors: loop over the hash table
	// and call the destructor method for each of the aggregates
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);
	idx_t count = 0;

	// iterate over all initialised slots of the hash table
	data_ptr_t payload_ptr = data;
	for (idx_t i = 0; i < total_groups; i++) {
		if (group_is_set[i]) {
			data_pointers[count++] = payload_ptr;
			if (count == STANDARD_VECTOR_SIZE) {
				RowOperations::DestroyStates(layout, addresses, count);
				count = 0;
			}
		}
		payload_ptr += tuple_size;
	}
	RowOperations::DestroyStates(layout, addresses, count);
}

} // namespace duckdb











namespace duckdb {

string PhysicalOperator::GetName() const {
	return PhysicalOperatorToString(type);
}

string PhysicalOperator::ToString() const {
	TreeRenderer renderer;
	return renderer.ToString(*this);
}

// LCOV_EXCL_START
void PhysicalOperator::Print() const {
	Printer::Print(ToString());
}
// LCOV_EXCL_STOP

vector<PhysicalOperator *> PhysicalOperator::GetChildren() const {
	vector<PhysicalOperator *> result;
	for (auto &child : children) {
		result.push_back(child.get());
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
// LCOV_EXCL_START
unique_ptr<OperatorState> PhysicalOperator::GetOperatorState(ExecutionContext &context) const {
	return make_unique<OperatorState>();
}

unique_ptr<GlobalOperatorState> PhysicalOperator::GetGlobalOperatorState(ClientContext &context) const {
	return make_unique<GlobalOperatorState>();
}

OperatorResultType PhysicalOperator::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                             GlobalOperatorState &gstate, OperatorState &state) const {
	throw InternalException("Calling Execute on a node that is not an operator!");
}
// LCOV_EXCL_STOP

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
unique_ptr<LocalSourceState> PhysicalOperator::GetLocalSourceState(ExecutionContext &context,
                                                                   GlobalSourceState &gstate) const {
	return make_unique<LocalSourceState>();
}

unique_ptr<GlobalSourceState> PhysicalOperator::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<GlobalSourceState>();
}

// LCOV_EXCL_START
void PhysicalOperator::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                               LocalSourceState &lstate) const {
	throw InternalException("Calling GetData on a node that is not a source!");
}

idx_t PhysicalOperator::GetBatchIndex(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                      LocalSourceState &lstate) const {
	throw InternalException("Calling GetBatchIndex on a node that does not support it");
}

double PhysicalOperator::GetProgress(ClientContext &context, GlobalSourceState &gstate) const {
	return -1;
}
// LCOV_EXCL_STOP

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
// LCOV_EXCL_START
SinkResultType PhysicalOperator::Sink(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate,
                                      DataChunk &input) const {
	throw InternalException("Calling Sink on a node that is not a sink!");
}
// LCOV_EXCL_STOP

void PhysicalOperator::Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const {
}

SinkFinalizeType PhysicalOperator::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            GlobalSinkState &gstate) const {
	return SinkFinalizeType::READY;
}

unique_ptr<LocalSinkState> PhysicalOperator::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<LocalSinkState>();
}

unique_ptr<GlobalSinkState> PhysicalOperator::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<GlobalSinkState>();
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalOperator::AddPipeline(Executor &executor, shared_ptr<Pipeline> pipeline, PipelineBuildState &state) {
	if (!state.recursive_cte) {
		// regular pipeline: schedule it
		state.AddPipeline(executor, move(pipeline));
	} else {
		// CTE pipeline! add it to the CTE pipelines
		auto &cte = (PhysicalRecursiveCTE &)*state.recursive_cte;
		cte.pipelines.push_back(move(pipeline));
	}
}

void PhysicalOperator::BuildChildPipeline(Executor &executor, Pipeline &current, PipelineBuildState &state,
                                          PhysicalOperator *pipeline_child) {
	auto pipeline = make_shared<Pipeline>(executor);
	state.SetPipelineSink(*pipeline, this);
	// the current is dependent on this pipeline to complete
	current.AddDependency(pipeline);
	// recurse into the pipeline child
	pipeline_child->BuildPipelines(executor, *pipeline, state);
	AddPipeline(executor, move(pipeline), state);
}

void PhysicalOperator::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
	op_state.reset();
	if (IsSink()) {
		// operator is a sink, build a pipeline
		sink_state.reset();

		// single operator:
		// the operator becomes the data source of the current pipeline
		state.SetPipelineSource(current, this);
		// we create a new pipeline starting from the child
		D_ASSERT(children.size() == 1);

		BuildChildPipeline(executor, current, state, children[0].get());
	} else {
		// operator is not a sink! recurse in children
		if (children.empty()) {
			// source
			state.SetPipelineSource(current, this);
		} else {
			if (children.size() != 1) {
				throw InternalException("Operator not supported in BuildPipelines");
			}
			state.AddPipelineOperator(current, this);
			children[0]->BuildPipelines(executor, current, state);
		}
	}
}

vector<const PhysicalOperator *> PhysicalOperator::GetSources() const {
	vector<const PhysicalOperator *> result;
	if (IsSink()) {
		D_ASSERT(children.size() == 1);
		result.push_back(this);
		return result;
	} else {
		if (children.empty()) {
			// source
			result.push_back(this);
			return result;
		} else {
			if (children.size() != 1) {
				throw InternalException("Operator not supported in GetSource");
			}
			return children[0]->GetSources();
		}
	}
}

bool PhysicalOperator::AllSourcesSupportBatchIndex() const {
	auto sources = GetSources();
	for (auto &source : sources) {
		if (!source->SupportsBatchIndex()) {
			return false;
		}
	}
	return true;
}

void PhysicalOperator::Verify() {
#ifdef DEBUG
	auto sources = GetSources();
	D_ASSERT(!sources.empty());
	for (auto &child : children) {
		child->Verify();
	}
#endif
}

} // namespace duckdb












namespace duckdb {

static uint32_t RequiredBitsForValue(uint32_t n) {
	idx_t required_bits = 0;
	while (n > 0) {
		n >>= 1;
		required_bits++;
	}
	return required_bits;
}

static bool CanUsePerfectHashAggregate(ClientContext &context, LogicalAggregate &op, vector<idx_t> &bits_per_group) {
	if (op.grouping_sets.size() > 1 || !op.grouping_functions.empty()) {
		return false;
	}
	idx_t perfect_hash_bits = 0;
	if (op.group_stats.empty()) {
		op.group_stats.resize(op.groups.size());
	}
	for (idx_t group_idx = 0; group_idx < op.groups.size(); group_idx++) {
		auto &group = op.groups[group_idx];
		auto &stats = op.group_stats[group_idx];

		switch (group->return_type.InternalType()) {
		case PhysicalType::INT8:
		case PhysicalType::INT16:
		case PhysicalType::INT32:
		case PhysicalType::INT64:
			break;
		default:
			// we only support simple integer types for perfect hashing
			return false;
		}
		// check if the group has stats available
		auto &group_type = group->return_type;
		if (!stats) {
			// no stats, but we might still be able to use perfect hashing if the type is small enough
			// for small types we can just set the stats to [type_min, type_max]
			switch (group_type.InternalType()) {
			case PhysicalType::INT8:
				stats = make_unique<NumericStatistics>(group_type, Value::MinimumValue(group_type),
				                                       Value::MaximumValue(group_type), StatisticsType::LOCAL_STATS);
				break;
			case PhysicalType::INT16:
				stats = make_unique<NumericStatistics>(group_type, Value::MinimumValue(group_type),
				                                       Value::MaximumValue(group_type), StatisticsType::LOCAL_STATS);
				break;
			default:
				// type is too large and there are no stats: skip perfect hashing
				return false;
			}
			// we had no stats before, so we have no clue if there are null values or not
			stats->validity_stats = make_unique<ValidityStatistics>(true);
		}
		auto &nstats = (NumericStatistics &)*stats;

		if (nstats.min.IsNull() || nstats.max.IsNull()) {
			return false;
		}
		// we have a min and a max value for the stats: use that to figure out how many bits we have
		// we add two here, one for the NULL value, and one to make the computation one-indexed
		// (e.g. if min and max are the same, we still need one entry in total)
		int64_t range;
		switch (group_type.InternalType()) {
		case PhysicalType::INT8:
			range = int64_t(nstats.max.GetValueUnsafe<int8_t>()) - int64_t(nstats.min.GetValueUnsafe<int8_t>());
			break;
		case PhysicalType::INT16:
			range = int64_t(nstats.max.GetValueUnsafe<int16_t>()) - int64_t(nstats.min.GetValueUnsafe<int16_t>());
			break;
		case PhysicalType::INT32:
			range = int64_t(nstats.max.GetValueUnsafe<int32_t>()) - int64_t(nstats.min.GetValueUnsafe<int32_t>());
			break;
		case PhysicalType::INT64:
			if (!TrySubtractOperator::Operation(nstats.max.GetValueUnsafe<int64_t>(),
			                                    nstats.min.GetValueUnsafe<int64_t>(), range)) {
				return false;
			}
			break;
		default:
			throw InternalException("Unsupported type for perfect hash (should be caught before)");
		}
		// bail out on any range bigger than 2^32
		if (range >= NumericLimits<int32_t>::Maximum()) {
			return false;
		}
		range += 2;
		// figure out how many bits we need
		idx_t required_bits = RequiredBitsForValue(range);
		bits_per_group.push_back(required_bits);
		perfect_hash_bits += required_bits;
		// check if we have exceeded the bits for the hash
		if (perfect_hash_bits > ClientConfig::GetConfig(context).perfect_ht_threshold) {
			// too many bits for perfect hash
			return false;
		}
	}
	for (auto &expression : op.expressions) {
		auto &aggregate = (BoundAggregateExpression &)*expression;
		if (aggregate.distinct || !aggregate.function.combine) {
			// distinct aggregates are not supported in perfect hash aggregates
			return false;
		}
	}
	return true;
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalAggregate &op) {
	unique_ptr<PhysicalOperator> groupby;
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);

	plan = ExtractAggregateExpressions(move(plan), op.expressions, op.groups);

	if (op.groups.empty()) {
		// no groups, check if we can use a simple aggregation
		// special case: aggregate entire columns together
		bool use_simple_aggregation = true;
		for (auto &expression : op.expressions) {
			auto &aggregate = (BoundAggregateExpression &)*expression;
			if (!aggregate.function.simple_update || aggregate.distinct) {
				// unsupported aggregate for simple aggregation: use hash aggregation
				use_simple_aggregation = false;
				break;
			}
		}
		if (use_simple_aggregation) {
			groupby = make_unique_base<PhysicalOperator, PhysicalSimpleAggregate>(op.types, move(op.expressions),
			                                                                      op.estimated_cardinality);
		} else {
			groupby = make_unique_base<PhysicalOperator, PhysicalHashAggregate>(context, op.types, move(op.expressions),
			                                                                    op.estimated_cardinality);
		}
	} else {
		// groups! create a GROUP BY aggregator
		// use a perfect hash aggregate if possible
		vector<idx_t> required_bits;
		if (CanUsePerfectHashAggregate(context, op, required_bits)) {
			groupby = make_unique_base<PhysicalOperator, PhysicalPerfectHashAggregate>(
			    context, op.types, move(op.expressions), move(op.groups), move(op.group_stats), move(required_bits),
			    op.estimated_cardinality);
		} else {
			groupby = make_unique_base<PhysicalOperator, PhysicalHashAggregate>(
			    context, op.types, move(op.expressions), move(op.groups), move(op.grouping_sets),
			    move(op.grouping_functions), op.estimated_cardinality);
		}
	}
	groupby->children.push_back(move(plan));
	return groupby;
}

unique_ptr<PhysicalOperator>
PhysicalPlanGenerator::ExtractAggregateExpressions(unique_ptr<PhysicalOperator> child,
                                                   vector<unique_ptr<Expression>> &aggregates,
                                                   vector<unique_ptr<Expression>> &groups) {
	vector<unique_ptr<Expression>> expressions;
	vector<LogicalType> types;

	for (auto &group : groups) {
		auto ref = make_unique<BoundReferenceExpression>(group->return_type, expressions.size());
		types.push_back(group->return_type);
		expressions.push_back(move(group));
		group = move(ref);
	}

	for (auto &aggr : aggregates) {
		auto &bound_aggr = (BoundAggregateExpression &)*aggr;
		for (auto &child : bound_aggr.children) {
			auto ref = make_unique<BoundReferenceExpression>(child->return_type, expressions.size());
			types.push_back(child->return_type);
			expressions.push_back(move(child));
			child = move(ref);
		}
		if (bound_aggr.filter) {
			auto &filter = bound_aggr.filter;
			auto ref = make_unique<BoundReferenceExpression>(filter->return_type, expressions.size());
			types.push_back(filter->return_type);
			expressions.push_back(move(filter));
			bound_aggr.filter = move(ref);
		}
	}
	if (expressions.empty()) {
		return child;
	}
	auto projection = make_unique<PhysicalProjection>(move(types), move(expressions), child->estimated_cardinality);
	projection->children.push_back(move(child));
	return move(projection);
}

} // namespace duckdb




namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalAnyJoin &op) {
	// first visit the child nodes
	D_ASSERT(op.children.size() == 2);
	D_ASSERT(op.condition);

	auto left = CreatePlan(*op.children[0]);
	auto right = CreatePlan(*op.children[1]);

	// create the blockwise NL join
	return make_unique<PhysicalBlockwiseNLJoin>(op, move(left), move(right), move(op.condition), op.join_type,
	                                            op.estimated_cardinality);
}

} // namespace duckdb




namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalChunkGet &op) {
	D_ASSERT(op.children.size() == 0);
	D_ASSERT(op.collection);

	// create a PhysicalChunkScan pointing towards the owned collection
	auto chunk_scan =
	    make_unique<PhysicalChunkScan>(op.types, PhysicalOperatorType::CHUNK_SCAN, op.estimated_cardinality);
	chunk_scan->owned_collection = move(op.collection);
	chunk_scan->collection = chunk_scan->owned_collection.get();
	return move(chunk_scan);
}

} // namespace duckdb



















namespace duckdb {

static bool CanPlanIndexJoin(Transaction &transaction, TableScanBindData *bind_data, PhysicalTableScan &scan) {
	if (!bind_data) {
		// not a table scan
		return false;
	}
	auto table = bind_data->table;
	if (transaction.storage.Find(table->storage.get())) {
		// transaction local appends: skip index join
		return false;
	}
	if (scan.table_filters && !scan.table_filters->filters.empty()) {
		// table scan filters
		return false;
	}
	return true;
}

bool ExtractNumericValue(Value val, int64_t &result) {
	if (!val.type().IsIntegral()) {
		switch (val.type().InternalType()) {
		case PhysicalType::INT16:
			result = val.GetValueUnsafe<int16_t>();
			break;
		case PhysicalType::INT32:
			result = val.GetValueUnsafe<int32_t>();
			break;
		case PhysicalType::INT64:
			result = val.GetValueUnsafe<int64_t>();
			break;
		default:
			return false;
		}
	} else {
		if (!val.TryCastAs(LogicalType::BIGINT)) {
			return false;
		}
		result = val.GetValue<int64_t>();
	}
	return true;
}

void CheckForPerfectJoinOpt(LogicalComparisonJoin &op, PerfectHashJoinStats &join_state) {
	// we only do this optimization for inner joins
	if (op.join_type != JoinType::INNER) {
		return;
	}
	// with one condition
	if (op.conditions.size() != 1) {
		return;
	}
	// with propagated statistics
	if (op.join_stats.empty()) {
		return;
	}
	// with equality condition and null values not equal
	for (auto &&condition : op.conditions) {
		if (condition.comparison != ExpressionType::COMPARE_EQUAL) {
			return;
		}
	}
	// with integral internal types
	for (auto &&join_stat : op.join_stats) {
		if (!TypeIsInteger(join_stat->type.InternalType()) || join_stat->type.InternalType() == PhysicalType::INT128) {
			// perfect join not possible for non-integral types or hugeint
			return;
		}
	}

	// and when the build range is smaller than the threshold
	auto stats_build = reinterpret_cast<NumericStatistics *>(op.join_stats[0].get()); // lhs stats
	if (stats_build->min.IsNull() || stats_build->max.IsNull()) {
		return;
	}
	int64_t min_value, max_value;
	if (!ExtractNumericValue(stats_build->min, min_value) || !ExtractNumericValue(stats_build->max, max_value)) {
		return;
	}
	int64_t build_range;
	if (!TrySubtractOperator::Operation(max_value, min_value, build_range)) {
		return;
	}

	// Fill join_stats for invisible join
	auto stats_probe = reinterpret_cast<NumericStatistics *>(op.join_stats[1].get()); // rhs stats

	// The max size our build must have to run the perfect HJ
	const idx_t MAX_BUILD_SIZE = 1000000;
	join_state.probe_min = stats_probe->min;
	join_state.probe_max = stats_probe->max;
	join_state.build_min = stats_build->min;
	join_state.build_max = stats_build->max;
	join_state.estimated_cardinality = op.estimated_cardinality;
	join_state.build_range = build_range;
	if (join_state.build_range > MAX_BUILD_SIZE || stats_probe->max.IsNull() || stats_probe->min.IsNull()) {
		return;
	}
	if (stats_build->min <= stats_probe->min && stats_probe->max <= stats_build->max) {
		join_state.is_probe_in_domain = true;
	}
	join_state.is_build_small = true;
	return;
}

static void CanUseIndexJoin(TableScanBindData *tbl, Expression &expr, Index **result_index) {
	tbl->table->storage->info->indexes.Scan([&](Index &index) {
		if (index.unbound_expressions.size() != 1) {
			return false;
		}
		if (expr.alias == index.unbound_expressions[0]->alias) {
			*result_index = &index;
			return true;
		}
		return false;
	});
}

void TransformIndexJoin(ClientContext &context, LogicalComparisonJoin &op, Index **left_index, Index **right_index,
                        PhysicalOperator *left, PhysicalOperator *right) {
	auto &transaction = Transaction::GetTransaction(context);
	// check if one of the tables has an index on column
	if (op.join_type == JoinType::INNER && op.conditions.size() == 1) {
		// check if one of the children are table scans and if they have an index in the join attribute
		// (op.condition)
		if (left->type == PhysicalOperatorType::TABLE_SCAN) {
			auto &tbl_scan = (PhysicalTableScan &)*left;
			auto tbl = dynamic_cast<TableScanBindData *>(tbl_scan.bind_data.get());
			if (CanPlanIndexJoin(transaction, tbl, tbl_scan)) {
				CanUseIndexJoin(tbl, *op.conditions[0].left, left_index);
			}
		}
		if (right->type == PhysicalOperatorType::TABLE_SCAN) {
			auto &tbl_scan = (PhysicalTableScan &)*right;
			auto tbl = dynamic_cast<TableScanBindData *>(tbl_scan.bind_data.get());
			if (CanPlanIndexJoin(transaction, tbl, tbl_scan)) {
				CanUseIndexJoin(tbl, *op.conditions[0].right, right_index);
			}
		}
	}
}

static void RewriteJoinCondition(Expression &expr, idx_t offset) {
	if (expr.type == ExpressionType::BOUND_REF) {
		auto &ref = (BoundReferenceExpression &)expr;
		ref.index += offset;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { RewriteJoinCondition(child, offset); });
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalComparisonJoin &op) {
	// now visit the children
	D_ASSERT(op.children.size() == 2);
	idx_t lhs_cardinality = op.children[0]->EstimateCardinality(context);
	idx_t rhs_cardinality = op.children[1]->EstimateCardinality(context);
	auto left = CreatePlan(*op.children[0]);
	auto right = CreatePlan(*op.children[1]);
	D_ASSERT(left && right);

	if (op.conditions.empty()) {
		// no conditions: insert a cross product
		return make_unique<PhysicalCrossProduct>(op.types, move(left), move(right), op.estimated_cardinality);
	}

	bool has_equality = false;
	// bool has_inequality = false;
	size_t has_range = 0;
	for (size_t c = 0; c < op.conditions.size(); ++c) {
		auto &cond = op.conditions[c];
		switch (cond.comparison) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			has_equality = true;
			break;
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			++has_range;
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
		case ExpressionType::COMPARE_DISTINCT_FROM:
			// has_inequality = true;
			break;
		default:
			throw NotImplementedException("Unimplemented comparison join");
		}
	}

	unique_ptr<PhysicalOperator> plan;
	if (has_equality) {
		Index *left_index {}, *right_index {};
		TransformIndexJoin(context, op, &left_index, &right_index, left.get(), right.get());
		if (left_index &&
		    (ClientConfig::GetConfig(context).force_index_join || rhs_cardinality < 0.01 * lhs_cardinality)) {
			auto &tbl_scan = (PhysicalTableScan &)*left;
			swap(op.conditions[0].left, op.conditions[0].right);
			return make_unique<PhysicalIndexJoin>(op, move(right), move(left), move(op.conditions), op.join_type,
			                                      op.right_projection_map, op.left_projection_map, tbl_scan.column_ids,
			                                      left_index, false, op.estimated_cardinality);
		}
		if (right_index &&
		    (ClientConfig::GetConfig(context).force_index_join || lhs_cardinality < 0.01 * rhs_cardinality)) {
			auto &tbl_scan = (PhysicalTableScan &)*right;
			return make_unique<PhysicalIndexJoin>(op, move(left), move(right), move(op.conditions), op.join_type,
			                                      op.left_projection_map, op.right_projection_map, tbl_scan.column_ids,
			                                      right_index, true, op.estimated_cardinality);
		}
		// Equality join with small number of keys : possible perfect join optimization
		PerfectHashJoinStats perfect_join_stats;
		CheckForPerfectJoinOpt(op, perfect_join_stats);
		plan = make_unique<PhysicalHashJoin>(op, move(left), move(right), move(op.conditions), op.join_type,
		                                     op.left_projection_map, op.right_projection_map, move(op.delim_types),
		                                     op.estimated_cardinality, perfect_join_stats);

	} else {
		bool can_merge = has_range > 0;
		bool can_iejoin = has_range >= 2 && rec_ctes.empty();
		switch (op.join_type) {
		case JoinType::SEMI:
		case JoinType::ANTI:
		case JoinType::MARK:
			can_merge = can_merge && op.conditions.size() == 1;
			can_iejoin = false;
			break;
		default:
			break;
		}
		if (can_iejoin) {
			plan = make_unique<PhysicalIEJoin>(op, move(left), move(right), move(op.conditions), op.join_type,
			                                   op.estimated_cardinality);
		} else if (can_merge) {
			// range join: use piecewise merge join
			plan = make_unique<PhysicalPiecewiseMergeJoin>(op, move(left), move(right), move(op.conditions),
			                                               op.join_type, op.estimated_cardinality);
		} else if (PhysicalNestedLoopJoin::IsSupported(op.conditions)) {
			// inequality join: use nested loop
			plan = make_unique<PhysicalNestedLoopJoin>(op, move(left), move(right), move(op.conditions), op.join_type,
			                                           op.estimated_cardinality);
		} else {
			for (auto &cond : op.conditions) {
				RewriteJoinCondition(*cond.right, left->types.size());
			}
			auto condition = JoinCondition::CreateExpression(move(op.conditions));
			plan = make_unique<PhysicalBlockwiseNLJoin>(op, move(left), move(right), move(condition), op.join_type,
			                                            op.estimated_cardinality);
		}
	}
	return plan;
}

} // namespace duckdb




namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCopyToFile &op) {
	auto plan = CreatePlan(*op.children[0]);
	bool use_tmp_file = op.is_file_and_exists && op.use_tmp_file;
	if (use_tmp_file) {
		op.file_path += ".tmp";
	}
	// COPY from select statement to file
	auto copy = make_unique<PhysicalCopyToFile>(op.types, op.function, move(op.bind_data), op.estimated_cardinality);
	copy->file_path = op.file_path;
	copy->use_tmp_file = use_tmp_file;

	copy->children.push_back(move(plan));
	return move(copy);
}

} // namespace duckdb










namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCreate &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_CREATE_SEQUENCE:
		return make_unique<PhysicalCreateSequence>(unique_ptr_cast<CreateInfo, CreateSequenceInfo>(move(op.info)),
		                                           op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_CREATE_VIEW:
		return make_unique<PhysicalCreateView>(unique_ptr_cast<CreateInfo, CreateViewInfo>(move(op.info)),
		                                       op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_CREATE_SCHEMA:
		return make_unique<PhysicalCreateSchema>(unique_ptr_cast<CreateInfo, CreateSchemaInfo>(move(op.info)),
		                                         op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_CREATE_MACRO:
		return make_unique<PhysicalCreateFunction>(unique_ptr_cast<CreateInfo, CreateMacroInfo>(move(op.info)),
		                                           op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_CREATE_TYPE:
		return make_unique<PhysicalCreateType>(unique_ptr_cast<CreateInfo, CreateTypeInfo>(move(op.info)),
		                                       op.estimated_cardinality);
	default:
		throw NotImplementedException("Unimplemented type for logical simple create");
	}
}

} // namespace duckdb





namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCreateIndex &op) {
	D_ASSERT(op.children.empty());
	dependencies.insert(&op.table);
	return make_unique<PhysicalCreateIndex>(op, op.table, op.column_ids, move(op.expressions), move(op.info),
	                                        move(op.unbound_expressions), op.estimated_cardinality);
}

} // namespace duckdb









namespace duckdb {

static void ExtractDependencies(Expression &expr, unordered_set<CatalogEntry *> &dependencies) {
	if (expr.type == ExpressionType::BOUND_FUNCTION) {
		auto &function = (BoundFunctionExpression &)expr;
		if (function.function.dependency) {
			function.function.dependency(function, dependencies);
		}
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { ExtractDependencies(child, dependencies); });
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCreateTable &op) {
	// extract dependencies from any default values
	for (auto &default_value : op.info->bound_defaults) {
		if (default_value) {
			ExtractDependencies(*default_value, op.info->dependencies);
		}
	}
	auto &create_info = (CreateTableInfo &)*op.info->base;
	auto &catalog = Catalog::GetCatalog(context);
	auto existing_entry =
	    catalog.GetEntry(context, CatalogType::TABLE_ENTRY, create_info.schema, create_info.table, true);
	bool replace = op.info->Base().on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT;
	if ((!existing_entry || replace) && !op.children.empty()) {
		D_ASSERT(op.children.size() == 1);
		auto create = make_unique<PhysicalCreateTableAs>(op, op.schema, move(op.info), op.estimated_cardinality);
		auto plan = CreatePlan(*op.children[0]);
		create->children.push_back(move(plan));
		return move(create);
	} else {
		return make_unique<PhysicalCreateTable>(op, op.schema, move(op.info), op.estimated_cardinality);
	}
}

} // namespace duckdb




namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCrossProduct &op) {
	D_ASSERT(op.children.size() == 2);

	auto left = CreatePlan(*op.children[0]);
	auto right = CreatePlan(*op.children[1]);
	return make_unique<PhysicalCrossProduct>(op.types, move(left), move(right), op.estimated_cardinality);
}

} // namespace duckdb






namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDelete &op) {
	D_ASSERT(op.children.size() == 1);
	D_ASSERT(op.expressions.size() == 1);
	D_ASSERT(op.expressions[0]->type == ExpressionType::BOUND_REF);

	auto plan = CreatePlan(*op.children[0]);

	// get the index of the row_id column
	auto &bound_ref = (BoundReferenceExpression &)*op.expressions[0];

	dependencies.insert(op.table);
	auto del = make_unique<PhysicalDelete>(op.types, *op.table, *op.table->storage, bound_ref.index,
	                                       op.estimated_cardinality, op.return_chunk);
	del->children.push_back(move(plan));
	return move(del);
}

} // namespace duckdb




namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDelimGet &op) {
	D_ASSERT(op.children.empty());

	// create a PhysicalChunkScan without an owned_collection, the collection will be added later
	auto chunk_scan =
	    make_unique<PhysicalChunkScan>(op.types, PhysicalOperatorType::DELIM_SCAN, op.estimated_cardinality);
	return move(chunk_scan);
}

} // namespace duckdb











namespace duckdb {

static void GatherDelimScans(PhysicalOperator *op, vector<PhysicalOperator *> &delim_scans) {
	D_ASSERT(op);
	if (op->type == PhysicalOperatorType::DELIM_SCAN) {
		delim_scans.push_back(op);
	}
	for (auto &child : op->children) {
		GatherDelimScans(child.get(), delim_scans);
	}
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDelimJoin &op) {
	// first create the underlying join
	auto plan = CreatePlan((LogicalComparisonJoin &)op);
	// this should create a join, not a cross product
	D_ASSERT(plan && plan->type != PhysicalOperatorType::CROSS_PRODUCT);
	// duplicate eliminated join
	// first gather the scans on the duplicate eliminated data set from the RHS
	vector<PhysicalOperator *> delim_scans;
	GatherDelimScans(plan->children[1].get(), delim_scans);
	if (delim_scans.empty()) {
		// no duplicate eliminated scans in the RHS!
		// in this case we don't need to create a delim join
		// just push the normal join
		return plan;
	}
	vector<LogicalType> delim_types;
	vector<unique_ptr<Expression>> distinct_groups, distinct_expressions;
	for (auto &delim_expr : op.duplicate_eliminated_columns) {
		D_ASSERT(delim_expr->type == ExpressionType::BOUND_REF);
		auto &bound_ref = (BoundReferenceExpression &)*delim_expr;
		delim_types.push_back(bound_ref.return_type);
		distinct_groups.push_back(make_unique<BoundReferenceExpression>(bound_ref.return_type, bound_ref.index));
	}
	// now create the duplicate eliminated join
	auto delim_join = make_unique<PhysicalDelimJoin>(op.types, move(plan), delim_scans, op.estimated_cardinality);
	// we still have to create the DISTINCT clause that is used to generate the duplicate eliminated chunk
	delim_join->distinct = make_unique<PhysicalHashAggregate>(context, delim_types, move(distinct_expressions),
	                                                          move(distinct_groups), op.estimated_cardinality);
	return move(delim_join);
}

} // namespace duckdb








namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreateDistinctOn(unique_ptr<PhysicalOperator> child,
                                                                     vector<unique_ptr<Expression>> distinct_targets) {
	D_ASSERT(child);
	D_ASSERT(!distinct_targets.empty());

	auto &types = child->GetTypes();
	vector<unique_ptr<Expression>> groups, aggregates, projections;
	idx_t group_count = distinct_targets.size();
	unordered_map<idx_t, idx_t> group_by_references;
	vector<LogicalType> aggregate_types;
	// creates one group per distinct_target
	for (idx_t i = 0; i < distinct_targets.size(); i++) {
		auto &target = distinct_targets[i];
		if (target->type == ExpressionType::BOUND_REF) {
			auto &bound_ref = (BoundReferenceExpression &)*target;
			group_by_references[bound_ref.index] = i;
		}
		aggregate_types.push_back(target->return_type);
		groups.push_back(move(target));
	}
	bool requires_projection = false;
	if (types.size() != group_count) {
		requires_projection = true;
	}
	// we need to create one aggregate per column in the select_list
	for (idx_t i = 0; i < types.size(); ++i) {
		auto logical_type = types[i];
		// check if we can directly refer to a group, or if we need to push an aggregate with FIRST
		auto entry = group_by_references.find(i);
		if (entry != group_by_references.end()) {
			auto group_index = entry->second;
			// entry is found: can directly refer to a group
			projections.push_back(make_unique<BoundReferenceExpression>(logical_type, group_index));
			if (group_index != i) {
				// we require a projection only if this group element is out of order
				requires_projection = true;
			}
		} else {
			// entry is not one of the groups: need to push a FIRST aggregate
			auto bound = make_unique<BoundReferenceExpression>(logical_type, i);
			vector<unique_ptr<Expression>> first_children;
			first_children.push_back(move(bound));
			auto first_aggregate = AggregateFunction::BindAggregateFunction(
			    context, FirstFun::GetFunction(logical_type), move(first_children), nullptr, false);
			// add the projection
			projections.push_back(make_unique<BoundReferenceExpression>(logical_type, group_count + aggregates.size()));
			// push it to the list of aggregates
			aggregate_types.push_back(logical_type);
			aggregates.push_back(move(first_aggregate));
			requires_projection = true;
		}
	}

	child = ExtractAggregateExpressions(move(child), aggregates, groups);

	// we add a physical hash aggregation in the plan to select the distinct groups
	auto groupby = make_unique<PhysicalHashAggregate>(context, aggregate_types, move(aggregates), move(groups),
	                                                  child->estimated_cardinality);
	groupby->children.push_back(move(child));
	if (!requires_projection) {
		return move(groupby);
	}

	// we add a physical projection on top of the aggregation to project all members in the select list
	auto aggr_projection = make_unique<PhysicalProjection>(types, move(projections), groupby->estimated_cardinality);
	aggr_projection->children.push_back(move(groupby));
	return move(aggr_projection);
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDistinct &op) {
	D_ASSERT(op.children.size() == 1);
	auto plan = CreatePlan(*op.children[0]);
	return CreateDistinctOn(move(plan), move(op.distinct_targets));
}

} // namespace duckdb




namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDummyScan &op) {
	D_ASSERT(op.children.size() == 0);
	return make_unique<PhysicalDummyScan>(op.types, op.estimated_cardinality);
}

} // namespace duckdb




namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalEmptyResult &op) {
	D_ASSERT(op.children.size() == 0);
	return make_unique<PhysicalEmptyResult>(op.types, op.estimated_cardinality);
}

} // namespace duckdb




namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalExecute &op) {
	if (!op.prepared->plan) {
		D_ASSERT(op.children.size() == 1);
		auto owned_plan = CreatePlan(*op.children[0]);
		auto execute = make_unique<PhysicalExecute>(owned_plan.get());
		execute->owned_plan = move(owned_plan);
		execute->prepared = move(op.prepared);
		return move(execute);
	} else {
		D_ASSERT(op.children.size() == 0);
		return make_unique<PhysicalExecute>(op.prepared->plan.get());
	}
}

} // namespace duckdb








namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalExplain &op) {
	D_ASSERT(op.children.size() == 1);
	auto logical_plan_opt = op.children[0]->ToString();
	auto plan = CreatePlan(*op.children[0]);
	if (op.explain_type == ExplainType::EXPLAIN_ANALYZE) {
		auto result = make_unique<PhysicalExplainAnalyze>(op.types);
		result->children.push_back(move(plan));
		return move(result);
	}

	op.physical_plan = plan->ToString();
	// the output of the explain
	vector<string> keys, values;
	switch (ClientConfig::GetConfig(context).explain_output_type) {
	case ExplainOutputType::OPTIMIZED_ONLY:
		keys = {"logical_opt"};
		values = {logical_plan_opt};
		break;
	case ExplainOutputType::PHYSICAL_ONLY:
		keys = {"physical_plan"};
		values = {op.physical_plan};
		break;
	default:
		keys = {"logical_plan", "logical_opt", "physical_plan"};
		values = {op.logical_plan_unopt, logical_plan_opt, op.physical_plan};
	}

	// create a ChunkCollection from the output
	auto &allocator = Allocator::Get(context);
	auto collection = make_unique<ChunkCollection>(allocator);

	DataChunk chunk;
	chunk.Initialize(allocator, op.types);
	for (idx_t i = 0; i < keys.size(); i++) {
		chunk.SetValue(0, chunk.size(), Value(keys[i]));
		chunk.SetValue(1, chunk.size(), Value(values[i]));
		chunk.SetCardinality(chunk.size() + 1);
		if (chunk.size() == STANDARD_VECTOR_SIZE) {
			collection->Append(chunk);
			chunk.Reset();
		}
	}
	collection->Append(chunk);

	// create a chunk scan to output the result
	auto chunk_scan =
	    make_unique<PhysicalChunkScan>(op.types, PhysicalOperatorType::CHUNK_SCAN, op.estimated_cardinality);
	chunk_scan->owned_collection = move(collection);
	chunk_scan->collection = chunk_scan->owned_collection.get();
	return move(chunk_scan);
}

} // namespace duckdb





namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalExport &op) {
	auto &config = DBConfig::GetConfig(context);
	if (!config.enable_external_access) {
		throw PermissionException("Export is disabled through configuration");
	}
	auto export_node = make_unique<PhysicalExport>(op.types, op.function, move(op.copy_info), op.estimated_cardinality,
	                                               op.exported_tables);
	// plan the underlying copy statements, if any
	if (!op.children.empty()) {
		auto plan = CreatePlan(*op.children[0]);
		export_node->children.push_back(move(plan));
	}
	return move(export_node);
}

} // namespace duckdb





namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalExpressionGet &op) {
	D_ASSERT(op.children.size() == 1);
	auto plan = CreatePlan(*op.children[0]);

	auto expr_scan = make_unique<PhysicalExpressionScan>(op.types, move(op.expressions), op.estimated_cardinality);
	expr_scan->children.push_back(move(plan));
	if (!expr_scan->IsFoldable()) {
		return move(expr_scan);
	}
	auto &allocator = Allocator::Get(context);
	// simple expression scan (i.e. no subqueries to evaluate and no prepared statement parameters)
	// we can evaluate all the expressions right now and turn this into a chunk collection scan
	auto chunk_scan =
	    make_unique<PhysicalChunkScan>(op.types, PhysicalOperatorType::CHUNK_SCAN, expr_scan->expressions.size());
	chunk_scan->owned_collection = make_unique<ChunkCollection>(allocator);
	chunk_scan->collection = chunk_scan->owned_collection.get();

	DataChunk chunk;
	chunk.Initialize(allocator, op.types);
	for (idx_t expression_idx = 0; expression_idx < expr_scan->expressions.size(); expression_idx++) {
		chunk.Reset();
		expr_scan->EvaluateExpression(allocator, expression_idx, nullptr, chunk);
		chunk_scan->owned_collection->Append(chunk);
	}
	return move(chunk_scan);
}

} // namespace duckdb










namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalFilter &op) {
	D_ASSERT(op.children.size() == 1);
	unique_ptr<PhysicalOperator> plan = CreatePlan(*op.children[0]);
	if (!op.expressions.empty()) {
		D_ASSERT(plan->types.size() > 0);
		// create a filter if there is anything to filter
		auto filter = make_unique<PhysicalFilter>(plan->types, move(op.expressions), op.estimated_cardinality);
		filter->children.push_back(move(plan));
		plan = move(filter);
	}
	if (!op.projection_map.empty()) {
		// there is a projection map, generate a physical projection
		vector<unique_ptr<Expression>> select_list;
		for (idx_t i = 0; i < op.projection_map.size(); i++) {
			select_list.push_back(make_unique<BoundReferenceExpression>(op.types[i], op.projection_map[i]));
		}
		auto proj = make_unique<PhysicalProjection>(op.types, move(select_list), op.estimated_cardinality);
		proj->children.push_back(move(plan));
		plan = move(proj);
	}
	return plan;
}

} // namespace duckdb










namespace duckdb {

unique_ptr<TableFilterSet> CreateTableFilterSet(TableFilterSet &table_filters, vector<column_t> &column_ids) {
	// create the table filter map
	auto table_filter_set = make_unique<TableFilterSet>();
	for (auto &table_filter : table_filters.filters) {
		// find the relative column index from the absolute column index into the table
		idx_t column_index = DConstants::INVALID_INDEX;
		for (idx_t i = 0; i < column_ids.size(); i++) {
			if (table_filter.first == column_ids[i]) {
				column_index = i;
				break;
			}
		}
		if (column_index == DConstants::INVALID_INDEX) {
			throw InternalException("Could not find column index for table filter");
		}
		table_filter_set->filters[column_index] = move(table_filter.second);
	}
	return table_filter_set;
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalGet &op) {
	if (!op.children.empty()) {
		// this is for table producing functions that consume subquery results
		D_ASSERT(op.children.size() == 1);
		auto node = make_unique<PhysicalTableInOutFunction>(op.returned_types, op.function, move(op.bind_data),
		                                                    op.column_ids, op.estimated_cardinality);
		node->children.push_back(CreatePlan(move(op.children[0])));
		return move(node);
	}

	unique_ptr<TableFilterSet> table_filters;
	if (!op.table_filters.filters.empty()) {
		table_filters = CreateTableFilterSet(op.table_filters, op.column_ids);
	}

	if (op.function.dependency) {
		op.function.dependency(dependencies, op.bind_data.get());
	}
	// create the table scan node
	if (!op.function.projection_pushdown) {
		// function does not support projection pushdown
		auto node = make_unique<PhysicalTableScan>(op.returned_types, op.function, move(op.bind_data), op.column_ids,
		                                           op.names, move(table_filters), op.estimated_cardinality);
		// first check if an additional projection is necessary
		if (op.column_ids.size() == op.returned_types.size()) {
			bool projection_necessary = false;
			for (idx_t i = 0; i < op.column_ids.size(); i++) {
				if (op.column_ids[i] != i) {
					projection_necessary = true;
					break;
				}
			}
			if (!projection_necessary) {
				// a projection is not necessary if all columns have been requested in-order
				// in that case we just return the node

				return move(node);
			}
		}
		// push a projection on top that does the projection
		vector<LogicalType> types;
		vector<unique_ptr<Expression>> expressions;
		for (auto &column_id : op.column_ids) {
			if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
				types.emplace_back(LogicalType::BIGINT);
				expressions.push_back(make_unique<BoundConstantExpression>(Value::BIGINT(0)));
			} else {
				auto type = op.returned_types[column_id];
				types.push_back(type);
				expressions.push_back(make_unique<BoundReferenceExpression>(type, column_id));
			}
		}

		auto projection = make_unique<PhysicalProjection>(move(types), move(expressions), op.estimated_cardinality);
		projection->children.push_back(move(node));
		return move(projection);
	} else {
		return make_unique<PhysicalTableScan>(op.types, op.function, move(op.bind_data), op.column_ids, op.names,
		                                      move(table_filters), op.estimated_cardinality);
	}
}

} // namespace duckdb





namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalInsert &op) {
	unique_ptr<PhysicalOperator> plan;
	if (!op.children.empty()) {
		D_ASSERT(op.children.size() == 1);
		plan = CreatePlan(*op.children[0]);
	}

	dependencies.insert(op.table);
	auto insert = make_unique<PhysicalInsert>(op.types, op.table, op.column_index_map, move(op.bound_defaults),
	                                          op.estimated_cardinality, op.return_chunk);
	if (plan) {
		insert->children.push_back(move(plan));
	}
	return move(insert);
}

} // namespace duckdb






namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalLimit &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);
	auto &config = DBConfig::GetConfig(context);
	unique_ptr<PhysicalOperator> limit;
	if (!config.preserve_insertion_order) {
		// use parallel streaming limit if insertion order is not important
		limit = make_unique<PhysicalStreamingLimit>(op.types, (idx_t)op.limit_val, op.offset_val, move(op.limit),
		                                            move(op.offset), op.estimated_cardinality, true);
	} else {
		// maintaining insertion order is important
		bool all_sources_support_batch_index = plan->AllSourcesSupportBatchIndex();

		if (all_sources_support_batch_index) {
			// source supports batch index: use parallel batch limit
			limit = make_unique<PhysicalLimit>(op.types, (idx_t)op.limit_val, op.offset_val, move(op.limit),
			                                   move(op.offset), op.estimated_cardinality);
		} else {
			// source does not support batch index: use a non-parallel streaming limit
			limit = make_unique<PhysicalStreamingLimit>(op.types, (idx_t)op.limit_val, op.offset_val, move(op.limit),
			                                            move(op.offset), op.estimated_cardinality, false);
		}
	}

	limit->children.push_back(move(plan));
	return limit;
}

} // namespace duckdb




namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalLimitPercent &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);

	auto limit = make_unique<PhysicalLimitPercent>(op.types, op.limit_percent, op.offset_val, move(op.limit),
	                                               move(op.offset), op.estimated_cardinality);
	limit->children.push_back(move(plan));
	return move(limit);
}

} // namespace duckdb




namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalOrder &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);
	if (!op.orders.empty()) {
		auto order = make_unique<PhysicalOrder>(op.types, move(op.orders), op.estimated_cardinality);
		order->children.push_back(move(plan));
		plan = move(order);
	}
	return plan;
}

} // namespace duckdb




namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalPragma &op) {
	return make_unique<PhysicalPragma>(op.function, op.info, op.estimated_cardinality);
}

} // namespace duckdb





namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalPrepare &op) {
	D_ASSERT(op.children.size() == 1);

	// generate physical plan
	auto plan = CreatePlan(*op.children[0]);
	op.prepared->types = plan->types;
	op.prepared->plan = move(plan);

	return make_unique<PhysicalPrepare>(op.name, move(op.prepared), op.estimated_cardinality);
}

} // namespace duckdb





namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalProjection &op) {
	D_ASSERT(op.children.size() == 1);
	auto plan = CreatePlan(*op.children[0]);

#ifdef DEBUG
	for (auto &expr : op.expressions) {
		D_ASSERT(!expr->IsWindow());
		D_ASSERT(!expr->IsAggregate());
	}
#endif
	if (plan->types.size() == op.types.size()) {
		// check if this projection can be omitted entirely
		// this happens if a projection simply emits the columns in the same order
		// e.g. PROJECTION(#0, #1, #2, #3, ...)
		bool omit_projection = true;
		for (idx_t i = 0; i < op.types.size(); i++) {
			if (op.expressions[i]->type == ExpressionType::BOUND_REF) {
				auto &bound_ref = (BoundReferenceExpression &)*op.expressions[i];
				if (bound_ref.index == i) {
					continue;
				}
			}
			omit_projection = false;
			break;
		}
		if (omit_projection) {
			// the projection only directly projects the child' columns: omit it entirely
			return plan;
		}
	}

	auto projection = make_unique<PhysicalProjection>(op.types, move(op.expressions), op.estimated_cardinality);
	projection->children.push_back(move(plan));
	return move(projection);
}

} // namespace duckdb







namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalRecursiveCTE &op) {
	D_ASSERT(op.children.size() == 2);

	// Create the working_table that the PhysicalRecursiveCTE will use for evaluation.
	auto working_table = std::make_shared<ChunkCollection>(context);

	// Add the ChunkCollection to the context of this PhysicalPlanGenerator
	rec_ctes[op.table_index] = working_table;

	auto left = CreatePlan(*op.children[0]);
	auto right = CreatePlan(*op.children[1]);

	auto cte =
	    make_unique<PhysicalRecursiveCTE>(op.types, op.union_all, move(left), move(right), op.estimated_cardinality);
	cte->working_table = working_table;

	return move(cte);
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCTERef &op) {
	D_ASSERT(op.children.empty());

	auto chunk_scan =
	    make_unique<PhysicalChunkScan>(op.types, PhysicalOperatorType::RECURSIVE_CTE_SCAN, op.estimated_cardinality);

	// CreatePlan of a LogicalRecursiveCTE must have happened before.
	auto cte = rec_ctes.find(op.cte_index);
	if (cte == rec_ctes.end()) {
		throw Exception("Referenced recursive CTE does not exist.");
	}
	chunk_scan->collection = cte->second.get();
	return move(chunk_scan);
}

} // namespace duckdb





namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalSample &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);

	unique_ptr<PhysicalOperator> sample;
	switch (op.sample_options->method) {
	case SampleMethod::RESERVOIR_SAMPLE:
		sample = make_unique<PhysicalReservoirSample>(op.types, move(op.sample_options), op.estimated_cardinality);
		break;
	case SampleMethod::SYSTEM_SAMPLE:
	case SampleMethod::BERNOULLI_SAMPLE:
		if (!op.sample_options->is_percentage) {
			throw ParserException("Sample method %s cannot be used with a discrete sample count, either switch to "
			                      "reservoir sampling or use a sample_size",
			                      SampleMethodToString(op.sample_options->method));
		}
		sample = make_unique<PhysicalStreamingSample>(op.types, op.sample_options->method,
		                                              op.sample_options->sample_size.GetValue<double>(),
		                                              op.sample_options->seed, op.estimated_cardinality);
		break;
	default:
		throw InternalException("Unimplemented sample method");
	}
	sample->children.push_back(move(plan));
	return sample;
}

} // namespace duckdb




namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalSet &op) {
	return make_unique<PhysicalSet>(op.name, op.value, op.scope, op.estimated_cardinality);
}

} // namespace duckdb






namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalSetOperation &op) {
	D_ASSERT(op.children.size() == 2);

	auto left = CreatePlan(*op.children[0]);
	auto right = CreatePlan(*op.children[1]);

	if (left->GetTypes() != right->GetTypes()) {
		throw Exception("Type mismatch for SET OPERATION");
	}

	switch (op.type) {
	case LogicalOperatorType::LOGICAL_UNION:
		// UNION
		return make_unique<PhysicalUnion>(op.types, move(left), move(right), op.estimated_cardinality);
	default: {
		// EXCEPT/INTERSECT
		D_ASSERT(op.type == LogicalOperatorType::LOGICAL_EXCEPT || op.type == LogicalOperatorType::LOGICAL_INTERSECT);
		auto &types = left->GetTypes();
		vector<JoinCondition> conditions;
		// create equality condition for all columns
		for (idx_t i = 0; i < types.size(); i++) {
			JoinCondition cond;
			cond.left = make_unique<BoundReferenceExpression>(types[i], i);
			cond.right = make_unique<BoundReferenceExpression>(types[i], i);
			cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
			conditions.push_back(move(cond));
		}
		// EXCEPT is ANTI join
		// INTERSECT is SEMI join
		PerfectHashJoinStats join_stats; // used in inner joins only
		JoinType join_type = op.type == LogicalOperatorType::LOGICAL_EXCEPT ? JoinType::ANTI : JoinType::SEMI;
		return make_unique<PhysicalHashJoin>(op, move(left), move(right), move(conditions), join_type,
		                                     op.estimated_cardinality, join_stats);
	}
	}
}

} // namespace duckdb





namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalShow &op) {
	DataChunk output;
	output.Initialize(Allocator::Get(context), op.types);

	auto collection = make_unique<ChunkCollection>(Allocator::Get(context));
	for (idx_t column_idx = 0; column_idx < op.types_select.size(); column_idx++) {
		auto type = op.types_select[column_idx];
		auto &name = op.aliases[column_idx];

		// "name", TypeId::VARCHAR
		output.SetValue(0, output.size(), Value(name));
		// "type", TypeId::VARCHAR
		output.SetValue(1, output.size(), Value(type.ToString()));
		// "null", TypeId::VARCHAR
		output.SetValue(2, output.size(), Value("YES"));
		// "pk", TypeId::BOOL
		output.SetValue(3, output.size(), Value());
		// "dflt_value", TypeId::VARCHAR
		output.SetValue(4, output.size(), Value());
		// "extra", TypeId::VARCHAR
		output.SetValue(5, output.size(), Value());

		output.SetCardinality(output.size() + 1);
		if (output.size() == STANDARD_VECTOR_SIZE) {
			collection->Append(output);
			output.Reset();
		}
	}

	collection->Append(output);

	// create a chunk scan to output the result
	auto chunk_scan =
	    make_unique<PhysicalChunkScan>(op.types, PhysicalOperatorType::CHUNK_SCAN, op.estimated_cardinality);
	chunk_scan->owned_collection = move(collection);
	chunk_scan->collection = chunk_scan->owned_collection.get();
	return move(chunk_scan);
}

} // namespace duckdb













namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalSimple &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_ALTER:
		return make_unique<PhysicalAlter>(unique_ptr_cast<ParseInfo, AlterInfo>(move(op.info)),
		                                  op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_DROP:
		return make_unique<PhysicalDrop>(unique_ptr_cast<ParseInfo, DropInfo>(move(op.info)), op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_TRANSACTION:
		return make_unique<PhysicalTransaction>(unique_ptr_cast<ParseInfo, TransactionInfo>(move(op.info)),
		                                        op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_VACUUM:
		return make_unique<PhysicalVacuum>(unique_ptr_cast<ParseInfo, VacuumInfo>(move(op.info)),
		                                   op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_LOAD:
		return make_unique<PhysicalLoad>(unique_ptr_cast<ParseInfo, LoadInfo>(move(op.info)), op.estimated_cardinality);
	default:
		throw NotImplementedException("Unimplemented type for logical simple operator");
	}
}

} // namespace duckdb




namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalTopN &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);

	auto top_n =
	    make_unique<PhysicalTopN>(op.types, move(op.orders), (idx_t)op.limit, op.offset, op.estimated_cardinality);
	top_n->children.push_back(move(plan));
	return move(top_n);
}

} // namespace duckdb




namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalUnnest &op) {
	D_ASSERT(op.children.size() == 1);
	auto plan = CreatePlan(*op.children[0]);
	auto unnest = make_unique<PhysicalUnnest>(op.types, move(op.expressions), op.estimated_cardinality);
	unnest->children.push_back(move(plan));
	return move(unnest);
}

} // namespace duckdb





namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalUpdate &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);

	dependencies.insert(op.table);
	auto update = make_unique<PhysicalUpdate>(op.types, *op.table, *op.table->storage, op.columns, move(op.expressions),
	                                          move(op.bound_defaults), op.estimated_cardinality, op.return_chunk);

	update->update_is_del_and_insert = op.update_is_del_and_insert;
	update->children.push_back(move(plan));
	return move(update);
}

} // namespace duckdb








#include <numeric>

namespace duckdb {

static bool IsStreamingWindow(unique_ptr<Expression> &expr) {
	auto wexpr = reinterpret_cast<BoundWindowExpression *>(expr.get());
	if (!wexpr->partitions.empty() || !wexpr->orders.empty() || wexpr->ignore_nulls) {
		return false;
	}
	switch (wexpr->type) {
	// TODO: add more expression types here?
	case ExpressionType::WINDOW_FIRST_VALUE:
	case ExpressionType::WINDOW_PERCENT_RANK:
	case ExpressionType::WINDOW_RANK:
	case ExpressionType::WINDOW_RANK_DENSE:
	case ExpressionType::WINDOW_ROW_NUMBER:
		return true;
	default:
		return false;
	}
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalWindow &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);
#ifdef DEBUG
	for (auto &expr : op.expressions) {
		D_ASSERT(expr->IsWindow());
	}
#endif

	// Slice types
	auto types = op.types;
	const auto output_idx = types.size() - op.expressions.size();
	types.resize(output_idx);

	// Identify streaming windows
	vector<idx_t> blocking_windows;
	vector<idx_t> streaming_windows;
	for (idx_t expr_idx = 0; expr_idx < op.expressions.size(); expr_idx++) {
		if (IsStreamingWindow(op.expressions[expr_idx])) {
			streaming_windows.push_back(expr_idx);
		} else {
			blocking_windows.push_back(expr_idx);
		}
	}

	// Process the window functions by sharing the partition/order definitions
	vector<idx_t> evaluation_order;
	while (!blocking_windows.empty() || !streaming_windows.empty()) {
		const bool process_streaming = blocking_windows.empty();
		auto &remaining = process_streaming ? streaming_windows : blocking_windows;

		// Find all functions that share the partitioning of the first remaining expression
		const auto over_idx = remaining[0];
		auto over_expr = reinterpret_cast<BoundWindowExpression *>(op.expressions[over_idx].get());

		vector<idx_t> matching;
		vector<idx_t> unprocessed;
		for (const auto &expr_idx : remaining) {
			D_ASSERT(op.expressions[expr_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
			auto wexpr = reinterpret_cast<BoundWindowExpression *>(op.expressions[expr_idx].get());
			if (over_expr->KeysAreCompatible(wexpr)) {
				matching.emplace_back(expr_idx);
			} else {
				unprocessed.emplace_back(expr_idx);
			}
		}
		remaining.swap(unprocessed);

		// Extract the matching expressions
		vector<unique_ptr<Expression>> select_list;
		for (const auto &expr_idx : matching) {
			select_list.emplace_back(move(op.expressions[expr_idx]));
			types.emplace_back(op.types[output_idx + expr_idx]);
		}

		// Chain the new window operator on top of the plan
		unique_ptr<PhysicalOperator> window;
		if (process_streaming) {
			window = make_unique<PhysicalStreamingWindow>(types, move(select_list), op.estimated_cardinality);
		} else {
			window = make_unique<PhysicalWindow>(types, move(select_list), op.estimated_cardinality);
		}
		window->children.push_back(move(plan));
		plan = move(window);

		// Remember the projection order if we changed it
		if (!remaining.empty() || !evaluation_order.empty()) {
			evaluation_order.insert(evaluation_order.end(), matching.begin(), matching.end());
		}
	}

	// Put everything back into place if it moved
	if (!evaluation_order.empty()) {
		vector<unique_ptr<Expression>> select_list(op.types.size());
		// The inputs don't move
		for (idx_t i = 0; i < output_idx; ++i) {
			select_list[i] = make_unique<BoundReferenceExpression>(op.types[i], i);
		}
		// The outputs have been rearranged
		for (idx_t i = 0; i < evaluation_order.size(); ++i) {
			const auto expr_idx = evaluation_order[i] + output_idx;
			select_list[expr_idx] = make_unique<BoundReferenceExpression>(op.types[expr_idx], i + output_idx);
		}
		auto proj = make_unique<PhysicalProjection>(op.types, move(select_list), op.estimated_cardinality);
		proj->children.push_back(move(plan));
		plan = move(proj);
	}

	return plan;
}

} // namespace duckdb







namespace duckdb {

class DependencyExtractor : public LogicalOperatorVisitor {
public:
	explicit DependencyExtractor(unordered_set<CatalogEntry *> &dependencies) : dependencies(dependencies) {
	}

protected:
	unique_ptr<Expression> VisitReplace(BoundFunctionExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		// extract dependencies from the bound function expression
		if (expr.function.dependency) {
			expr.function.dependency(expr, dependencies);
		}
		return nullptr;
	}

private:
	unordered_set<CatalogEntry *> &dependencies;
};

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(unique_ptr<LogicalOperator> op) {
	auto &profiler = QueryProfiler::Get(context);

	// first resolve column references
	profiler.StartPhase("column_binding");
	ColumnBindingResolver resolver;
	resolver.VisitOperator(*op);
	profiler.EndPhase();

	// now resolve types of all the operators
	profiler.StartPhase("resolve_types");
	op->ResolveOperatorTypes();
	profiler.EndPhase();

	// extract dependencies from the logical plan
	DependencyExtractor extractor(dependencies);
	extractor.VisitOperator(*op);

	// then create the main physical plan
	profiler.StartPhase("create_plan");
	auto plan = CreatePlan(*op);
	profiler.EndPhase();

	plan->Verify();
	return plan;
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalOperator &op) {
	op.estimated_cardinality = op.EstimateCardinality(context);
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
		return CreatePlan((LogicalGet &)op);
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return CreatePlan((LogicalProjection &)op);
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		return CreatePlan((LogicalEmptyResult &)op);
	case LogicalOperatorType::LOGICAL_FILTER:
		return CreatePlan((LogicalFilter &)op);
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return CreatePlan((LogicalAggregate &)op);
	case LogicalOperatorType::LOGICAL_WINDOW:
		return CreatePlan((LogicalWindow &)op);
	case LogicalOperatorType::LOGICAL_UNNEST:
		return CreatePlan((LogicalUnnest &)op);
	case LogicalOperatorType::LOGICAL_LIMIT:
		return CreatePlan((LogicalLimit &)op);
	case LogicalOperatorType::LOGICAL_LIMIT_PERCENT:
		return CreatePlan((LogicalLimitPercent &)op);
	case LogicalOperatorType::LOGICAL_SAMPLE:
		return CreatePlan((LogicalSample &)op);
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		return CreatePlan((LogicalOrder &)op);
	case LogicalOperatorType::LOGICAL_TOP_N:
		return CreatePlan((LogicalTopN &)op);
	case LogicalOperatorType::LOGICAL_COPY_TO_FILE:
		return CreatePlan((LogicalCopyToFile &)op);
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		return CreatePlan((LogicalDummyScan &)op);
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
		return CreatePlan((LogicalAnyJoin &)op);
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		return CreatePlan((LogicalDelimJoin &)op);
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		return CreatePlan((LogicalComparisonJoin &)op);
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return CreatePlan((LogicalCrossProduct &)op);
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
		return CreatePlan((LogicalSetOperation &)op);
	case LogicalOperatorType::LOGICAL_INSERT:
		return CreatePlan((LogicalInsert &)op);
	case LogicalOperatorType::LOGICAL_DELETE:
		return CreatePlan((LogicalDelete &)op);
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
		return CreatePlan((LogicalChunkGet &)op);
	case LogicalOperatorType::LOGICAL_DELIM_GET:
		return CreatePlan((LogicalDelimGet &)op);
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
		return CreatePlan((LogicalExpressionGet &)op);
	case LogicalOperatorType::LOGICAL_UPDATE:
		return CreatePlan((LogicalUpdate &)op);
	case LogicalOperatorType::LOGICAL_CREATE_TABLE:
		return CreatePlan((LogicalCreateTable &)op);
	case LogicalOperatorType::LOGICAL_CREATE_INDEX:
		return CreatePlan((LogicalCreateIndex &)op);
	case LogicalOperatorType::LOGICAL_EXPLAIN:
		return CreatePlan((LogicalExplain &)op);
	case LogicalOperatorType::LOGICAL_SHOW:
		return CreatePlan((LogicalShow &)op);
	case LogicalOperatorType::LOGICAL_DISTINCT:
		return CreatePlan((LogicalDistinct &)op);
	case LogicalOperatorType::LOGICAL_PREPARE:
		return CreatePlan((LogicalPrepare &)op);
	case LogicalOperatorType::LOGICAL_EXECUTE:
		return CreatePlan((LogicalExecute &)op);
	case LogicalOperatorType::LOGICAL_CREATE_VIEW:
	case LogicalOperatorType::LOGICAL_CREATE_SEQUENCE:
	case LogicalOperatorType::LOGICAL_CREATE_SCHEMA:
	case LogicalOperatorType::LOGICAL_CREATE_MACRO:
	case LogicalOperatorType::LOGICAL_CREATE_TYPE:
		return CreatePlan((LogicalCreate &)op);
	case LogicalOperatorType::LOGICAL_PRAGMA:
		return CreatePlan((LogicalPragma &)op);
	case LogicalOperatorType::LOGICAL_TRANSACTION:
	case LogicalOperatorType::LOGICAL_ALTER:
	case LogicalOperatorType::LOGICAL_DROP:
	case LogicalOperatorType::LOGICAL_VACUUM:
	case LogicalOperatorType::LOGICAL_LOAD:
		return CreatePlan((LogicalSimple &)op);
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
		return CreatePlan((LogicalRecursiveCTE &)op);
	case LogicalOperatorType::LOGICAL_CTE_REF:
		return CreatePlan((LogicalCTERef &)op);
	case LogicalOperatorType::LOGICAL_EXPORT:
		return CreatePlan((LogicalExport &)op);
	case LogicalOperatorType::LOGICAL_SET:
		return CreatePlan((LogicalSet &)op);
	default:
		throw NotImplementedException("Unimplemented logical operator type!");
	}
}

} // namespace duckdb





namespace duckdb {

RadixPartitionedHashTable::RadixPartitionedHashTable(GroupingSet &grouping_set_p, const PhysicalHashAggregate &op_p)
    : grouping_set(grouping_set_p), op(op_p) {

	for (idx_t i = 0; i < op.groups.size(); i++) {
		if (grouping_set.find(i) == grouping_set.end()) {
			null_groups.push_back(i);
		}
	}

	// 10000 seems like a good compromise here
	radix_limit = 10000;

	if (grouping_set.empty()) {
		// fake a single group with a constant value for aggregation without groups
		group_types.emplace_back(LogicalType::TINYINT);
	}
	for (auto &entry : grouping_set) {
		D_ASSERT(entry < op.group_types.size());
		group_types.push_back(op.group_types[entry]);
	}
	// compute the GROUPING values
	// for each parameter to the GROUPING clause, we check if the hash table groups on this particular group
	// if it does, we return 0, otherwise we return 1
	// we then use bitshifts to combine these values
	for (auto &grouping : op.grouping_functions) {
		int64_t grouping_value = 0;
		for (idx_t i = 0; i < grouping.size(); i++) {
			if (grouping_set.find(grouping[i]) == grouping_set.end()) {
				// we don't group on this value!
				grouping_value += 1 << (grouping.size() - (i + 1));
			}
		}
		grouping_values.push_back(Value::BIGINT(grouping_value));
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class RadixHTGlobalState : public GlobalSinkState {
public:
	explicit RadixHTGlobalState(ClientContext &context)
	    : is_empty(true), multi_scan(true), total_groups(0),
	      partition_info((idx_t)TaskScheduler::GetScheduler(context).NumberOfThreads()) {
	}

	vector<unique_ptr<PartitionableHashTable>> intermediate_hts;
	vector<unique_ptr<GroupedAggregateHashTable>> finalized_hts;

	//! Whether or not any tuples were added to the HT
	bool is_empty;
	//! Whether or not the hash table should be scannable multiple times
	bool multi_scan;
	//! The lock for updating the global aggregate state
	mutex lock;
	//! a counter to determine if we should switch over to p
	atomic<idx_t> total_groups;

	bool is_finalized = false;
	bool is_partitioned = false;

	RadixPartitionInfo partition_info;
};

class RadixHTLocalState : public LocalSinkState {
public:
	explicit RadixHTLocalState(const RadixPartitionedHashTable &ht) : is_empty(true) {
		// if there are no groups we create a fake group so everything has the same group
		group_chunk.InitializeEmpty(ht.group_types);
		if (ht.grouping_set.empty()) {
			group_chunk.data[0].Reference(Value::TINYINT(42));
		}
	}

	DataChunk group_chunk;
	//! The aggregate HT
	unique_ptr<PartitionableHashTable> ht;

	//! Whether or not any tuples were added to the HT
	bool is_empty;
};

void RadixPartitionedHashTable::SetMultiScan(GlobalSinkState &state) {
	auto &gstate = (RadixHTGlobalState &)state;
	gstate.multi_scan = true;
}

unique_ptr<GlobalSinkState> RadixPartitionedHashTable::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<RadixHTGlobalState>(context);
}

unique_ptr<LocalSinkState> RadixPartitionedHashTable::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<RadixHTLocalState>(*this);
}

void RadixPartitionedHashTable::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                     DataChunk &input, DataChunk &aggregate_input_chunk) const {
	auto &llstate = (RadixHTLocalState &)lstate;
	auto &gstate = (RadixHTGlobalState &)state;
	D_ASSERT(!gstate.is_finalized);

	DataChunk &group_chunk = llstate.group_chunk;
	idx_t chunk_index = 0;
	for (auto &group_idx : grouping_set) {
		auto &group = op.groups[group_idx];
		D_ASSERT(group->type == ExpressionType::BOUND_REF);
		auto &bound_ref_expr = (BoundReferenceExpression &)*group;
		group_chunk.data[chunk_index++].Reference(input.data[bound_ref_expr.index]);
	}
	group_chunk.SetCardinality(input.size());
	group_chunk.Verify();

	// if we have non-combinable aggregates (e.g. string_agg) or any distinct aggregates we cannot keep parallel hash
	// tables
	if (ForceSingleHT(state)) {
		lock_guard<mutex> glock(gstate.lock);
		gstate.is_empty = gstate.is_empty && group_chunk.size() == 0;
		if (gstate.finalized_hts.empty()) {
			gstate.finalized_hts.push_back(make_unique<GroupedAggregateHashTable>(
			    Allocator::Get(context.client), BufferManager::GetBufferManager(context.client), group_types,
			    op.payload_types, op.bindings, HtEntryType::HT_WIDTH_64));
		}
		D_ASSERT(gstate.finalized_hts.size() == 1);
		D_ASSERT(gstate.finalized_hts[0]);
		gstate.total_groups += gstate.finalized_hts[0]->AddChunk(group_chunk, aggregate_input_chunk);
		return;
	}

	D_ASSERT(!op.any_distinct);

	if (group_chunk.size() > 0) {
		llstate.is_empty = false;
	}

	if (!llstate.ht) {
		llstate.ht = make_unique<PartitionableHashTable>(
		    Allocator::Get(context.client), BufferManager::GetBufferManager(context.client), gstate.partition_info,
		    group_types, op.payload_types, op.bindings);
	}

	gstate.total_groups +=
	    llstate.ht->AddChunk(group_chunk, aggregate_input_chunk,
	                         gstate.total_groups > radix_limit && gstate.partition_info.n_partitions > 1);
}

void RadixPartitionedHashTable::Combine(ExecutionContext &context, GlobalSinkState &state,
                                        LocalSinkState &lstate) const {
	auto &llstate = (RadixHTLocalState &)lstate;
	auto &gstate = (RadixHTGlobalState &)state;
	D_ASSERT(!gstate.is_finalized);

	// this actually does not do a lot but just pushes the local HTs into the global state so we can later combine them
	// in parallel

	if (ForceSingleHT(state)) {
		D_ASSERT(gstate.finalized_hts.size() <= 1);
		return;
	}

	if (!llstate.ht) {
		return; // no data
	}

	if (!llstate.ht->IsPartitioned() && gstate.partition_info.n_partitions > 1 && gstate.total_groups > radix_limit) {
		llstate.ht->Partition();
	}

	lock_guard<mutex> glock(gstate.lock);
	D_ASSERT(!op.any_distinct);

	if (!llstate.is_empty) {
		gstate.is_empty = false;
	}

	// we will never add new values to these HTs so we can drop the first part of the HT
	llstate.ht->Finalize();

	// at this point we just collect them the PhysicalHashAggregateFinalizeTask (below) will merge them in parallel
	gstate.intermediate_hts.push_back(move(llstate.ht));
}

bool RadixPartitionedHashTable::Finalize(ClientContext &context, GlobalSinkState &gstate_p) const {
	auto &gstate = (RadixHTGlobalState &)gstate_p;
	D_ASSERT(!gstate.is_finalized);
	gstate.is_finalized = true;

	// special case if we have non-combinable aggregates
	// we have already aggreagted into a global shared HT that does not require any additional finalization steps
	if (ForceSingleHT(gstate)) {
		D_ASSERT(gstate.finalized_hts.size() <= 1);
		D_ASSERT(gstate.finalized_hts.empty() || gstate.finalized_hts[0]);
		return false;
	}

	// we can have two cases now, non-partitioned for few groups and radix-partitioned for very many groups.
	// go through all of the child hts and see if we ever called partition() on any of them
	// if we did, its the latter case.
	bool any_partitioned = false;
	for (auto &pht : gstate.intermediate_hts) {
		if (pht->IsPartitioned()) {
			any_partitioned = true;
			break;
		}
	}

	auto &allocator = Allocator::Get(context);
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	if (any_partitioned) {
		// if one is partitioned, all have to be
		// this should mostly have already happened in Combine, but if not we do it here
		for (auto &pht : gstate.intermediate_hts) {
			if (!pht->IsPartitioned()) {
				pht->Partition();
			}
		}
		// schedule additional tasks to combine the partial HTs
		gstate.finalized_hts.resize(gstate.partition_info.n_partitions);
		for (idx_t r = 0; r < gstate.partition_info.n_partitions; r++) {
			gstate.finalized_hts[r] = make_unique<GroupedAggregateHashTable>(
			    allocator, buffer_manager, group_types, op.payload_types, op.bindings, HtEntryType::HT_WIDTH_64);
		}
		gstate.is_partitioned = true;
		return true;
	} else { // in the non-partitioned case we immediately combine all the unpartitioned hts created by the threads.
		     // TODO possible optimization, if total count < limit for 32 bit ht, use that one
		     // create this ht here so finalize needs no lock on gstate

		gstate.finalized_hts.push_back(make_unique<GroupedAggregateHashTable>(
		    allocator, buffer_manager, group_types, op.payload_types, op.bindings, HtEntryType::HT_WIDTH_64));
		for (auto &pht : gstate.intermediate_hts) {
			auto unpartitioned = pht->GetUnpartitioned();
			for (auto &unpartitioned_ht : unpartitioned) {
				D_ASSERT(unpartitioned_ht);
				gstate.finalized_hts[0]->Combine(*unpartitioned_ht);
				unpartitioned_ht.reset();
			}
			unpartitioned.clear();
		}
		D_ASSERT(gstate.finalized_hts[0]);
		gstate.finalized_hts[0]->Finalize();
		return false;
	}
}

// this task is run in multiple threads and combines the radix-partitioned hash tables into a single onen and then
// folds them into the global ht finally.
class RadixAggregateFinalizeTask : public ExecutorTask {
public:
	RadixAggregateFinalizeTask(Executor &executor, shared_ptr<Event> event_p, RadixHTGlobalState &state_p,
	                           idx_t radix_p)
	    : ExecutorTask(executor), event(move(event_p)), state(state_p), radix(radix_p) {
	}

	static void FinalizeHT(RadixHTGlobalState &gstate, idx_t radix) {
		D_ASSERT(gstate.partition_info.n_partitions <= gstate.finalized_hts.size());
		D_ASSERT(gstate.finalized_hts[radix]);
		for (auto &pht : gstate.intermediate_hts) {
			for (auto &ht : pht->GetPartition(radix)) {
				gstate.finalized_hts[radix]->Combine(*ht);
				ht.reset();
			}
		}
		gstate.finalized_hts[radix]->Finalize();
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		FinalizeHT(state, radix);
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	shared_ptr<Event> event;
	RadixHTGlobalState &state;
	idx_t radix;
};

void RadixPartitionedHashTable::ScheduleTasks(Executor &executor, const shared_ptr<Event> &event,
                                              GlobalSinkState &state, vector<unique_ptr<Task>> &tasks) const {
	auto &gstate = (RadixHTGlobalState &)state;
	if (!gstate.is_partitioned) {
		return;
	}
	for (idx_t r = 0; r < gstate.partition_info.n_partitions; r++) {
		D_ASSERT(gstate.partition_info.n_partitions <= gstate.finalized_hts.size());
		D_ASSERT(gstate.finalized_hts[r]);
		tasks.push_back(make_unique<RadixAggregateFinalizeTask>(executor, event, gstate, r));
	}
}

bool RadixPartitionedHashTable::ForceSingleHT(GlobalSinkState &state) const {
	auto &gstate = (RadixHTGlobalState &)state;
	return op.any_distinct || gstate.partition_info.n_partitions < 2;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class RadixHTGlobalSourceState : public GlobalSourceState {
public:
	explicit RadixHTGlobalSourceState(Allocator &allocator, const RadixPartitionedHashTable &ht)
	    : ht_index(0), ht_scan_position(0), finished(false) {
		auto scan_chunk_types = ht.group_types;
		for (auto &aggr_type : ht.op.aggregate_return_types) {
			scan_chunk_types.push_back(aggr_type);
		}
		scan_chunk.Initialize(allocator, scan_chunk_types);
	}

	//! Materialized GROUP BY expressions & aggregates
	DataChunk scan_chunk;
	//! The current position to scan the HT for output tuples
	idx_t ht_index;
	idx_t ht_scan_position;
	bool finished;
};

unique_ptr<GlobalSourceState> RadixPartitionedHashTable::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<RadixHTGlobalSourceState>(Allocator::Get(context), *this);
}

void RadixPartitionedHashTable::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSinkState &sink_state,
                                        GlobalSourceState &source_state) const {
	auto &gstate = (RadixHTGlobalState &)sink_state;
	auto &state = (RadixHTGlobalSourceState &)source_state;
	D_ASSERT(gstate.is_finalized);
	if (state.finished) {
		return;
	}

	state.scan_chunk.Reset();
	// special case hack to sort out aggregating from empty intermediates
	// for aggregations without groups
	if (gstate.is_empty && grouping_set.empty()) {
		D_ASSERT(chunk.ColumnCount() == null_groups.size() + op.aggregates.size());
		// for each column in the aggregates, set to initial state
		chunk.SetCardinality(1);
		for (auto null_group : null_groups) {
			chunk.data[null_group].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(chunk.data[null_group], true);
		}
		for (idx_t i = 0; i < op.aggregates.size(); i++) {
			D_ASSERT(op.aggregates[i]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
			auto &aggr = (BoundAggregateExpression &)*op.aggregates[i];
			auto aggr_state = unique_ptr<data_t[]>(new data_t[aggr.function.state_size()]);
			aggr.function.initialize(aggr_state.get());

			AggregateInputData aggr_input_data(aggr.bind_info.get());
			Vector state_vector(Value::POINTER((uintptr_t)aggr_state.get()));
			aggr.function.finalize(state_vector, aggr_input_data, chunk.data[null_groups.size() + i], 1, 0);
			if (aggr.function.destructor) {
				aggr.function.destructor(state_vector, 1);
			}
		}
		state.finished = true;
		return;
	}
	if (gstate.is_empty && !state.finished) {
		state.finished = true;
		return;
	}
	idx_t elements_found = 0;

	while (true) {
		if (state.ht_index == gstate.finalized_hts.size()) {
			state.finished = true;
			return;
		}
		D_ASSERT(gstate.finalized_hts[state.ht_index]);
		elements_found = gstate.finalized_hts[state.ht_index]->Scan(state.ht_scan_position, state.scan_chunk);

		if (elements_found > 0) {
			break;
		}
		if (!gstate.multi_scan) {
			gstate.finalized_hts[state.ht_index].reset();
		}
		state.ht_index++;
		state.ht_scan_position = 0;
	}

	// compute the final projection list
	chunk.SetCardinality(elements_found);

	idx_t chunk_index = 0;
	for (auto &entry : grouping_set) {
		chunk.data[entry].Reference(state.scan_chunk.data[chunk_index++]);
	}
	for (auto null_group : null_groups) {
		chunk.data[null_group].SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(chunk.data[null_group], true);
	}
	for (idx_t col_idx = 0; col_idx < op.aggregates.size(); col_idx++) {
		chunk.data[op.groups.size() + col_idx].Reference(state.scan_chunk.data[group_types.size() + col_idx]);
	}
	D_ASSERT(op.grouping_functions.size() == grouping_values.size());
	for (idx_t i = 0; i < op.grouping_functions.size(); i++) {
		chunk.data[op.groups.size() + op.aggregates.size() + i].Reference(grouping_values[i]);
	}
}

} // namespace duckdb



namespace duckdb {

ReservoirSample::ReservoirSample(Allocator &allocator, idx_t sample_count, int64_t seed)
    : BlockingSample(seed), sample_count(sample_count), reservoir(allocator) {
}

void ReservoirSample::AddToReservoir(DataChunk &input) {
	if (sample_count == 0) {
		return;
	}
	// Input: A population V of n weighted items
	// Output: A reservoir R with a size m
	// 1: The first m items of V are inserted into R
	// first we need to check if the reservoir already has "m" elements
	if (reservoir.Count() < sample_count) {
		if (FillReservoir(input) == 0) {
			// entire chunk was consumed by reservoir
			return;
		}
	}
	// find the position of next_index relative to current_count
	idx_t remaining = input.size();
	idx_t base_offset = 0;
	while (true) {
		idx_t offset = base_reservoir_sample.next_index - base_reservoir_sample.current_count;
		if (offset >= remaining) {
			// not in this chunk! increment current count and go to the next chunk
			base_reservoir_sample.current_count += remaining;
			return;
		}
		// in this chunk! replace the element
		ReplaceElement(input, base_offset + offset);
		// shift the chunk forward
		remaining -= offset;
		base_offset += offset;
	}
}

unique_ptr<DataChunk> ReservoirSample::GetChunk() {
	return reservoir.Fetch();
}

void ReservoirSample::ReplaceElement(DataChunk &input, idx_t index_in_chunk) {
	// replace the entry in the reservoir
	// 8. The item in R with the minimum key is replaced by item vi
	for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
		reservoir.SetValue(col_idx, base_reservoir_sample.min_entry, input.GetValue(col_idx, index_in_chunk));
	}
	base_reservoir_sample.ReplaceElement();
}

idx_t ReservoirSample::FillReservoir(DataChunk &input) {
	idx_t chunk_count = input.size();
	input.Normalify();

	// we have not: append to the reservoir
	idx_t required_count;
	if (reservoir.Count() + chunk_count >= sample_count) {
		// have to limit the count of the chunk
		required_count = sample_count - reservoir.Count();
	} else {
		// we copy the entire chunk
		required_count = chunk_count;
	}
	// instead of copying we just change the pointer in the current chunk
	input.SetCardinality(required_count);
	reservoir.Append(input);

	base_reservoir_sample.InitializeReservoir(reservoir.Count(), sample_count);

	// check if there are still elements remaining
	// this happens if we are on a boundary
	// for example, input.size() is 1024, but our sample size is 10
	if (required_count == chunk_count) {
		// we are done here
		return 0;
	}
	// we still need to process a part of the chunk
	// create a selection vector of the remaining elements
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	for (idx_t i = required_count; i < chunk_count; i++) {
		sel.set_index(i - required_count, i);
	}
	// slice the input vector and continue
	input.Slice(sel, chunk_count - required_count);
	return input.size();
}

ReservoirSamplePercentage::ReservoirSamplePercentage(Allocator &allocator, double percentage, int64_t seed)
    : BlockingSample(seed), allocator(allocator), sample_percentage(percentage / 100.0), current_count(0),
      is_finalized(false) {
	reservoir_sample_size = idx_t(sample_percentage * RESERVOIR_THRESHOLD);
	current_sample = make_unique<ReservoirSample>(allocator, reservoir_sample_size, random.NextRandomInteger());
}

void ReservoirSamplePercentage::AddToReservoir(DataChunk &input) {
	if (current_count + input.size() > RESERVOIR_THRESHOLD) {
		// we don't have enough space in our current reservoir
		// first check what we still need to append to the current sample
		idx_t append_to_current_sample_count = RESERVOIR_THRESHOLD - current_count;
		idx_t append_to_next_sample = input.size() - append_to_current_sample_count;
		if (append_to_current_sample_count > 0) {
			// we have elements remaining, first add them to the current sample
			input.Normalify();

			input.SetCardinality(append_to_current_sample_count);
			current_sample->AddToReservoir(input);
		}
		if (append_to_next_sample > 0) {
			// slice the input for the remainder
			SelectionVector sel(STANDARD_VECTOR_SIZE);
			for (idx_t i = 0; i < append_to_next_sample; i++) {
				sel.set_index(i, append_to_current_sample_count + i);
			}
			input.Slice(sel, append_to_next_sample);
		}
		// now our first sample is filled: append it to the set of finished samples
		finished_samples.push_back(move(current_sample));

		// allocate a new sample, and potentially add the remainder of the current input to that sample
		current_sample = make_unique<ReservoirSample>(allocator, reservoir_sample_size, random.NextRandomInteger());
		if (append_to_next_sample > 0) {
			current_sample->AddToReservoir(input);
		}
		current_count = append_to_next_sample;
	} else {
		// we can just append to the current sample
		current_count += input.size();
		current_sample->AddToReservoir(input);
	}
}

unique_ptr<DataChunk> ReservoirSamplePercentage::GetChunk() {
	if (!is_finalized) {
		Finalize();
	}
	while (!finished_samples.empty()) {
		auto &front = finished_samples.front();
		auto chunk = front->GetChunk();
		if (chunk && chunk->size() > 0) {
			return chunk;
		}
		// move to the next sample
		finished_samples.erase(finished_samples.begin());
	}
	return nullptr;
}

void ReservoirSamplePercentage::Finalize() {
	// need to finalize the current sample, if any
	if (current_count > 0) {
		// create a new sample
		auto new_sample_size = idx_t(round(sample_percentage * current_count));
		auto new_sample = make_unique<ReservoirSample>(allocator, new_sample_size, random.NextRandomInteger());
		while (true) {
			auto chunk = current_sample->GetChunk();
			if (!chunk || chunk->size() == 0) {
				break;
			}
			new_sample->AddToReservoir(*chunk);
		}
		finished_samples.push_back(move(new_sample));
	}
	is_finalized = true;
}

BaseReservoirSampling::BaseReservoirSampling(int64_t seed) : random(seed) {
	next_index = 0;
	min_threshold = 0;
	min_entry = 0;
	current_count = 0;
}

BaseReservoirSampling::BaseReservoirSampling() : BaseReservoirSampling(-1) {
}

void BaseReservoirSampling::InitializeReservoir(idx_t cur_size, idx_t sample_size) {
	//! 1: The first m items of V are inserted into R
	//! first we need to check if the reservoir already has "m" elements
	if (cur_size == sample_size) {
		//! 2. For each item vi ∈ R: Calculate a key ki = random(0, 1)
		//! we then define the threshold to enter the reservoir T_w as the minimum key of R
		//! we use a priority queue to extract the minimum key in O(1) time
		for (idx_t i = 0; i < sample_size; i++) {
			double k_i = random.NextRandom();
			reservoir_weights.push(std::make_pair(-k_i, i));
		}
		SetNextEntry();
	}
}

void BaseReservoirSampling::SetNextEntry() {
	//! 4. Let r = random(0, 1) and Xw = log(r) / log(T_w)
	auto &min_key = reservoir_weights.top();
	double t_w = -min_key.first;
	double r = random.NextRandom();
	double x_w = log(r) / log(t_w);
	//! 5. From the current item vc skip items until item vi , such that:
	//! 6. wc +wc+1 +···+wi−1 < Xw <= wc +wc+1 +···+wi−1 +wi
	//! since all our weights are 1 (uniform sampling), we can just determine the amount of elements to skip
	min_threshold = t_w;
	min_entry = min_key.second;
	next_index = MaxValue<idx_t>(1, idx_t(round(x_w)));
	current_count = 0;
}

void BaseReservoirSampling::ReplaceElement() {
	//! replace the entry in the reservoir
	//! pop the minimum entry
	reservoir_weights.pop();
	//! now update the reservoir
	//! 8. Let tw = Tw i , r2 = random(tw,1) and vi’s key: ki = (r2)1/wi
	//! 9. The new threshold Tw is the new minimum key of R
	//! we generate a random number between (min_threshold, 1)
	double r2 = random.NextRandom(min_threshold, 1);
	//! now we insert the new weight into the reservoir
	reservoir_weights.push(std::make_pair(-r2, min_entry));
	//! we update the min entry with the new min entry in the reservoir
	SetNextEntry();
}

} // namespace duckdb






namespace duckdb {

WindowSegmentTree::WindowSegmentTree(AggregateFunction &aggregate, FunctionData *bind_info,
                                     const LogicalType &result_type_p, ChunkCollection *input,
                                     const ValidityMask &filter_mask_p, WindowAggregationMode mode_p)
    : aggregate(aggregate), bind_info(bind_info), result_type(result_type_p), state(aggregate.state_size()),
      statep(Value::POINTER((idx_t)state.data())), frame(0, 0), active(0, 1),
      statev(Value::POINTER((idx_t)state.data())), internal_nodes(0), input_ref(input), filter_mask(filter_mask_p),
      mode(mode_p) {
#if STANDARD_VECTOR_SIZE < 512
	throw NotImplementedException("Window functions are not supported for vector sizes < 512");
#endif
	statep.Normalify(STANDARD_VECTOR_SIZE);
	statev.SetVectorType(VectorType::FLAT_VECTOR); // Prevent conversion of results to constants

	if (input_ref && input_ref->ColumnCount() > 0) {
		filter_sel.Initialize(STANDARD_VECTOR_SIZE);
		inputs.Initialize(Allocator::DefaultAllocator(), input_ref->Types());
		// if we have a frame-by-frame method, share the single state
		if (aggregate.window && UseWindowAPI()) {
			AggregateInit();
			inputs.Reference(input_ref->GetChunk(0));
		} else if (aggregate.combine && UseCombineAPI()) {
			ConstructTree();
		}
	}
}

WindowSegmentTree::~WindowSegmentTree() {
	if (!aggregate.destructor) {
		// nothing to destroy
		return;
	}
	// call the destructor for all the intermediate states
	data_ptr_t address_data[STANDARD_VECTOR_SIZE];
	Vector addresses(LogicalType::POINTER, (data_ptr_t)address_data);
	idx_t count = 0;
	for (idx_t i = 0; i < internal_nodes; i++) {
		address_data[count++] = data_ptr_t(levels_flat_native.get() + i * state.size());
		if (count == STANDARD_VECTOR_SIZE) {
			aggregate.destructor(addresses, count);
			count = 0;
		}
	}
	if (count > 0) {
		aggregate.destructor(addresses, count);
	}

	if (aggregate.window && UseWindowAPI()) {
		aggregate.destructor(statev, 1);
	}
}

void WindowSegmentTree::AggregateInit() {
	aggregate.initialize(state.data());
}

void WindowSegmentTree::AggegateFinal(Vector &result, idx_t rid) {
	AggregateInputData aggr_input_data(bind_info);
	aggregate.finalize(statev, aggr_input_data, result, 1, rid);

	if (aggregate.destructor) {
		aggregate.destructor(statev, 1);
	}
}

void WindowSegmentTree::ExtractFrame(idx_t begin, idx_t end) {
	const auto size = end - begin;
	if (size >= STANDARD_VECTOR_SIZE) {
		throw InternalException("Cannot compute window aggregation: bounds are too large");
	}

	const idx_t start_in_vector = begin % STANDARD_VECTOR_SIZE;
	const auto input_count = input_ref->ColumnCount();
	if (start_in_vector + size <= STANDARD_VECTOR_SIZE) {
		inputs.SetCardinality(size);
		auto &chunk = input_ref->GetChunkForRow(begin);
		for (idx_t i = 0; i < input_count; ++i) {
			auto &v = inputs.data[i];
			auto &vec = chunk.data[i];
			v.Slice(vec, start_in_vector);
			v.Verify(size);
		}
	} else {
		inputs.Reset();
		inputs.SetCardinality(size);

		// we cannot just slice the individual vector!
		auto &chunk_a = input_ref->GetChunkForRow(begin);
		auto &chunk_b = input_ref->GetChunkForRow(end);
		idx_t chunk_a_count = chunk_a.size() - start_in_vector;
		idx_t chunk_b_count = inputs.size() - chunk_a_count;
		for (idx_t i = 0; i < input_count; ++i) {
			auto &v = inputs.data[i];
			VectorOperations::Copy(chunk_a.data[i], v, chunk_a.size(), start_in_vector, 0);
			VectorOperations::Copy(chunk_b.data[i], v, chunk_b_count, 0, chunk_a_count);
		}
	}

	// Slice to any filtered rows
	if (!filter_mask.AllValid()) {
		idx_t filtered = 0;
		for (idx_t i = begin; i < end; ++i) {
			if (filter_mask.RowIsValid(i)) {
				filter_sel.set_index(filtered++, i - begin);
			}
		}
		if (filtered != inputs.size()) {
			inputs.Slice(filter_sel, filtered);
		}
	}
}

void WindowSegmentTree::WindowSegmentValue(idx_t l_idx, idx_t begin, idx_t end) {
	D_ASSERT(begin <= end);
	if (begin == end) {
		return;
	}

	if (end - begin >= STANDARD_VECTOR_SIZE) {
		throw InternalException("Cannot compute window aggregation: bounds are too large");
	}

	Vector s(statep, 0);
	if (l_idx == 0) {
		ExtractFrame(begin, end);
		AggregateInputData aggr_input_data(bind_info);
		aggregate.update(&inputs.data[0], aggr_input_data, input_ref->ColumnCount(), s, inputs.size());
	} else {
		inputs.Reset();
		inputs.SetCardinality(end - begin);
		// find out where the states begin
		data_ptr_t begin_ptr = levels_flat_native.get() + state.size() * (begin + levels_flat_start[l_idx - 1]);
		// set up a vector of pointers that point towards the set of states
		Vector v(LogicalType::POINTER);
		auto pdata = FlatVector::GetData<data_ptr_t>(v);
		for (idx_t i = 0; i < inputs.size(); i++) {
			pdata[i] = begin_ptr + i * state.size();
		}
		v.Verify(inputs.size());
		AggregateInputData aggr_input_data(bind_info);
		aggregate.combine(v, s, aggr_input_data, inputs.size());
	}
}

void WindowSegmentTree::ConstructTree() {
	D_ASSERT(input_ref);
	D_ASSERT(inputs.ColumnCount() > 0);

	// compute space required to store internal nodes of segment tree
	internal_nodes = 0;
	idx_t level_nodes = input_ref->Count();
	do {
		level_nodes = (level_nodes + (TREE_FANOUT - 1)) / TREE_FANOUT;
		internal_nodes += level_nodes;
	} while (level_nodes > 1);
	levels_flat_native = unique_ptr<data_t[]>(new data_t[internal_nodes * state.size()]);
	levels_flat_start.push_back(0);

	idx_t levels_flat_offset = 0;
	idx_t level_current = 0;
	// level 0 is data itself
	idx_t level_size;
	// iterate over the levels of the segment tree
	while ((level_size = (level_current == 0 ? input_ref->Count()
	                                         : levels_flat_offset - levels_flat_start[level_current - 1])) > 1) {
		for (idx_t pos = 0; pos < level_size; pos += TREE_FANOUT) {
			// compute the aggregate for this entry in the segment tree
			AggregateInit();
			WindowSegmentValue(level_current, pos, MinValue(level_size, pos + TREE_FANOUT));

			memcpy(levels_flat_native.get() + (levels_flat_offset * state.size()), state.data(), state.size());

			levels_flat_offset++;
		}

		levels_flat_start.push_back(levels_flat_offset);
		level_current++;
	}

	// Corner case: single element in the window
	if (levels_flat_offset == 0) {
		aggregate.initialize(levels_flat_native.get());
	}
}

void WindowSegmentTree::Compute(Vector &result, idx_t rid, idx_t begin, idx_t end) {
	D_ASSERT(input_ref);

	// No arguments, so just count
	if (inputs.ColumnCount() == 0) {
		D_ASSERT(GetTypeIdSize(result_type.InternalType()) == sizeof(idx_t));
		auto data = FlatVector::GetData<idx_t>(result);
		// Slice to any filtered rows
		if (!filter_mask.AllValid()) {
			idx_t filtered = 0;
			for (idx_t i = begin; i < end; ++i) {
				filtered += filter_mask.RowIsValid(i);
			}
			data[rid] = filtered;
		} else {
			data[rid] = end - begin;
		}
		return;
	}

	// If we have a window function, use that
	if (aggregate.window && UseWindowAPI()) {
		// Frame boundaries
		auto prev = frame;
		frame = FrameBounds(begin, end);

		// Extract the range
		auto &coll = *input_ref;
		const auto prev_active = active;
		const FrameBounds combined(MinValue(frame.first, prev.first), MaxValue(frame.second, prev.second));

		// The chunk bounds are the range that includes the begin and end - 1
		const FrameBounds prev_chunks(coll.LocateChunk(prev_active.first), coll.LocateChunk(prev_active.second - 1));
		const FrameBounds active_chunks(coll.LocateChunk(combined.first), coll.LocateChunk(combined.second - 1));

		// Extract the range
		if (active_chunks.first == active_chunks.second) {
			// If all the data is in a single chunk, then just reference it
			if (prev_chunks != active_chunks || (!prev.first && !prev.second)) {
				inputs.Reference(coll.GetChunk(active_chunks.first));
			}
		} else if (active_chunks.first == prev_chunks.first && prev_chunks.first != prev_chunks.second) {
			// If the start chunk did not change, and we are not just a reference, then extend if necessary
			for (auto chunk_idx = prev_chunks.second + 1; chunk_idx <= active_chunks.second; ++chunk_idx) {
				inputs.Append(coll.GetChunk(chunk_idx), true);
			}
		} else {
			// If the first chunk changed, start over
			inputs.Reset();
			for (auto chunk_idx = active_chunks.first; chunk_idx <= active_chunks.second; ++chunk_idx) {
				inputs.Append(coll.GetChunk(chunk_idx), true);
			}
		}

		active = FrameBounds(active_chunks.first * STANDARD_VECTOR_SIZE,
		                     MinValue((active_chunks.second + 1) * STANDARD_VECTOR_SIZE, coll.Count()));

		AggregateInputData aggr_input_data(bind_info);
		aggregate.window(inputs.data.data(), filter_mask, aggr_input_data, inputs.ColumnCount(), state.data(), frame,
		                 prev, result, rid, active.first);
		return;
	}

	AggregateInit();

	// Aggregate everything at once if we can't combine states
	if (!aggregate.combine || !UseCombineAPI()) {
		WindowSegmentValue(0, begin, end);
		AggegateFinal(result, rid);
		return;
	}

	for (idx_t l_idx = 0; l_idx < levels_flat_start.size() + 1; l_idx++) {
		idx_t parent_begin = begin / TREE_FANOUT;
		idx_t parent_end = end / TREE_FANOUT;
		if (parent_begin == parent_end) {
			WindowSegmentValue(l_idx, begin, end);
			break;
		}
		idx_t group_begin = parent_begin * TREE_FANOUT;
		if (begin != group_begin) {
			WindowSegmentValue(l_idx, begin, group_begin + TREE_FANOUT);
			parent_begin++;
		}
		idx_t group_end = parent_end * TREE_FANOUT;
		if (end != group_end) {
			WindowSegmentValue(l_idx, group_end, end);
		}
		begin = parent_begin;
		end = parent_end;
	}

	AggegateFinal(result, rid);
}

} // namespace duckdb







namespace duckdb {

template <class T>
struct AvgState {
	uint64_t count;
	T value;

	void Initialize() {
		this->count = 0;
	}

	void Combine(const AvgState<T> &other) {
		this->count += other.count;
		this->value += other.value;
	}
};

struct KahanAvgState {
	uint64_t count;
	double value;
	double err;

	void Initialize() {
		this->count = 0;
		this->err = 0.0;
	}

	void Combine(const KahanAvgState &other) {
		this->count += other.count;
		KahanAddInternal(other.value, this->value, this->err);
		KahanAddInternal(other.err, this->value, this->err);
	}
};

struct AverageDecimalBindData : public FunctionData {
	explicit AverageDecimalBindData(double scale) : scale(scale) {
	}

	double scale;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_unique<AverageDecimalBindData>(scale);
	};

	bool Equals(const FunctionData &other_p) const override {
		auto &other = (AverageDecimalBindData &)other_p;
		return scale == other.scale;
	}
};

struct AverageSetOperation {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->Initialize();
	}
	template <class STATE>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
		target->Combine(source);
	}
	template <class STATE>
	static void AddValues(STATE *state, idx_t count) {
		state->count += count;
	}
};

template <class T>
static T GetAverageDivident(uint64_t count, FunctionData *bind_data) {
	T divident = T(count);
	if (bind_data) {
		auto &avg_bind_data = (AverageDecimalBindData &)*bind_data;
		divident *= avg_bind_data.scale;
	}
	return divident;
}

struct IntegerAverageOperation : public BaseSumOperation<AverageSetOperation, RegularAdd> {
	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &aggr_input_data, STATE *state, T *target,
	                     ValidityMask &mask, idx_t idx) {
		if (state->count == 0) {
			mask.SetInvalid(idx);
		} else {
			double divident = GetAverageDivident<double>(state->count, aggr_input_data.bind_data);
			target[idx] = double(state->value) / divident;
		}
	}
};

struct IntegerAverageOperationHugeint : public BaseSumOperation<AverageSetOperation, HugeintAdd> {
	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &aggr_input_data, STATE *state, T *target,
	                     ValidityMask &mask, idx_t idx) {
		if (state->count == 0) {
			mask.SetInvalid(idx);
		} else {
			long double divident = GetAverageDivident<long double>(state->count, aggr_input_data.bind_data);
			target[idx] = Hugeint::Cast<long double>(state->value) / divident;
		}
	}
};

struct HugeintAverageOperation : public BaseSumOperation<AverageSetOperation, RegularAdd> {
	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &aggr_input_data, STATE *state, T *target,
	                     ValidityMask &mask, idx_t idx) {
		if (state->count == 0) {
			mask.SetInvalid(idx);
		} else {
			long double divident = GetAverageDivident<long double>(state->count, aggr_input_data.bind_data);
			target[idx] = Hugeint::Cast<long double>(state->value) / divident;
		}
	}
};

struct NumericAverageOperation : public BaseSumOperation<AverageSetOperation, RegularAdd> {
	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (state->count == 0) {
			mask.SetInvalid(idx);
		} else {
			if (!Value::DoubleIsFinite(state->value)) {
				throw OutOfRangeException("AVG is out of range!");
			}
			target[idx] = (state->value / state->count);
		}
	}
};

struct KahanAverageOperation : public BaseSumOperation<AverageSetOperation, KahanAdd> {
	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (state->count == 0) {
			mask.SetInvalid(idx);
		} else {
			if (!Value::DoubleIsFinite(state->value)) {
				throw OutOfRangeException("AVG is out of range!");
			}
			target[idx] = (state->value / state->count) + (state->err / state->count);
		}
	}
};

AggregateFunction GetAverageAggregate(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT16:
		return AggregateFunction::UnaryAggregate<AvgState<int64_t>, int16_t, double, IntegerAverageOperation>(
		    LogicalType::SMALLINT, LogicalType::DOUBLE, true);
	case PhysicalType::INT32: {
		auto function =
		    AggregateFunction::UnaryAggregate<AvgState<hugeint_t>, int32_t, double, IntegerAverageOperationHugeint>(
		        LogicalType::INTEGER, LogicalType::DOUBLE, true);
		return function;
	}
	case PhysicalType::INT64: {
		auto function =
		    AggregateFunction::UnaryAggregate<AvgState<hugeint_t>, int64_t, double, IntegerAverageOperationHugeint>(
		        LogicalType::BIGINT, LogicalType::DOUBLE, true);
		return function;
	}
	case PhysicalType::INT128:
		return AggregateFunction::UnaryAggregate<AvgState<hugeint_t>, hugeint_t, double, HugeintAverageOperation>(
		    LogicalType::HUGEINT, LogicalType::DOUBLE, true);
	default:
		throw InternalException("Unimplemented average aggregate");
	}
}

unique_ptr<FunctionData> BindDecimalAvg(ClientContext &context, AggregateFunction &function,
                                        vector<unique_ptr<Expression>> &arguments) {
	auto decimal_type = arguments[0]->return_type;
	function = GetAverageAggregate(decimal_type.InternalType());
	function.name = "avg";
	function.arguments[0] = decimal_type;
	function.return_type = LogicalType::DOUBLE;
	return make_unique<AverageDecimalBindData>(
	    Hugeint::Cast<double>(Hugeint::POWERS_OF_TEN[DecimalType::GetScale(decimal_type)]));
}

void AvgFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet avg("avg");
	avg.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr,
	                                  nullptr, nullptr, true, nullptr, BindDecimalAvg));
	avg.AddFunction(GetAverageAggregate(PhysicalType::INT16));
	avg.AddFunction(GetAverageAggregate(PhysicalType::INT32));
	avg.AddFunction(GetAverageAggregate(PhysicalType::INT64));
	avg.AddFunction(GetAverageAggregate(PhysicalType::INT128));
	avg.AddFunction(AggregateFunction::UnaryAggregate<AvgState<double>, double, double, NumericAverageOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, true));
	set.AddFunction(avg);

	avg.name = "mean";
	set.AddFunction(avg);

	AggregateFunctionSet favg("favg");
	favg.AddFunction(AggregateFunction::UnaryAggregate<KahanAvgState, double, double, KahanAverageOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, true));
	set.AddFunction(favg);
}

} // namespace duckdb






namespace duckdb {
void Corr::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet corr("corr");
	corr.AddFunction(AggregateFunction::BinaryAggregate<CorrState, double, double, double, CorrOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(corr);
}
} // namespace duckdb






#include <cmath>

namespace duckdb {

void CovarPopFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet covar_pop("covar_pop");
	covar_pop.AddFunction(AggregateFunction::BinaryAggregate<CovarState, double, double, double, CovarPopOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(covar_pop);
}

void CovarSampFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet covar_samp("covar_samp");
	covar_samp.AddFunction(AggregateFunction::BinaryAggregate<CovarState, double, double, double, CovarSampOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(covar_samp);
}

} // namespace duckdb




#include <cmath>

namespace duckdb {

void StdDevSampFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet stddev_samp("stddev_samp");
	stddev_samp.AddFunction(AggregateFunction::UnaryAggregate<StddevState, double, double, STDDevSampOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(stddev_samp);
	AggregateFunctionSet stddev("stddev");
	stddev.AddFunction(AggregateFunction::UnaryAggregate<StddevState, double, double, STDDevSampOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(stddev);
}

void StdDevPopFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet stddev_pop("stddev_pop");
	stddev_pop.AddFunction(AggregateFunction::UnaryAggregate<StddevState, double, double, STDDevPopOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(stddev_pop);
}

void VarPopFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet var_pop("var_pop");
	var_pop.AddFunction(AggregateFunction::UnaryAggregate<StddevState, double, double, VarPopOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(var_pop);
}

void VarSampFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet var_samp("var_samp");
	var_samp.AddFunction(AggregateFunction::UnaryAggregate<StddevState, double, double, VarSampOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(var_samp);
}
void VarianceFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet var_samp("variance");
	var_samp.AddFunction(AggregateFunction::UnaryAggregate<StddevState, double, double, VarSampOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(var_samp);
}

void StandardErrorOfTheMeanFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet sem("sem");
	sem.AddFunction(AggregateFunction::UnaryAggregate<StddevState, double, double, StandardErrorOfTheMeanOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(sem);
}

} // namespace duckdb



namespace duckdb {

void BuiltinFunctions::RegisterAlgebraicAggregates() {
	Register<AvgFun>();

	Register<CovarSampFun>();
	Register<CovarPopFun>();

	Register<StdDevSampFun>();
	Register<StdDevPopFun>();
	Register<VarPopFun>();
	Register<VarSampFun>();
	Register<VarianceFun>();
	Register<StandardErrorOfTheMeanFun>();
	Register<Corr>();
}

} // namespace duckdb








namespace duckdb {

struct ApproxDistinctCountState {
	HyperLogLog *log;
};

struct ApproxCountDistinctFunction {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->log = nullptr;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
		if (!source.log) {
			return;
		}
		if (!target->log) {
			target->log = new HyperLogLog();
		}
		D_ASSERT(target->log);
		D_ASSERT(source.log);
		auto new_log = target->log->MergePointer(*source.log);
		delete target->log;
		target->log = new_log;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (state->log) {
			target[idx] = state->log->Count();
		} else {
			target[idx] = 0;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
	template <class STATE>
	static void Destroy(STATE *state) {
		if (state->log) {
			delete state->log;
		}
	}
};

static void ApproxCountDistinctSimpleUpdateFunction(Vector inputs[], AggregateInputData &, idx_t input_count,
                                                    data_ptr_t state, idx_t count) {
	D_ASSERT(input_count == 1);

	auto agg_state = (ApproxDistinctCountState *)state;
	if (!agg_state->log) {
		agg_state->log = new HyperLogLog();
	}

	VectorData vdata;
	inputs[0].Orrify(count, vdata);

	uint64_t indices[STANDARD_VECTOR_SIZE];
	uint8_t counts[STANDARD_VECTOR_SIZE];

	HyperLogLog::ProcessEntries(vdata, inputs[0].GetType(), indices, counts, count);
	agg_state->log->AddToLog(vdata, count, indices, counts);
}

static void ApproxCountDistinctUpdateFunction(Vector inputs[], AggregateInputData &, idx_t input_count,
                                              Vector &state_vector, idx_t count) {
	D_ASSERT(input_count == 1);

	VectorData sdata;
	state_vector.Orrify(count, sdata);
	auto states = (ApproxDistinctCountState **)sdata.data;

	for (idx_t i = 0; i < count; i++) {
		auto agg_state = states[sdata.sel->get_index(i)];
		if (!agg_state->log) {
			agg_state->log = new HyperLogLog();
		}
	}

	VectorData vdata;
	inputs[0].Orrify(count, vdata);

	uint64_t indices[STANDARD_VECTOR_SIZE];
	uint8_t counts[STANDARD_VECTOR_SIZE];

	HyperLogLog::ProcessEntries(vdata, inputs[0].GetType(), indices, counts, count);
	HyperLogLog::AddToLogs(vdata, count, indices, counts, (HyperLogLog ***)states, sdata.sel);
}

AggregateFunction GetApproxCountDistinctFunction(const LogicalType &input_type) {
	return AggregateFunction(
	    {input_type}, LogicalTypeId::BIGINT, AggregateFunction::StateSize<ApproxDistinctCountState>,
	    AggregateFunction::StateInitialize<ApproxDistinctCountState, ApproxCountDistinctFunction>,
	    ApproxCountDistinctUpdateFunction,
	    AggregateFunction::StateCombine<ApproxDistinctCountState, ApproxCountDistinctFunction>,
	    AggregateFunction::StateFinalize<ApproxDistinctCountState, int64_t, ApproxCountDistinctFunction>,
	    ApproxCountDistinctSimpleUpdateFunction, nullptr,
	    AggregateFunction::StateDestroy<ApproxDistinctCountState, ApproxCountDistinctFunction>);
}

void ApproxCountDistinctFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet approx_count("approx_count_distinct");
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::UTINYINT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::USMALLINT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::UINTEGER));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::UBIGINT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::TINYINT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::SMALLINT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::BIGINT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::HUGEINT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::FLOAT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::DOUBLE));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::VARCHAR));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::TIMESTAMP));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::TIMESTAMP_TZ));
	set.AddFunction(approx_count);
}

} // namespace duckdb







namespace duckdb {

template <class T, class T2>
struct ArgMinMaxState {
	T arg;
	T2 value;
	bool is_initialized;
};

template <class T>
static void ArgMinMaxDestroyValue(T value) {
}

template <>
void ArgMinMaxDestroyValue(string_t value) {
	if (!value.IsInlined()) {
		delete[] value.GetDataUnsafe();
	}
}

template <class T>
static void ArgMinMaxAssignValue(T &target, T new_value, bool is_initialized) {
	target = new_value;
}

template <>
void ArgMinMaxAssignValue(string_t &target, string_t new_value, bool is_initialized) {
	if (is_initialized) {
		ArgMinMaxDestroyValue(target);
	}
	if (new_value.IsInlined()) {
		target = new_value;
	} else {
		// non-inlined string, need to allocate space for it
		auto len = new_value.GetSize();
		auto ptr = new char[len];
		memcpy(ptr, new_value.GetDataUnsafe(), len);

		target = string_t(ptr, len);
	}
}

template <class COMPARATOR>
struct ArgMinMaxBase {
	template <class STATE>
	static void Destroy(STATE *state) {
		if (state->is_initialized) {
			ArgMinMaxDestroyValue(state->arg);
			ArgMinMaxDestroyValue(state->value);
		}
	}

	template <class STATE>
	static void Initialize(STATE *state) {
		state->is_initialized = false;
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &, A_TYPE *x_data, B_TYPE *y_data, ValidityMask &amask,
	                      ValidityMask &bmask, idx_t xidx, idx_t yidx) {
		if (!state->is_initialized) {
			ArgMinMaxAssignValue<A_TYPE>(state->arg, x_data[xidx], false);
			ArgMinMaxAssignValue<B_TYPE>(state->value, y_data[yidx], false);
			state->is_initialized = true;
		} else {
			OP::template Execute<A_TYPE, B_TYPE, STATE>(state, x_data[xidx], y_data[yidx]);
		}
	}

	template <class A_TYPE, class B_TYPE, class STATE>
	static void Execute(STATE *state, A_TYPE x_data, B_TYPE y_data) {
		if (COMPARATOR::Operation(y_data, state->value)) {
			ArgMinMaxAssignValue<A_TYPE>(state->arg, x_data, true);
			ArgMinMaxAssignValue<B_TYPE>(state->value, y_data, true);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
		if (!source.is_initialized) {
			return;
		}
		if (!target->is_initialized || COMPARATOR::Operation(source.value, target->value)) {
			ArgMinMaxAssignValue(target->arg, source.arg, target->is_initialized);
			ArgMinMaxAssignValue(target->value, source.value, target->is_initialized);
			target->is_initialized = true;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

template <class COMPARATOR>
struct StringArgMinMax : public ArgMinMaxBase<COMPARATOR> {
	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (!state->is_initialized) {
			mask.SetInvalid(idx);
		} else {
			target[idx] = StringVector::AddStringOrBlob(result, state->arg);
		}
	}
};

template <class COMPARATOR>
struct NumericArgMinMax : public ArgMinMaxBase<COMPARATOR> {
	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (!state->is_initialized) {
			mask.SetInvalid(idx);
		} else {
			target[idx] = state->arg;
		}
	}
};

using NumericArgMinOperation = NumericArgMinMax<LessThan>;
using NumericArgMaxOperation = NumericArgMinMax<GreaterThan>;
using StringArgMinOperation = StringArgMinMax<LessThan>;
using StringArgMaxOperation = StringArgMinMax<GreaterThan>;

template <class OP, class T, class T2>
AggregateFunction GetArgMinMaxFunctionInternal(const LogicalType &arg_2, const LogicalType &arg) {
	auto function = AggregateFunction::BinaryAggregate<ArgMinMaxState<T, T2>, T, T2, T, OP>(arg, arg_2, arg);
	if (arg.InternalType() == PhysicalType::VARCHAR || arg_2.InternalType() == PhysicalType::VARCHAR) {
		function.destructor = AggregateFunction::StateDestroy<ArgMinMaxState<T, T2>, OP>;
	}
	return function;
}
template <class OP, class T>
AggregateFunction GetArgMinMaxFunctionArg2(const LogicalType &arg_2, const LogicalType &arg) {
	switch (arg_2.InternalType()) {
	case PhysicalType::INT32:
		return GetArgMinMaxFunctionInternal<OP, T, int32_t>(arg_2, arg);
	case PhysicalType::INT64:
		return GetArgMinMaxFunctionInternal<OP, T, int64_t>(arg_2, arg);
	case PhysicalType::DOUBLE:
		return GetArgMinMaxFunctionInternal<OP, T, double>(arg_2, arg);
	case PhysicalType::VARCHAR:
		return GetArgMinMaxFunctionInternal<OP, T, string_t>(arg_2, arg);
	default:
		throw InternalException("Unimplemented arg_min/arg_max aggregate");
	}
}

template <class OP, class T>
void AddArgMinMaxFunctionArg2(AggregateFunctionSet &fun, const LogicalType &arg) {
	fun.AddFunction(GetArgMinMaxFunctionArg2<OP, T>(LogicalType::INTEGER, arg));
	fun.AddFunction(GetArgMinMaxFunctionArg2<OP, T>(LogicalType::BIGINT, arg));
	fun.AddFunction(GetArgMinMaxFunctionArg2<OP, T>(LogicalType::DOUBLE, arg));
	fun.AddFunction(GetArgMinMaxFunctionArg2<OP, T>(LogicalType::VARCHAR, arg));
	fun.AddFunction(GetArgMinMaxFunctionArg2<OP, T>(LogicalType::DATE, arg));
	fun.AddFunction(GetArgMinMaxFunctionArg2<OP, T>(LogicalType::TIMESTAMP, arg));
	fun.AddFunction(GetArgMinMaxFunctionArg2<OP, T>(LogicalType::TIMESTAMP_TZ, arg));
	fun.AddFunction(GetArgMinMaxFunctionArg2<OP, T>(LogicalType::BLOB, arg));
}

template <class OP, class STRING_OP>
static void AddArgMinMaxFunctions(AggregateFunctionSet &fun) {
	AddArgMinMaxFunctionArg2<OP, int32_t>(fun, LogicalType::INTEGER);
	AddArgMinMaxFunctionArg2<OP, int64_t>(fun, LogicalType::BIGINT);
	AddArgMinMaxFunctionArg2<OP, double>(fun, LogicalType::DOUBLE);
	AddArgMinMaxFunctionArg2<STRING_OP, string_t>(fun, LogicalType::VARCHAR);
	AddArgMinMaxFunctionArg2<OP, date_t>(fun, LogicalType::DATE);
	AddArgMinMaxFunctionArg2<OP, timestamp_t>(fun, LogicalType::TIMESTAMP);
	AddArgMinMaxFunctionArg2<OP, timestamp_t>(fun, LogicalType::TIMESTAMP_TZ);
	AddArgMinMaxFunctionArg2<STRING_OP, string_t>(fun, LogicalType::BLOB);
}

void ArgMinFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet fun("argmin");
	AddArgMinMaxFunctions<NumericArgMinOperation, StringArgMinOperation>(fun);
	set.AddFunction(fun);

	//! Add min_by alias
	fun.name = "min_by";
	set.AddFunction(fun);

	//! Add arg_min alias
	fun.name = "arg_min";
	set.AddFunction(fun);
}

void ArgMaxFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet fun("argmax");
	AddArgMinMaxFunctions<NumericArgMaxOperation, StringArgMaxOperation>(fun);
	set.AddFunction(fun);

	//! Add max_by alias
	fun.name = "max_by";
	set.AddFunction(fun);

	//! Add arg_max alias
	fun.name = "arg_max";
	set.AddFunction(fun);
}

} // namespace duckdb






namespace duckdb {

template <class T>
struct BitState {
	bool is_set;
	T value;
};

template <class OP>
static AggregateFunction GetBitfieldUnaryAggregate(LogicalType type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return AggregateFunction::UnaryAggregate<BitState<uint8_t>, int8_t, int8_t, OP>(type, type);
	case LogicalTypeId::SMALLINT:
		return AggregateFunction::UnaryAggregate<BitState<uint16_t>, int16_t, int16_t, OP>(type, type);
	case LogicalTypeId::INTEGER:
		return AggregateFunction::UnaryAggregate<BitState<uint32_t>, int32_t, int32_t, OP>(type, type);
	case LogicalTypeId::BIGINT:
		return AggregateFunction::UnaryAggregate<BitState<uint64_t>, int64_t, int64_t, OP>(type, type);
	case LogicalTypeId::HUGEINT:
		return AggregateFunction::UnaryAggregate<BitState<hugeint_t>, hugeint_t, hugeint_t, OP>(type, type);
	case LogicalTypeId::UTINYINT:
		return AggregateFunction::UnaryAggregate<BitState<uint8_t>, uint8_t, uint8_t, OP>(type, type);
	case LogicalTypeId::USMALLINT:
		return AggregateFunction::UnaryAggregate<BitState<uint16_t>, uint16_t, uint16_t, OP>(type, type);
	case LogicalTypeId::UINTEGER:
		return AggregateFunction::UnaryAggregate<BitState<uint32_t>, uint32_t, uint32_t, OP>(type, type);
	case LogicalTypeId::UBIGINT:
		return AggregateFunction::UnaryAggregate<BitState<uint64_t>, uint64_t, uint64_t, OP>(type, type);
	default:
		throw InternalException("Unimplemented bitfield type for unary aggregate");
	}
}

struct BitAndOperation {
	template <class STATE>
	static void Initialize(STATE *state) {
		//  If there are no matching rows, BIT_AND() returns a null value.
		state->is_set = false;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask, idx_t idx) {
		if (!state->is_set) {
			state->is_set = true;
			state->value = input[idx];
		} else {
			state->value &= input[idx];
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, AggregateInputData &aggr_input_data, INPUT_TYPE *input,
	                              ValidityMask &mask, idx_t count) {
		//  count is irrelevant
		Operation<INPUT_TYPE, STATE, OP>(state, aggr_input_data, input, mask, 0);
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (!state->is_set) {
			mask.SetInvalid(idx);
		} else {
			target[idx] = state->value;
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
		if (!source.is_set) {
			// source is NULL, nothing to do.
			return;
		}
		if (!target->is_set) {
			// target is NULL, use source value directly.
			*target = source;
		} else {
			target->value &= source.value;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

void BitAndFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet bit_and("bit_and");
	for (auto &type : LogicalType::Integral()) {
		bit_and.AddFunction(GetBitfieldUnaryAggregate<BitAndOperation>(type));
	}
	set.AddFunction(bit_and);
}

struct BitOrOperation {
	template <class STATE>
	static void Initialize(STATE *state) {
		//  If there are no matching rows, BIT_OR() returns a null value.
		state->is_set = false;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask, idx_t idx) {
		if (!state->is_set) {
			state->is_set = true;
			state->value = input[idx];
		} else {
			state->value |= input[idx];
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, AggregateInputData &aggr_input_data, INPUT_TYPE *input,
	                              ValidityMask &mask, idx_t count) {
		//  count is irrelevant
		Operation<INPUT_TYPE, STATE, OP>(state, aggr_input_data, input, mask, 0);
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (!state->is_set) {
			mask.SetInvalid(idx);
		} else {
			target[idx] = state->value;
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
		if (!source.is_set) {
			// source is NULL, nothing to do.
			return;
		}
		if (!target->is_set) {
			// target is NULL, use source value directly.
			*target = source;
		} else {
			target->value |= source.value;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

void BitOrFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet bit_or("bit_or");
	for (auto &type : LogicalType::Integral()) {
		bit_or.AddFunction(GetBitfieldUnaryAggregate<BitOrOperation>(type));
	}
	set.AddFunction(bit_or);
}

struct BitXorOperation {
	template <class STATE>
	static void Initialize(STATE *state) {
		//  If there are no matching rows, BIT_XOR() returns a null value.
		state->is_set = false;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask, idx_t idx) {
		if (!state->is_set) {
			state->is_set = true;
			state->value = input[idx];
		} else {
			state->value ^= input[idx];
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, AggregateInputData &aggr_input_data, INPUT_TYPE *input,
	                              ValidityMask &mask, idx_t count) {
		//  count is irrelevant
		Operation<INPUT_TYPE, STATE, OP>(state, aggr_input_data, input, mask, 0);
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (!state->is_set) {
			mask.SetInvalid(idx);
		} else {
			target[idx] = state->value;
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
		if (!source.is_set) {
			// source is NULL, nothing to do.
			return;
		}
		if (!target->is_set) {
			// target is NULL, use source value directly.
			*target = source;
		} else {
			target->value ^= source.value;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

void BitXorFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet bit_xor("bit_xor");
	for (auto &type : LogicalType::Integral()) {
		bit_xor.AddFunction(GetBitfieldUnaryAggregate<BitXorOperation>(type));
	}
	set.AddFunction(bit_xor);
}

} // namespace duckdb






namespace duckdb {

struct BoolState {
	bool empty;
	bool val;
};

struct BoolAndFunFunction {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->val = true;
		state->empty = true;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
		target->val = target->val && source.val;
		target->empty = target->empty && source.empty;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (state->empty) {
			mask.SetInvalid(idx);
			return;
		}
		target[idx] = state->val;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask, idx_t idx) {
		state->empty = false;
		state->val = input[idx] && state->val;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, AggregateInputData &aggr_input_data, INPUT_TYPE *input,
	                              ValidityMask &mask, idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, aggr_input_data, input, mask, 0);
		}
	}
	static bool IgnoreNull() {
		return true;
	}
};

struct BoolOrFunFunction {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->val = false;
		state->empty = true;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
		target->val = target->val || source.val;
		target->empty = target->empty && source.empty;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (state->empty) {
			mask.SetInvalid(idx);
			return;
		}
		target[idx] = state->val;
	}
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask, idx_t idx) {
		state->empty = false;
		state->val = input[idx] || state->val;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, AggregateInputData &aggr_input_data, INPUT_TYPE *input,
	                              ValidityMask &mask, idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, aggr_input_data, input, mask, 0);
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

AggregateFunction BoolOrFun::GetFunction() {
	auto fun = AggregateFunction::UnaryAggregate<BoolState, bool, bool, BoolOrFunFunction>(
	    LogicalType(LogicalTypeId::BOOLEAN), LogicalType::BOOLEAN);
	fun.name = "bool_or";
	return fun;
}

AggregateFunction BoolAndFun::GetFunction() {
	auto fun = AggregateFunction::UnaryAggregate<BoolState, bool, bool, BoolAndFunFunction>(
	    LogicalType(LogicalTypeId::BOOLEAN), LogicalType::BOOLEAN);
	fun.name = "bool_and";
	return fun;
}

void BoolOrFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunction bool_or_function = BoolOrFun::GetFunction();
	AggregateFunctionSet bool_or("bool_or");
	bool_or.AddFunction(bool_or_function);
	set.AddFunction(bool_or);
}

void BoolAndFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunction bool_and_function = BoolAndFun::GetFunction();
	AggregateFunctionSet bool_and("bool_and");
	bool_and.AddFunction(bool_and_function);
	set.AddFunction(bool_and);
}

} // namespace duckdb






namespace duckdb {

struct BaseCountFunction {
	template <class STATE>
	static void Initialize(STATE *state) {
		*state = 0;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
		*target += source;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		target[idx] = *state;
	}
};

struct CountStarFunction : public BaseCountFunction {
	template <class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &, idx_t idx) {
		*state += 1;
	}

	template <class STATE, class OP>
	static void ConstantOperation(STATE *state, AggregateInputData &, idx_t count) {
		*state += count;
	}
};

struct CountFunction : public BaseCountFunction {
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask, idx_t idx) {
		*state += 1;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask,
	                              idx_t count) {
		*state += count;
	}

	static bool IgnoreNull() {
		return true;
	}
};

AggregateFunction CountFun::GetFunction() {
	auto fun = AggregateFunction::UnaryAggregate<int64_t, int64_t, int64_t, CountFunction>(
	    LogicalType(LogicalTypeId::ANY), LogicalType::BIGINT);
	fun.name = "count";
	return fun;
}

AggregateFunction CountStarFun::GetFunction() {
	auto fun = AggregateFunction::NullaryAggregate<int64_t, int64_t, CountStarFunction>(LogicalType::BIGINT);
	fun.name = "count_star";
	return fun;
}

unique_ptr<BaseStatistics> CountPropagateStats(ClientContext &context, BoundAggregateExpression &expr,
                                               FunctionData *bind_data, vector<unique_ptr<BaseStatistics>> &child_stats,
                                               NodeStatistics *node_stats) {
	if (!expr.distinct && child_stats[0] && !child_stats[0]->CanHaveNull()) {
		// count on a column without null values: use count star
		expr.function = CountStarFun::GetFunction();
		expr.function.name = "count_star";
		expr.children.clear();
	}
	return nullptr;
}

void CountFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunction count_function = CountFun::GetFunction();
	count_function.statistics = CountPropagateStats;
	AggregateFunctionSet count("count");
	count.AddFunction(count_function);
	// the count function can also be called without arguments
	count_function.arguments.clear();
	count_function.statistics = nullptr;
	count.AddFunction(count_function);
	set.AddFunction(count);
}

void CountStarFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet count("count_star");
	count.AddFunction(CountStarFun::GetFunction());
	set.AddFunction(count);
}

} // namespace duckdb





#include <unordered_map>

namespace duckdb {

template <class T>
struct EntropyState {
	using DistinctMap = unordered_map<T, idx_t>;

	idx_t count;
	DistinctMap *distinct;

	EntropyState &operator=(const EntropyState &other) = delete;

	EntropyState &Assign(const EntropyState &other) {
		D_ASSERT(!distinct);
		distinct = new DistinctMap(*other.distinct);
		count = other.count;
		return *this;
	}
};

struct EntropyFunctionBase {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->distinct = nullptr;
		state->count = 0;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
		if (!source.distinct) {
			return;
		}
		if (!target->distinct) {
			target->Assign(source);
			return;
		}
		for (auto &val : *source.distinct) {
			auto value = val.first;
			(*target->distinct)[value] += val.second;
		}
		target->count += source.count;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		double count = state->count;
		if (state->distinct) {
			double entropy = 0;
			for (auto &val : *state->distinct) {
				entropy += (val.second / count) * log2(count / val.second);
			}
			target[idx] = entropy;
		} else {
			target[idx] = 0;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
	template <class STATE>
	static void Destroy(STATE *state) {
		if (state->distinct) {
			delete state->distinct;
		}
	}
};

struct EntropyFunction : EntropyFunctionBase {
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask, idx_t idx) {
		if (!state->distinct) {
			state->distinct = new unordered_map<INPUT_TYPE, idx_t>();
		}
		(*state->distinct)[input[idx]]++;
		state->count++;
	}
	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, AggregateInputData &aggr_input_data, INPUT_TYPE *input,
	                              ValidityMask &mask, idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, aggr_input_data, input, mask, 0);
		}
	}
};

struct EntropyFunctionString : EntropyFunctionBase {
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask, idx_t idx) {
		if (!state->distinct) {
			state->distinct = new unordered_map<string, idx_t>();
		}
		auto value = input[idx].GetString();
		(*state->distinct)[value]++;
		state->count++;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, AggregateInputData &aggr_input_data, INPUT_TYPE *input,
	                              ValidityMask &mask, idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, aggr_input_data, input, mask, 0);
		}
	}
};

template <typename INPUT_TYPE, typename RESULT_TYPE>
AggregateFunction GetEntropyFunction(const LogicalType &input_type, const LogicalType &result_type) {
	return AggregateFunction::UnaryAggregateDestructor<EntropyState<INPUT_TYPE>, INPUT_TYPE, RESULT_TYPE,
	                                                   EntropyFunction>(input_type, result_type);
}

AggregateFunction GetEntropyFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::UINT16:
		return AggregateFunction::UnaryAggregateDestructor<EntropyState<uint16_t>, uint16_t, double, EntropyFunction>(
		    LogicalType::USMALLINT, LogicalType::DOUBLE);
	case PhysicalType::UINT32:
		return AggregateFunction::UnaryAggregateDestructor<EntropyState<uint32_t>, uint32_t, double, EntropyFunction>(
		    LogicalType::UINTEGER, LogicalType::DOUBLE);
	case PhysicalType::UINT64:
		return AggregateFunction::UnaryAggregateDestructor<EntropyState<uint64_t>, uint64_t, double, EntropyFunction>(
		    LogicalType::UBIGINT, LogicalType::DOUBLE);
	case PhysicalType::INT16:
		return AggregateFunction::UnaryAggregateDestructor<EntropyState<int16_t>, int16_t, double, EntropyFunction>(
		    LogicalType::SMALLINT, LogicalType::DOUBLE);
	case PhysicalType::INT32:
		return AggregateFunction::UnaryAggregateDestructor<EntropyState<int32_t>, int32_t, double, EntropyFunction>(
		    LogicalType::INTEGER, LogicalType::DOUBLE);
	case PhysicalType::INT64:
		return AggregateFunction::UnaryAggregateDestructor<EntropyState<int64_t>, int64_t, double, EntropyFunction>(
		    LogicalType::BIGINT, LogicalType::DOUBLE);
	case PhysicalType::FLOAT:
		return AggregateFunction::UnaryAggregateDestructor<EntropyState<float>, float, double, EntropyFunction>(
		    LogicalType::FLOAT, LogicalType::DOUBLE);
	case PhysicalType::DOUBLE:
		return AggregateFunction::UnaryAggregateDestructor<EntropyState<double>, double, double, EntropyFunction>(
		    LogicalType::DOUBLE, LogicalType::DOUBLE);
	case PhysicalType::VARCHAR:
		return AggregateFunction::UnaryAggregateDestructor<EntropyState<string>, string_t, double,
		                                                   EntropyFunctionString>(LogicalType::VARCHAR,
		                                                                          LogicalType::DOUBLE);

	default:
		throw InternalException("Unimplemented approximate_count aggregate");
	}
}

void EntropyFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet entropy("entropy");
	entropy.AddFunction(GetEntropyFunction(PhysicalType::UINT16));
	entropy.AddFunction(GetEntropyFunction(PhysicalType::UINT32));
	entropy.AddFunction(GetEntropyFunction(PhysicalType::UINT64));
	entropy.AddFunction(GetEntropyFunction(PhysicalType::FLOAT));
	entropy.AddFunction(GetEntropyFunction(PhysicalType::INT16));
	entropy.AddFunction(GetEntropyFunction(PhysicalType::INT32));
	entropy.AddFunction(GetEntropyFunction(PhysicalType::INT64));
	entropy.AddFunction(GetEntropyFunction(PhysicalType::DOUBLE));
	entropy.AddFunction(GetEntropyFunction(PhysicalType::VARCHAR));
	entropy.AddFunction(GetEntropyFunction<int64_t, double>(LogicalType::TIMESTAMP, LogicalType::DOUBLE));
	entropy.AddFunction(GetEntropyFunction<int64_t, double>(LogicalType::TIMESTAMP_TZ, LogicalType::DOUBLE));
	set.AddFunction(entropy);
}

} // namespace duckdb
