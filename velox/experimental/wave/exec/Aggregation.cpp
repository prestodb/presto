/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/experimental/wave/exec/Aggregation.h"

#include "velox/experimental/wave/common/IdMap.h"
#include "velox/experimental/wave/exec/ToWave.h"
#include "velox/experimental/wave/exec/Vectors.h"

#define KEY_TYPE_DISPATCH(_func, _kindExpr, ...)              \
  [&]() {                                                     \
    auto _kind = (_kindExpr);                                 \
    switch (_kind) {                                          \
      case PhysicalType::kInt32:                              \
        return _func<int32_t>(__VA_ARGS__);                   \
      case PhysicalType::kInt64:                              \
        return _func<int64_t>(__VA_ARGS__);                   \
      case PhysicalType::kString:                             \
        return _func<StringView>(__VA_ARGS__);                \
      default:                                                \
        VELOX_UNSUPPORTED("{}", static_cast<int32_t>(_kind)); \
    };                                                        \
  }()

namespace facebook::velox::wave {

namespace {

constexpr int kInitialNumGroups = 16;
constexpr int kInitialTableCapacity = 64;

template <typename T>
Aggregation::IdMapHolder createIdMap(GpuArena& arena, int capacity) {
  Aggregation::IdMapHolder holder;
  auto* idMap = arena.allocate<IdMap<T>>(1, holder.idMap);
  auto* keys = arena.allocate<T>(capacity, holder.keys);
  auto* ids = arena.allocate<int32_t>(capacity, holder.ids);
  idMap->init(capacity, keys, ids);
  return holder;
}

template <typename T>
int sizeOf() {
  return sizeof(T);
}

} // namespace

void Aggregation::toAggregateInfo(
    const TypePtr& inputType,
    const core::AggregationNode::Aggregate& aggregate,
    AggregateInfo* out) {
  std::vector<PhysicalType> argTypes;
  for (auto& arg : aggregate.call->inputs()) {
    int i = exec::exprToChannel(arg.get(), inputType);
    VELOX_CHECK_NE(i, kConstantChannel);
    out->inputChannels.push_back(i);
    argTypes.push_back(fromCpuType(*inputType->childAt(i)));
  }
  out->function =
      functionRegistry_->getFunction(aggregate.call->name(), argTypes);
  VELOX_CHECK_NOT_NULL(out->function);
}

Aggregation::Aggregation(
    CompileState& state,
    const core::AggregationNode& node,
    const std::shared_ptr<aggregation::AggregateFunctionRegistry>&
        functionRegistry)
    : WaveOperator(state, node.outputType(), node.id()),
      arena_(&state.arena()),
      functionRegistry_(functionRegistry) {
  VELOX_CHECK(node.step() == core::AggregationNode::Step::kSingle);
  VELOX_CHECK(node.preGroupedKeys().empty());
  auto& inputType = node.sources()[0]->outputType();
  container_ = arena_->allocate<aggregation::GroupsContainer>(
      1, containerHolder_.container);
  container_->numKeys = node.groupingKeys().size();
  container_->numAggregates = node.aggregates().size();
  container_->numGroups = kInitialNumGroups;
  container_->actualNumGroups = 0;
  container_->keyTypes = arena_->allocate<PhysicalType>(
      container_->numKeys, containerHolder_.keyTypes);
  container_->idMaps =
      arena_->allocate<void*>(container_->numKeys, containerHolder_.idMaps);
  container_->groups = arena_->allocate<aggregation::Group>(
      container_->numGroups, containerHolder_.groups);
  container_->useThreadLocalAccumulator = true;
  containerHolder_.idMapHolders.resize(container_->numKeys);
  containerHolder_.keys.resize(container_->numKeys);
  keyChannels_.resize(container_->numKeys);
  for (int i = 0; i < container_->numKeys; ++i) {
    auto j = exec::exprToChannel(node.groupingKeys()[i].get(), inputType);
    VELOX_CHECK_NE(j, kConstantChannel);
    keyChannels_[i] = j;
    container_->keyTypes[i] = fromCpuType(*inputType->childAt(j));
    auto kind = container_->keyTypes[i].kind;
    containerHolder_.idMapHolders[i] =
        KEY_TYPE_DISPATCH(createIdMap, kind, *arena_, kInitialTableCapacity);
    container_->idMaps[i] = containerHolder_.idMapHolders[i].idMap->as<void*>();
    containerHolder_.keys[i] =
        KEY_TYPE_DISPATCH(arena_->allocate, kind, container_->numGroups);
  }
  aggregates_.resize(container_->numAggregates);
  containerHolder_.accumulators.resize(container_->numAggregates);
  for (int i = 0; i < container_->numAggregates; ++i) {
    toAggregateInfo(inputType, node.aggregates()[i], &aggregates_[i]);
    auto accumulatorSize = aggregates_[i].function->accumulatorSize;
    if (container_->useThreadLocalAccumulator) {
      accumulatorSize *= kBlockSize;
    }
    containerHolder_.accumulators[i] =
        arena_->allocateBytes(container_->numGroups * accumulatorSize);
    bzero(
        containerHolder_.accumulators[i]->as<char>(),
        container_->numGroups * accumulatorSize);
  }
  void** groupKeys = arena_->allocate<void*>(
      container_->numGroups * container_->numKeys, containerHolder_.groupKeys);
  void** groupAccumulators = arena_->allocate<void*>(
      container_->numGroups * container_->numAggregates,
      containerHolder_.groupAccumulators);
  for (int i = 0; i < container_->numGroups; ++i) {
    container_->groups[i].initialized = 0;
    container_->groups[i].keys = &groupKeys[i * container_->numKeys];
    container_->groups[i].accumulators =
        &groupAccumulators[i * container_->numAggregates];
    for (int j = 0; j < container_->numKeys; ++j) {
      container_->groups[i].keys[j] = containerHolder_.keys[j]->as<char>() +
          i * KEY_TYPE_DISPATCH(sizeOf, container_->keyTypes[j].kind);
    }
    for (int j = 0; j < container_->numAggregates; ++j) {
      auto accumulatorSize = aggregates_[j].function->accumulatorSize;
      if (container_->useThreadLocalAccumulator) {
        accumulatorSize *= kBlockSize;
      }
      container_->groups[i].accumulators[j] =
          containerHolder_.accumulators[j]->as<char>() + i * accumulatorSize;
    }
  }
}

Aggregation::~Aggregation() {
  if (flushStream_) {
    WaveStream::releaseStream(std::move(flushStream_));
  }
}

BlockStatus* Aggregation::getStatus(int size, WaveBufferPtr& holder) {
  if (holder && holder->capacity() >= size * sizeof(BlockStatus)) {
    return holder->as<BlockStatus>();
  }
  auto* status = arena_->allocate<BlockStatus>(size, holder);
  bzero(status, size * sizeof(BlockStatus));
  return status;
}

void Aggregation::normalizeKeys() {
  auto& holder = normalizeKeysHolder_;
  int numBlocks = 0;
  for (auto& input : inputs_) {
    numBlocks =
        std::max(numBlocks, (input->size() + kBlockSize - 1) / kBlockSize);
  }
  auto* programs = arena_->allocate<aggregation::ThreadBlockProgram>(
      numBlocks, holder.programs);
  auto* instructions = arena_->allocate<aggregation::Instruction>(
      inputs_.size(), holder.instructions);
  holder.operands.resize(inputs_.size());
  if (holder.results.size() < inputs_.size()) {
    holder.results.resize(inputs_.size());
  }
  for (int i = 0; i < inputs_.size(); ++i) {
    instructions[i].opCode = aggregation::OpCode::kNormalizeKeys;
    auto& normalizeKeys = instructions[i]._.normalizeKeys;
    auto* operands =
        arena_->allocate<Operand>(keyChannels_.size() + 1, holder.operands[i]);
    normalizeKeys.container = container_;
    normalizeKeys.inputs = operands + 1;
    normalizeKeys.result = operands;
    for (int j = 0; j < keyChannels_.size(); ++j) {
      inputs_[i]->childAt(keyChannels_[j]).toOperand(&normalizeKeys.inputs[j]);
    }
    ensureWaveVector(
        holder.results[i], INTEGER(), inputs_[i]->size(), false, *arena_);
    holder.results[i]->toOperand(normalizeKeys.result);
  }
  for (int j = 0; j < numBlocks; ++j) {
    programs[j].numInstructions = inputs_.size();
    programs[j].instructions = instructions;
  }
  auto* status = getStatus(numBlocks, holder.status);
  aggregation::call(*flushStream_, numBlocks, programs, nullptr, status, 0);
}

void Aggregation::doAggregates() {
  auto& holder = aggregateHolder_;
  auto* programs = arena_->allocate<aggregation::ThreadBlockProgram>(
      aggregates_.size(), holder.programs);
  auto numInstructions = aggregates_.size() * inputs_.size();
  auto* instructions = arena_->allocate<aggregation::Instruction>(
      numInstructions, holder.instructions);
  holder.operands.resize(numInstructions);
  for (int i = 0; i < aggregates_.size(); ++i) {
    programs[i].numInstructions = inputs_.size();
    programs[i].instructions = &instructions[i * inputs_.size()];
    auto& inputChannels = aggregates_[i].inputChannels;
    for (int j = 0; j < inputs_.size(); ++j) {
      int k = j + i * inputs_.size();
      instructions[k].opCode = aggregation::OpCode::kAggregate;
      auto& aggregate = instructions[k]._.aggregate;
      aggregate.container = container_;
      aggregate.normalizedKey = normalizeKeysHolder_.operands[j]->as<Operand>();
      aggregate.function = aggregates_[i].function;
      aggregate.numInputs = inputChannels.size();
      aggregate.inputs =
          arena_->allocate<Operand>(inputChannels.size(), holder.operands[k]);
      aggregate.accumulatorIndex = i;
      for (int w = 0; w < inputChannels.size(); ++w) {
        inputs_[j]->childAt(inputChannels[w]).toOperand(&aggregate.inputs[w]);
      }
    }
  }
  auto* status = getStatus(aggregates_.size(), holder.status);
  aggregation::call(
      *flushStream_, aggregates_.size(), programs, nullptr, status, 0);
}

void Aggregation::waitFlushDone() {
  flushDone_.wait();
  for (auto& input : inputs_) {
    stats_.ingestedRowCount += input->size();
  }
  stats_.gpuTimeMs += flushDone_.elapsedTime(flushStart_);
}

void Aggregation::flush(bool noMoreInput) {
  if (noMoreInput) {
    if (!noMoreInput_) {
      VLOG(1) << "No more input";
      noMoreInput_ = true;
    }
  } else {
    VELOX_CHECK(!noMoreInput_);
  }
  if (!inputs_.empty()) {
    VELOX_CHECK(flushStream_);
    if (noMoreInput) {
      waitFlushDone();
    } else if (!flushDone_.query()) {
      return;
    }
    inputs_.clear();
  }
  if (buffered_.empty()) {
    return;
  }
  if (!flushStream_) {
    flushStream_ = WaveStream::streamFromReserve();
  }
  VLOG(1) << "Flush " << buffered_.size() << " batches";
  inputs_ = std::move(buffered_);
  flushStart_.record(*flushStream_);
  normalizeKeys();
  doAggregates();
  flushDone_.record(*flushStream_);
}

int32_t Aggregation::canAdvance() {
  if (!noMoreInput_ || finished_) {
    return 0;
  }
  while (!inputs_.empty()) {
    waitFlushDone();
    flush(true);
  }
  return container_->actualNumGroups;
}

void Aggregation::schedule(WaveStream& waveStream, int32_t maxRows) {
  VELOX_CHECK(buffered_.empty());
  VELOX_CHECK(inputs_.empty());
  int numColumns = container_->numKeys + container_->numAggregates;
  auto exec = std::make_unique<Executable>();
  auto* programs = arena_->allocate<aggregation::ThreadBlockProgram>(
      numColumns, exec->deviceData.emplace_back());
  auto* instructions = arena_->allocate<aggregation::Instruction>(
      numColumns, exec->deviceData.emplace_back());
  auto* status = arena_->allocate<BlockStatus>(
      numColumns, exec->deviceData.emplace_back());
  bzero(status, numColumns * sizeof(BlockStatus));
  exec->operands =
      arena_->allocate<Operand>(numColumns, exec->deviceData.emplace_back());
  exec->outputOperands = outputIds_;
  for (int i = 0; i < numColumns; ++i) {
    auto column = WaveVector::create(outputType_->childAt(i), *arena_);
    column->resize(maxRows, false);
    column->toOperand(&exec->operands[i]);
    exec->output.push_back(std::move(column));
    programs[i].numInstructions = 1;
    programs[i].instructions = instructions + i;
  }
  for (int i = 0; i < container_->numKeys; ++i) {
    instructions[i].opCode = aggregation::OpCode::kExtractKeys;
    instructions[i]._.extractKeys.container = container_;
    instructions[i]._.extractKeys.keyIndex = i;
    instructions[i]._.extractKeys.result = &exec->operands[i];
  }
  for (int i = 0; i < container_->numAggregates; ++i) {
    int j = i + container_->numKeys;
    instructions[j].opCode = aggregation::OpCode::kExtractValues;
    instructions[j]._.extractValues.container = container_;
    instructions[j]._.extractValues.function = aggregates_[i].function;
    instructions[j]._.extractValues.accumulatorIndex = i;
    instructions[j]._.extractValues.result = &exec->operands[j];
  }
  waveStream.installExecutables(
      folly::Range(&exec, 1),
      [&](Stream* stream, folly::Range<Executable**> exes) {
        int sharedSize = std::max(
            aggregation::ExtractKeys::sharedSize(),
            aggregation::ExtractValues::sharedSize());
        aggregation::call(
            *stream, numColumns, programs, nullptr, status, sharedSize);
        waveStream.markLaunch(*stream, *exes[0]);
      });
  finished_ = true;
  VLOG(1) << "Average throughput "
          << stats_.ingestedRowCount / stats_.gpuTimeMs * 1000 << " rows/s";
}

vector_size_t Aggregation::outputSize(WaveStream&) const {
  return container_->actualNumGroups;
}

} // namespace facebook::velox::wave
